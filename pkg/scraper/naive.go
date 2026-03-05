package scraper

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	. "go-azimuth/pkg/db"
)

// Fetch all the Naive logs, and then fetch the transaction data for each log
func CatchUpNaiveLogs(client *ethclient.Client, db DB) {
	latest_block, err := client.BlockNumber(context.Background())
	if err != nil {
		panic(err)
	}

	contract := db.GetContractByName("Naive")

	from_block := max(contract.StartBlockNum, contract.LatestBlockNumFetched)
	if from_block <= latest_block {
		// Assume we can fetch all the logs in 1 query
		// TODO: not a good assumption
		query := ethereum.FilterQuery{
			FromBlock: big.NewInt(0).SetUint64(from_block),
			ToBlock:   big.NewInt(0).SetUint64(latest_block),
			Addresses: []common.Address{contract.Address},
		}
		logs, err := client.FilterLogs(context.Background(), query)
		if err != nil {
			// TODO: this would be where to handle if there's too many logs for 1 query
			panic(err)
		}

		// First, save the Ethereum logs
		for _, l := range logs {
			fmt.Println("----------")
			fmt.Println("Log Block Number:", l.BlockNumber)
			fmt.Printf(EVENT_NAMES[l.Topics[0]])
			fmt.Printf("(")
			for _, t := range l.Topics[1:] {
				fmt.Print(t, ", ")
			}
			fmt.Printf(")\n")
			if len(l.Data) != 0 {
				fmt.Println(hex.EncodeToString(l.Data))
			}

			naive_event_log := ParseEthereumLog(l)
			// Save it in the DB (idempotent)
			db.SaveEvent(&naive_event_log)
		}

		// Record that we've fetched logs even if tx data is still pending.
		db.SetLatestContractBlockFetched(contract.ID, latest_block)
	}

	// Fetch tx data for any Naive events missing call-data.
	missing_logs := db.GetEventsMissingData(contract.Address)
	if len(missing_logs) == 0 {
		return
	}

	// To get Tx data, we have to use batching; otherwise, turbo slow
	GetNaiveTransactionData(client, db, missing_logs)
}

const naiveRPCTimeout = 15 * time.Second

// Get transaction data (call-data) for Batch events, in batches (yes)
func GetNaiveTransactionData(client *ethclient.Client, db DB, logs []EthereumEventLog) {
	contract := db.GetContractByName("Naive")
	chainCtx, cancelChain := context.WithTimeout(context.Background(), naiveRPCTimeout)
	chainID, chainErr := client.ChainID(chainCtx)
	cancelChain()
	var signer types.Signer
	if chainErr == nil {
		signer = types.LatestSignerForChainID(chainID)
	} else {
		log.Printf("naive tx metadata: failed to fetch chain ID: %v", chainErr)
	}

	// Callback function to execute RPC batches
	is_retryable_tx_error := func(err error) bool {
		if err == nil {
			return false
		}
		if _, is_temp_unavailable := check_error(err); is_temp_unavailable {
			return true
		}
		err_msg := strings.ToLower(err.Error())
		return strings.Contains(err_msg, "response batch did not contain a response") ||
			strings.Contains(err_msg, "rate limit") ||
			strings.Contains(err_msg, "too many requests") ||
			strings.Contains(err_msg, "temporarily unavailable") ||
			strings.Contains(err_msg, "timeout") ||
			strings.Contains(err_msg, "i/o timeout") ||
			strings.Contains(err_msg, "connection reset") ||
			strings.Contains(err_msg, "429")
	}

	fetch_tx_by_hash_with_retry := func(hash common.Hash) (*types.Transaction, error) {
		const maxRetries = 8
		backoff := 1 * time.Second
		var last_err error
		for attempt := 0; attempt < maxRetries; attempt++ {
			ctx, cancel := context.WithTimeout(context.Background(), naiveRPCTimeout)
			tx, _, err := client.TransactionByHash(ctx, hash)
			cancel()
			if err == nil && tx != nil {
				return tx, nil
			}
			if err == nil && tx == nil {
				err = fmt.Errorf("missing tx result for %s", hash.Hex())
			}
			if !is_retryable_tx_error(err) {
				return nil, err
			}
			last_err = err
			log.Printf("tx fetch retry %d/%d for %s: %v", attempt+1, maxRetries, hash.Hex(), err)
			time.Sleep(backoff)
			if backoff < 15*time.Second {
				backoff *= 2
				if backoff > 15*time.Second {
					backoff = 15 * time.Second
				}
			}
		}
		return nil, fmt.Errorf("failed to fetch tx %s after %d attempts: %w", hash.Hex(), maxRetries, last_err)
	}

	fetch_batch_sequential := func(batch []rpc.BatchElem) ([]*types.Transaction, error) {
		ret := make([]*types.Transaction, 0, len(batch))
		for _, elem := range batch {
			hash, ok := elem.Args[0].(common.Hash)
			if !ok {
				return nil, fmt.Errorf("unexpected tx hash arg: %#v", elem.Args)
			}
			tx, err := fetch_tx_by_hash_with_retry(hash)
			if err != nil {
				return nil, err
			}
			ret = append(ret, tx)
		}
		return ret, nil
	}

	do_batched_rpc := func(batch []rpc.BatchElem) ([]*types.Transaction, error) {
		ctx, cancel := context.WithTimeout(context.Background(), naiveRPCTimeout)
		err := client.Client().BatchCallContext(ctx, batch)
		cancel()
		if err != nil {
			log.Printf("Batch call failed: %v; falling back to single RPCs", err)
			return fetch_batch_sequential(batch)
		}

		ret := make([]*types.Transaction, 0, len(batch)) // Avoid copying an atomic.Pointer
		for _, elem := range batch {
			// rpc.BatchElem{
			// 	Method:"eth_getTransactionByHash",
			// 	Args:[]interface {}{0xad2f676e4c35c7271123e77bd5616d4e89ae0e93cd0f5a9e4fb93a735ded42be},
			// 	Result:(*types.Transaction)(0xc000332180),
			// 	Error: &rpc.jsonError{Code:-32603, Message:"service temporarily unavailable", Data:interface {}(nil)},
			// }
			if elem.Error != nil {
				if is_retryable_tx_error(elem.Error) {
					hash, ok := elem.Args[0].(common.Hash)
					if !ok {
						return nil, fmt.Errorf("unexpected tx hash arg: %#v", elem.Args)
					}
					tx, err := fetch_tx_by_hash_with_retry(hash)
					if err != nil {
						return nil, err
					}
					ret = append(ret, tx)
					continue
				}
				return nil, elem.Error
			}

			tx, is_ok := elem.Result.(*types.Transaction)
			if !is_ok || tx == nil {
				return nil, fmt.Errorf("unexpected tx result: %#v", elem.Result)
			}
			ret = append(ret, tx)
		}
		return ret, nil
	}

	// Construct batches
	const MAX_BATCH_SIZE = 20
	for i := 0; i < len(logs); i += MAX_BATCH_SIZE {
		// Compute batch set upper-bound
		ii := i + MAX_BATCH_SIZE
		if ii > len(logs) {
			ii = len(logs)
		}

		fmt.Printf("===================================\ni: %d\n======================================\n\n", i)
		fmt.Printf("Naive contract: fetching tx data %d - %d; total txs is %d\n", i, ii, len(logs))

		// Prepare a batch for RPC-ing
		batch := []rpc.BatchElem{}
		// Index to let us map back from tx hash to update the logs
		logs_by_txhash := make(map[common.Hash]EthereumEventLog)
		for _, l := range logs[i:ii] {
			batch = append(batch,
				rpc.BatchElem{
					Method: "eth_getTransactionByHash",
					Args:   []interface{}{l.TxHash},
					Result: new(types.Transaction), // Use go-ethereum's Transaction struct
				},
			)

			// Also build the index of tx hashes to logs, while we're here
			logs_by_txhash[l.TxHash] = l
		}

		// Execute the batch
		txs, err := do_batched_rpc(batch)
		if err != nil {
			log.Fatalf("Failed to fetch tx data: %v", err)
		}

		// Save the result in the DB
		for _, tx := range txs {
			eventLog, is_ok := logs_by_txhash[tx.Hash()]
			if !is_ok {
				panic(tx.Hash())
			}
			eventLog.Data = tx.Data()
			db.SmuggleNaiveBatchDataIntoEvent(eventLog)

			if signer != nil && !db.HasTransaction(tx.Hash()) {
				receipt, err := fetchReceiptWithRetry(client, tx.Hash())
				if err != nil {
					log.Printf("naive tx metadata: receipt fetch failed %s: %v", tx.Hash().Hex(), err)
				} else {
					from, err := types.Sender(signer, tx)
					if err != nil {
						log.Printf("naive tx metadata: sender recovery failed %s: %v", tx.Hash().Hex(), err)
					} else {
						to := common.Address{}
						if tx.To() != nil {
							to = *tx.To()
						}

						blockHash := eventLog.BlockHash
						blockNumber := eventLog.BlockNumber
						txIndex := uint(0)
						if receipt != nil {
							if receipt.BlockHash != (common.Hash{}) {
								blockHash = receipt.BlockHash
							}
							if receipt.BlockNumber != nil {
								blockNumber = receipt.BlockNumber.Uint64()
							}
							txIndex = receipt.TransactionIndex
						}

						db.SaveTransaction(TransactionMetadata{
							Hash:        tx.Hash(),
							BlockHash:   blockHash,
							BlockNumber: blockNumber,
							TxIndex:     txIndex,
							FromAddress: from,
							ToAddress:   to,
						})
					}
				}
			}
		}

		// Update latest-block-fetched
		// WTF: transactions do not expose a block number, and you can't get it with ethclient
		// See: https://github.com/ethereum/go-ethereum/issues/15210
		db.SetLatestContractBlockFetched(contract.ID, logs[ii-1].BlockNumber)

		time.Sleep(1 * time.Second)
	}
}
