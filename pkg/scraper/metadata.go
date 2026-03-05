package scraper

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	. "go-azimuth/pkg/db"
)

const metadataRPCTimeout = 15 * time.Second

func EnsureBlockAndTxMetadata(client *ethclient.Client, db DB, logs []EthereumEventLog) {
	if len(logs) == 0 {
		return
	}

	chainID, err := client.ChainID(context.Background())
	var signer types.Signer
	if err != nil {
		log.Printf("metadata: failed to fetch chain ID (skipping tx metadata): %v", err)
	} else {
		signer = types.LatestSignerForChainID(chainID)
	}

	blockSeen := make(map[common.Hash]bool)
	txSeen := make(map[common.Hash]bool)

	for i, l := range logs {
		if i%100 == 0 {
			log.Printf("metadata: processing %d/%d", i, len(logs))
		}
		if !blockSeen[l.BlockHash] {
			blockSeen[l.BlockHash] = true
			ensureBlockMetadata(client, db, l.BlockHash)
		}

		if signer != nil && !txSeen[l.TxHash] {
			txSeen[l.TxHash] = true
			ensureTransactionMetadata(client, db, signer, l)
		}
	}
}

func ensureBlockMetadata(client *ethclient.Client, db DB, hash common.Hash) {
	if db.HasBlock(hash) {
		return
	}

	header, err := fetchHeaderWithRetry(client, hash)
	if err != nil {
		log.Printf("metadata: failed to fetch block header %s: %v", hash.Hex(), err)
		return
	}

	db.SaveBlock(BlockMetadata{
		Hash:       header.Hash(),
		Number:     header.Number.Uint64(),
		ParentHash: header.ParentHash,
		Timestamp:  header.Time,
	})
}

func ensureTransactionMetadata(client *ethclient.Client, db DB, signer types.Signer, logEntry EthereumEventLog) {
	if db.HasTransaction(logEntry.TxHash) {
		return
	}

	tx, err := fetchTransactionWithRetry(client, logEntry.TxHash)
	if err != nil {
		log.Printf("metadata: failed to fetch tx %s: %v", logEntry.TxHash.Hex(), err)
		return
	}

	receipt, err := fetchReceiptWithRetry(client, logEntry.TxHash)
	if err != nil {
		log.Printf("metadata: failed to fetch receipt %s: %v", logEntry.TxHash.Hex(), err)
		return
	}

	from, err := types.Sender(signer, tx)
	if err != nil {
		log.Printf("metadata: failed to recover sender %s: %v", logEntry.TxHash.Hex(), err)
		return
	}

	to := common.Address{}
	if tx.To() != nil {
		to = *tx.To()
	}

	blockHash := logEntry.BlockHash
	blockNumber := logEntry.BlockNumber
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

func fetchHeaderWithRetry(client *ethclient.Client, hash common.Hash) (*types.Header, error) {
	const maxRetries = 5
	backoff := 1 * time.Second
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), metadataRPCTimeout)
		header, err := client.HeaderByHash(ctx, hash)
		cancel()
		if err == nil && header != nil {
			return header, nil
		}
		if err == nil && header == nil {
			err = fmt.Errorf("missing header for %s", hash.Hex())
		}
		if !isRetryableRPCError(err) {
			return nil, err
		}
		lastErr = err
		time.Sleep(backoff)
		if backoff < 10*time.Second {
			backoff *= 2
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}
	}
	return nil, fmt.Errorf("failed to fetch header %s after %d attempts: %w", hash.Hex(), maxRetries, lastErr)
}

func fetchTransactionWithRetry(client *ethclient.Client, hash common.Hash) (*types.Transaction, error) {
	const maxRetries = 5
	backoff := 1 * time.Second
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), metadataRPCTimeout)
		tx, _, err := client.TransactionByHash(ctx, hash)
		cancel()
		if err == nil && tx != nil {
			return tx, nil
		}
		if err == nil && tx == nil {
			err = fmt.Errorf("missing tx for %s", hash.Hex())
		}
		if !isRetryableRPCError(err) {
			return nil, err
		}
		lastErr = err
		time.Sleep(backoff)
		if backoff < 10*time.Second {
			backoff *= 2
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}
	}
	return nil, fmt.Errorf("failed to fetch tx %s after %d attempts: %w", hash.Hex(), maxRetries, lastErr)
}

func fetchReceiptWithRetry(client *ethclient.Client, hash common.Hash) (*types.Receipt, error) {
	const maxRetries = 5
	backoff := 1 * time.Second
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), metadataRPCTimeout)
		receipt, err := client.TransactionReceipt(ctx, hash)
		cancel()
		if err == nil && receipt != nil {
			return receipt, nil
		}
		if err == nil && receipt == nil {
			err = fmt.Errorf("missing receipt for %s", hash.Hex())
		}
		if !isRetryableRPCError(err) {
			return nil, err
		}
		lastErr = err
		time.Sleep(backoff)
		if backoff < 10*time.Second {
			backoff *= 2
			if backoff > 10*time.Second {
				backoff = 10 * time.Second
			}
		}
	}
	return nil, fmt.Errorf("failed to fetch receipt %s after %d attempts: %w", hash.Hex(), maxRetries, lastErr)
}

func isRetryableRPCError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	if _, is_temp_unavailable := check_error(err); is_temp_unavailable {
		return true
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "response batch did not contain a response") ||
		strings.Contains(errMsg, "rate limit") ||
		strings.Contains(errMsg, "Too Many Requests") ||
		strings.Contains(errMsg, "temporarily unavailable") ||
		strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "context deadline") ||
		strings.Contains(errMsg, "i/o timeout") ||
		strings.Contains(errMsg, "connection reset") ||
		strings.Contains(errMsg, "429")
}
