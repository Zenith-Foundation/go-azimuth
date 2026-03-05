package db

import (
	"database/sql"
	"errors"

	"github.com/ethereum/go-ethereum/common"
)

type BlockMetadata struct {
	Hash       common.Hash `db:"hash"`
	Number     uint64      `db:"number"`
	ParentHash common.Hash `db:"parent_hash"`
	Timestamp  uint64      `db:"timestamp"`
}

type TransactionMetadata struct {
	Hash        common.Hash    `db:"hash"`
	BlockHash   common.Hash    `db:"block_hash"`
	BlockNumber uint64         `db:"block_number"`
	TxIndex     uint           `db:"tx_index"`
	FromAddress common.Address `db:"from_address"`
	ToAddress   common.Address `db:"to_address"`
}

func (db *DB) SaveBlock(meta BlockMetadata) {
	_, err := db.DB.NamedExec(`
		insert into blocks (hash, number, parent_hash, timestamp)
		     values (:hash, :number, :parent_hash, :timestamp)
		on conflict (hash) do nothing
	`, meta)
	if err != nil {
		panic(err)
	}
}

func (db *DB) SaveTransaction(meta TransactionMetadata) {
	_, err := db.DB.NamedExec(`
		insert into transactions (hash, block_hash, block_number, tx_index, from_address, to_address)
		     values (:hash, :block_hash, :block_number, :tx_index, :from_address, :to_address)
		on conflict (hash) do nothing
	`, meta)
	if err != nil {
		panic(err)
	}
}

func (db *DB) HasBlock(hash common.Hash) bool {
	var exists int
	err := db.DB.Get(&exists, `select 1 from blocks where hash = ? limit 1`, hash)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		panic(err)
	}
	return true
}

func (db *DB) HasTransaction(hash common.Hash) bool {
	var exists int
	err := db.DB.Get(&exists, `select 1 from transactions where hash = ? limit 1`, hash)
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		panic(err)
	}
	return true
}
