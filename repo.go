package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type BlockRepo struct {
	db *sql.DB
}

func (r *BlockRepo) Open(config *Config) error {
	db, err := sql.Open("mysql",
		fmt.Sprintf("%s:%s@tcp(%s:%s)/%s",
			config.DbUser,
			config.DbPassword,
			config.DbHost,
			config.DbPort,
			config.DbName,
		),
	)

	if err != nil {
		return err
	}

	r.db = db

	return nil
}

func (r *BlockRepo) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, nil)
}

func (r *BlockRepo) CommitTx(tx *sql.Tx) error {
	return tx.Commit()
}

func (r *BlockRepo) SaveBlocks(blocks []*BlockHeader) error {
	tx, err := r.BeginTx(context.TODO())
	if err != nil {
		return err
	}

	if err := r.SaveBlocksTx(tx, blocks); err != nil {
		return err
	}

	return r.CommitTx(tx)
}

func (r *BlockRepo) SaveBlocksTx(tx *sql.Tx, blocks []*BlockHeader) error {
	values := []interface{}{}
	var b strings.Builder

	b.WriteString(`INSERT INTO blocks (number, hash, parentHash, timestamp) VALUES `)

	for i, v := range blocks {
		fmt.Fprintf(&b, "(?, ?, ?, ?)")
		if i < len(blocks)-1 {
			fmt.Fprintf(&b, ",")
		}
		fmt.Fprintf(&b, " ")
		values = append(values, v.Number.Int64(), v.Hash.Hex(), v.ParentHash.Hex(), v.Time)
	}

	q := b.String()

	_, err := tx.Exec(q, values...)

	return err
}

func (r *BlockRepo) SaveTransactions(transactions []*Transaction) error {
	tx, err := r.BeginTx(context.TODO())
	if err != nil {
		return err
	}

	if err := r.SaveTransactionsTx(tx, transactions); err != nil {
		return err
	}

	return r.CommitTx(tx)
}

func (r *BlockRepo) SaveTransactionsTx(tx *sql.Tx, transactions []*Transaction) error {
	values := []interface{}{}
	var b strings.Builder

	b.WriteString(`INSERT INTO transactions (block_number, hash, from_address, to_address, nonce, input, value, logs) VALUES `)

	for i, t := range transactions {
		fmt.Fprintf(&b, "(?, ?, ?, ?, ?, ?, ?, ?)")
		if i < len(transactions)-1 {
			fmt.Fprintf(&b, ",")
		}
		fmt.Fprintf(&b, " ")

		logs, err := json.Marshal(t.Logs)
		if err != nil {
			return err
		}

		values = append(values,
			t.BlockNumber.Int64(), t.Hash.Hex(), t.FromAddress, t.ToAddress,
			t.Nonce.Int64(), t.Input, t.Value.Int64(), logs,
		)
	}

	q := b.String()

	_, err := tx.Exec(q, values...)

	return err
}

func (r *BlockRepo) NewestFetchedBlockNumber() (*big.Int, error) {
	q := `SELECT MAX(number) FROM blocks`
	row := r.db.QueryRow(q)

	// todo: deal with datatype mismatch
	var i sql.NullInt64

	if err := row.Scan(&i); err != nil {
		return nil, err
	}

	if i.Valid {
		return big.NewInt(i.Int64), nil
	}

	return nil, nil
}

func (r *BlockRepo) OldestFetchedBlockNumber() (*big.Int, error) {
	q := `SELECT MIN(number) FROM blocks`
	row := r.db.QueryRow(q)

	// todo: deal with datatype mismatch
	var i sql.NullInt64

	if err := row.Scan(&i); err != nil {
		return nil, err
	}

	if i.Valid {
		return big.NewInt(i.Int64), nil
	}

	return nil, nil
}

func (r *BlockRepo) MostRecentBlockHeaders(n int) ([]*BlockHeader, error) {
	q := `SELECT number, hash, parentHash, timestamp FROM blocks ORDER BY number DESC LIMIT ?`

	rows, err := r.db.Query(q, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	headers := []*BlockHeader{}

	for rows.Next() {
		var h BlockHeader
		var hash, parentHash []byte
		var number sql.NullInt64
		if err := rows.Scan(&number, &hash, &parentHash, &h.Time); err != nil {
			return nil, err
		}

		if err := h.Hash.UnmarshalText(hash); err != nil {
			return nil, err
		}

		if err := h.ParentHash.UnmarshalText(parentHash); err != nil {
			return nil, err
		}

		if number.Valid {
			h.Number = big.NewInt(number.Int64)
		}

		headers = append(headers, &h)
	}

	return headers, nil
}

func (r *BlockRepo) GetBlockHeader(n *big.Int) (*BlockHeader, error) {
	q := `SELECT b.number, b.hash, b.parentHash, b.timestamp, t.hash
	FROM blocks AS b
	JOIN transactions AS t
	ON b.number = t.block_number
	WHERE number = ?`

	rows, err := r.db.Query(q, n.Int64())
	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}
	defer rows.Close()

	var h *BlockHeader

	for rows.Next() {
		var timestamp uint64
		var hash, parentHash, transactionHash []byte
		var number sql.NullInt64
		if err := rows.Scan(&number, &hash, &parentHash, &timestamp, &transactionHash); err != nil {
			return nil, err
		}

		if h != nil {
			h.TransactionHashes = append(h.TransactionHashes, string(transactionHash))
			continue
		}

		h = new(BlockHeader)
		h.TransactionHashes = []string{}

		h.Time = timestamp

		if err := h.Hash.UnmarshalText(hash); err != nil {
			return nil, err
		}

		if err := h.ParentHash.UnmarshalText(parentHash); err != nil {
			return nil, err
		}

		if number.Valid {
			h.Number = big.NewInt(number.Int64)
		}

		h.TransactionHashes = append(h.TransactionHashes, string(transactionHash))
	}

	return h, nil
}

func (r *BlockRepo) GetTransaction(hash string) (*Transaction, error) {
	q := `SELECT hash, from_address, to_address, nonce, input, value, logs FROM transactions WHERE hash = ?`

	t := &Transaction{}

	var h []byte
	var nonce, value int64
	var logs []byte
	err := r.db.QueryRow(q, hash).Scan(&h, &t.FromAddress, &t.ToAddress, &nonce, &t.Input, &value, &logs)

	switch {
	case err == sql.ErrNoRows:
		return nil, nil
	case err != nil:
		return nil, err
	}

	if err := t.Hash.UnmarshalText(h); err != nil {
		return nil, err
	}

	t.Nonce = big.NewInt(nonce)
	t.Value = big.NewInt(value)

	if err := json.Unmarshal(logs, &t.Logs); err != nil {
		return nil, err
	}

	if t.Logs == nil {
		t.Logs = []TransactionLog{}
	}

	return t, nil

	// block_number DECIMAL(65) NOT NULL,
	// hash VARCHAR(66) UNIQUE NOT NULL,
	// from_address VARCHAR(42) NOT NULL,
	// to_address VARCHAR(42),
	// nonce DECIMAL(65) NOT NULL,
	// input TEXT NOT NULL,
	// value DECIMAL(65) NOT NULL,
	// logs LONGBLOB,
}
