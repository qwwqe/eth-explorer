package main

import (
	"database/sql"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/core/types"
	_ "github.com/go-sql-driver/mysql"
)

type BlockRepo struct {
	db *sql.DB
}

func (r *BlockRepo) Open(config Config) error {
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

func (r *BlockRepo) SaveBlocks(blocks []*types.Block) error {
	values := []interface{}{}
	var b strings.Builder

	b.WriteString(`INSERT INTO blocks (number, hash, parentHash, timestamp) VALUES `)

	for i, v := range blocks {
		fmt.Fprintf(&b, "(?, ?, ?, ?)")
		if i < len(blocks)-1 {
			fmt.Fprintf(&b, ",")
		}
		fmt.Fprintf(&b, " ")
		values = append(values, v.Number().Int64(), v.Hash().String(), v.ParentHash().String(), v.Time())
	}

	q := b.String()

	_, err := r.db.Exec(q, values...)

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

// todo: stub
func (r *BlockRepo) MostRecentGap() (*big.Int, *big.Int, error) {
	return nil, nil, nil
}
