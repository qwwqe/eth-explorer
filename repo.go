package main

import (
	"database/sql"
	"fmt"
	"math/big"

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

func (r *BlockRepo) LastFetchedBlockNumber() (*big.Int, error) {
	q := `SELECT MAX(number) FROM blocks`
	row := r.db.QueryRow(q)

	i := new(big.Int)

	if err := row.Scan(i); err != nil {
		return nil, err
	}

	return i, nil
}

// todo: stub
func (r *BlockRepo) MostRecentGap() (*big.Int, *big.Int, error) {
	return nil, nil, nil
}
