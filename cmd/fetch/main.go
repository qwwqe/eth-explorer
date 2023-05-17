package main

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/qwwqe/eth-explorer/pkg/common"
	"github.com/qwwqe/eth-explorer/pkg/fetcher"
	"github.com/qwwqe/eth-explorer/pkg/repo"
)

func main() {
	config := &common.Config{
		DbHost:           "127.0.0.1",
		DbPort:           "3306",
		DbUser:           "eth",
		DbPassword:       "eth",
		DbName:           "eth",
		RpcNode:          "https://data-seed-prebsc-1-s1.binance.org:8545/",
		HeaderBatchSize:  250,
		TxBatchSize:      100,
		LogBatchSize:     100,
		RateLimitValue:   10000,
		RateLimitSeconds: time.Minute * 5,
		ApiListenPort:    "8080",
	}

	client, err := rpc.DialContext(context.TODO(), config.RpcNode)
	if err != nil {
		panic(err)
	}

	repo := &repo.BlockRepo{}

	if err := repo.Open(config); err != nil {
		panic(err)
	}

	fetcher, err := fetcher.NewBlockFetcher(client, repo, config)
	if err != nil {
		panic(err)
	}

	panic(fetcher.Fetch())
}
