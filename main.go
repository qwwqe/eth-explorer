package main

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/rpc"
)

type Config struct {
	DbHost           string        `env:"ETHEXPLORER_DB_HOST"`
	DbPort           string        `env:"ETHEXPLORER_DB_PORT"`
	DbUser           string        `env:"ETHEXPLORER_DB_USER"`
	DbPassword       string        `env:"ETHEXPLORER_DB_PASSWORD"`
	DbName           string        `env:"ETHEXPLORER_DB_NAME"`
	RpcNode          string        `env:"ETHEXPLORER_RPC_NODE"`
	HeaderBatchSize  int           `env:"ETHEXPLORER_HEADER_BATCH_SIZE"`
	TxBatchSize      int           `env:"ETHEXPLORER_TX_BATCH_SIZE"`
	LogBatchSize     int           `env:"ETHEXPLORER_LOG_BATCH_SIZE"`
	RateLimitValue   int           `env:"ETHEXPLORER_RATE_LIMIT_VALUE"`
	RateLimitSeconds time.Duration `env:"ETHEXPLORER_RATE_LIMIT_SECONDS"`
	ApiListenPort    string        `env:"ETHEXPLORER_API_LISTEN_PORT"`
}

func main() {
	config := &Config{
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

	repo := &BlockRepo{}

	if err := repo.Open(config); err != nil {
		panic(err)
	}

	fetcher, err := NewBlockFetcher(client, repo, config)
	if err != nil {
		panic(err)
	}

	restApi := NewRestServer(repo)

	errChan := make(chan error)

	go func() {
		errChan <- fetcher.Fetch()
	}()

	go func() {
		errChan <- restApi.Start(config.ApiListenPort)
	}()

	panic(<-errChan)
}
