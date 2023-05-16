package main

import (
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

type Config struct {
	DbHost           string        `env:"ETHEXPLORER_DB_HOST"`
	DbPort           string        `env:"ETHEXPLORER_DB_PORT"`
	DbUser           string        `env:"ETHEXPLORER_DB_USER"`
	DbPassword       string        `env:"ETHEXPLORER_DB_PASSWORD"`
	DbName           string        `env:"ETHEXPLORER_DB_NAME"`
	RpcNode          string        `env:"ETHEXPLORER_RPC_NODE"`
	BatchSize        int           `env:"ETHEXPLORER_BATCH_SIZE"`
	RateLimitValue   int           `env:"ETHEXPLORER_RATE_LIMIT_VALUE"`
	RateLimitSeconds time.Duration `env:"ETHEXPLORER_RATE_LIMIT_SECONDS"`
}

func main() {
	config := &Config{
		DbHost:           "127.0.0.1",
		DbPort:           "3306",
		DbUser:           "eth",
		DbPassword:       "eth",
		DbName:           "eth",
		RpcNode:          "https://data-seed-prebsc-1-s1.binance.org:8545/",
		BatchSize:        25,
		RateLimitValue:   10000,
		RateLimitSeconds: time.Minute * 5,
	}

	// todo: 用rpcclient
	client, err := ethclient.Dial(config.RpcNode)
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

	if err := fetcher.Fetch(); err != nil {
		panic(err)
	}
}
