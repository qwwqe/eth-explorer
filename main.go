package main

import (
	"bufio"
	"os"

	"github.com/ethereum/go-ethereum/ethclient"
)

var host = "https://data-seed-prebsc-1-s1.binance.org:8545/"

type Config struct {
	DbHost     string `env:"ETHEXPLORER_DB_HOST"`
	DbPort     string `env:"ETHEXPLORER_DB_PORT"`
	DbUser     string `env:"ETHEXPLORER_DB_USER"`
	DbPassword string `env:"ETHEXPLORER_DB_PASSWORD"`
	DbName     string `env:"ETHEXPLORER_DB_NAME"`
	RpcNode    string `env:"ETHEXPLORER_RPC_NODE"`
	BatchSize  int    `env:"ETHEXPLORER_BATCH_SIZE"`
}

func main() {
	// todo: 用rpcclient
	client, err := ethclient.Dial(host)
	if err != nil {
		panic(err)
	}

	config := &Config{
		DbHost:     "127.0.0.1",
		DbPort:     "3306",
		DbUser:     "eth",
		DbPassword: "eth",
		DbName:     "eth",
		RpcNode:    "https://data-seed-prebsc-1-s1.binance.org:8545/",
		BatchSize:  20,
	}

	repo := &BlockRepo{}

	if err := repo.Open(config); err != nil {
		panic(err)
	}

	fetcher := NewBlockFetcher(client, repo, config)

	reader := bufio.NewReader(os.Stdin)

	for i := 1; true; i++ {
		if err := fetcher.FetchBlocks(); err != nil {
			panic(err)
		}

		reader.ReadLine()
	}
}
