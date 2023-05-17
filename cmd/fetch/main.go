package main

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/qwwqe/eth-explorer/pkg/common"
	"github.com/qwwqe/eth-explorer/pkg/config"
	"github.com/qwwqe/eth-explorer/pkg/fetcher"
	"github.com/qwwqe/eth-explorer/pkg/repo"
)

func main() {
	config, err := config.CreateFromEnv[common.Config]()
	if err != nil {
		panic(err)
	}

	fmt.Println(config)

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
