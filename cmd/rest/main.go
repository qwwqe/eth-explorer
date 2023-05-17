package main

import (
	"github.com/qwwqe/eth-explorer/pkg/common"
	"github.com/qwwqe/eth-explorer/pkg/config"
	"github.com/qwwqe/eth-explorer/pkg/repo"
	"github.com/qwwqe/eth-explorer/pkg/rest"
)

func main() {
	config, err := config.CreateFromEnv[common.Config]()
	if err != nil {
		panic(err)
	}

	repo := &repo.BlockRepo{}

	if err := repo.Open(config); err != nil {
		panic(err)
	}

	restApi := rest.NewRestServer(repo)

	panic(restApi.Start(config.ApiListenPort))
}
