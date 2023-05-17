package main

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type ApiServer struct {
	blockRepo *BlockRepo
	echo      *echo.Echo
}

type GetBlocksResponse struct {
	Blocks []SimpleBlockResponse `json:"blocks"`
}

type GetBlockResponse struct {
	SimpleBlockResponse
	TransactionHashes []string `json:"transactions"`
}

type SimpleBlockResponse struct {
	Number     *big.Int    `json:"block_num"`
	BlockHash  common.Hash `json:"block_hash"`
	ParentHash common.Hash `json:"parent_hash"`
	Time       uint64      `json:"block_time"`
}

func NewRestServer(repo *BlockRepo) *ApiServer {
	s := ApiServer{}

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.CORS())
	e.Use(middleware.Gzip())

	e.GET("/blocks", s.getBlocksHandler)
	e.GET("/blocks/:id", s.getBlockHandler)

	s.blockRepo = repo
	s.echo = e

	return &s
}

func (s *ApiServer) Start(port string) error {
	return s.echo.Start(fmt.Sprintf(":%s", port))
}

func (s *ApiServer) getBlocksHandler(c echo.Context) error {
	limitString := c.QueryParam("limit")

	limit, err := strconv.Atoi(limitString)
	if err != nil {
		return err
	}

	headers, err := s.blockRepo.MostRecentBlockHeaders(limit)
	if err != nil {
		return err
	}

	response := GetBlocksResponse{
		Blocks: make([]SimpleBlockResponse, 0, len(headers)),
	}

	for _, block := range headers {
		response.Blocks = append(response.Blocks, SimpleBlockResponse{
			Number:     block.Number,
			BlockHash:  block.Hash,
			ParentHash: block.ParentHash,
			Time:       block.Time,
		})
	}

	return c.JSON(200, response)
}

func (s *ApiServer) getBlockHandler(c echo.Context) error {
	numberString := c.Param("id")

	number, ok := new(big.Int).SetString(numberString, 10)
	if !ok {
		return fmt.Errorf("Invalid block id `%s`", numberString)
	}

	block, err := s.blockRepo.GetBlockHeader(number)
	if err != nil {
		return err
	}

	response := GetBlockResponse{
		SimpleBlockResponse: SimpleBlockResponse{
			Number:     block.Number,
			BlockHash:  block.Hash,
			ParentHash: block.ParentHash,
			Time:       block.Time,
		},
		TransactionHashes: block.TransactionHashes,
	}

	return c.JSON(200, response)
}
