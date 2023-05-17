package main

import (
	"strconv"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type ApiServer struct {
	blockRepo *BlockRepo
	echo      *echo.Echo
}

func NewRestServer(repo *BlockRepo) *ApiServer {
	s := ApiServer{}

	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.CORS())
	e.Use(middleware.Gzip())

	e.GET("/", s.getBlocksHandler)

	s.blockRepo = repo
	s.echo = e

	return &s
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

	return c.JSON(200, headers)
}
