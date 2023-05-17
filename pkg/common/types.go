package common

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
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

type BlockHeader struct {
	Number            *big.Int    `json:"number"`
	ParentHash        common.Hash `json:"parentHash"`
	Hash              common.Hash `json:"hash"`
	Time              uint64      `json:"timestamp"`
	TransactionHashes []string    `json:"transactions"`
}

func (h *BlockHeader) UnmarshalJSON(b []byte) error {
	type blockHeader struct {
		Number            *json.RawMessage `json:"number"`
		ParentHash        common.Hash      `json:"parentHash"`
		Hash              common.Hash      `json:"hash"`
		Time              *json.RawMessage `json:"timestamp"`
		TransactionHashes []string         `json:"transactions"`
	}

	var bh blockHeader
	if err := json.Unmarshal(b, &bh); err != nil {
		return err
	}

	h.ParentHash = bh.ParentHash
	h.Hash = bh.Hash
	h.TransactionHashes = bh.TransactionHashes

	if bh.Number != nil && string(*bh.Number) != "null" {
		s := strings.Trim(string(*bh.Number), `"`)

		i, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		h.Number = i
	}

	if bh.Time != nil && string(*bh.Time) != "null" {
		s := strings.Trim(string(*bh.Time), `"`)

		t, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		h.Time = t.Uint64()
	}

	return nil
}

type Transaction struct {
	BlockNumber *big.Int         `json:"blockNumber"`
	Hash        common.Hash      `json:"hash"`
	FromAddress string           `json:"from"`
	ToAddress   string           `json:"to"`
	Nonce       *big.Int         `json:"nonce"`
	Value       *big.Int         `json:"value"`
	Input       string           `json:"data"`
	Logs        []TransactionLog `json:"logs"`
}

func (t *Transaction) UnmarshalJSON(b []byte) error {
	type transaction struct {
		BlockNumber *json.RawMessage `json:"blockNumber"`
		Hash        common.Hash      `json:"hash"`
		FromAddress string           `json:"from"`
		ToAddress   string           `json:"to"`
		Nonce       *json.RawMessage `json:"nonce"`
		Value       *json.RawMessage `json:"value"`
		Input       string           `json:"data"`
	}

	var tx transaction
	if err := json.Unmarshal(b, &tx); err != nil {
		return err
	}

	t.Hash = tx.Hash
	t.FromAddress = tx.FromAddress
	t.ToAddress = tx.ToAddress
	t.Input = tx.Input

	if tx.BlockNumber != nil && string(*tx.BlockNumber) != "null" {
		s := strings.Trim(string(*tx.BlockNumber), `"`)

		i, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		t.BlockNumber = i
	}

	if tx.Nonce != nil && string(*tx.Nonce) != "null" {
		s := strings.Trim(string(*tx.Nonce), `"`)

		i, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		t.Nonce = i
	}

	if tx.Value != nil && string(*tx.Value) != "null" {
		s := strings.Trim(string(*tx.Value), `"`)

		i, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		t.Value = i
	}

	return nil
}

type TransactionReceipt struct {
	TransactionHash common.Hash      `json:"transactionHash"`
	Logs            []TransactionLog `json:"logs"`
}

type TransactionLog struct {
	Index *big.Int `json:"logIndex"`
	Data  string   `json:"data"`
}

func (l *TransactionLog) UnmarshalJSON(b []byte) error {
	type log struct {
		Index *json.RawMessage `json:"logIndex"`
		Data  string           `json:"data"`
	}

	var tl log
	if err := json.Unmarshal(b, &tl); err != nil {
		return err
	}

	l.Data = tl.Data

	if tl.Index != nil && string(*tl.Index) != "null" {

		s := strings.Trim(string(*tl.Index), `"`)

		i, ok := new(big.Int).SetString(s, 0)
		if !ok {
			return fmt.Errorf("Could not unmarshal `%s` into *big.Int", s)
		}

		l.Index = i
	}

	return nil
}
