# Ethereum Explorer

This project is a combination of two things: a program to index blocks and their related transactional data on ethereum networks; and a simple API server to query such data.

## Introduction

The indexing program can be run as follows:

```
$ go run cmd/fetch/main.go
```

The API server can be run as follows:

```
$ go run cmd/rest/main.go
```

Both applications require a number of environment variables to be configured in order to work correctly. By default, both applications will read from a [.env](.env) file in the root of the project (example provided in this repo). An instance of MariaDB or MySQL is also required for the applications to function properly. To this extent, a [Docker compose file](docker-compose.yml) has been included for convenience.

## Environment variables

Below is a selection of explanations for certain environment variables read by the application.

`ETHEXPLORER_HEADER_BATCH_SIZE` - How many block headers to fetch in a single HTTP request.

`ETHEXPLORER_TX_BATCH_SIZE` - How many transactions to fetch in a single HTTP request.

`ETHEXPLORER_LOG_BATCH_SIZE` - How many transaction logs to fetch in a single HTTP request.

`ETHEXPLORER_RATE_LIMIT_VALUE` - The HTTP request rate limit of the provided RPC node.

`ETHEXPLORER_RATE_LIMIT_SECONDS` - The window of time in which the above rate limit is calculated.

## Indexing logic

With an empty database, the application will begin indexing blocks from the latest block header. It will then proceed to simultaneously gather any new blocks that are collated on the block chain, as well as older blocks that were collated before the oldest block known to the application.

When the application is started with a non-empty database, it will first index all blocks between the newest known block to the application and the newest block on the chain. When these blocks are fully indexed, the application will then continue to index older blocks once again.