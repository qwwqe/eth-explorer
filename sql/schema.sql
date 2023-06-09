CREATE TABLE IF NOT EXISTS blocks (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  number DECIMAL(65) UNIQUE,
  hash VARCHAR(66),
  parentHash VARCHAR(66) NOT NULL,
  timestamp BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  block_number DECIMAL(65) NOT NULL,
  hash VARCHAR(66) UNIQUE NOT NULL,
  from_address VARCHAR(42) NOT NULL,
  to_address VARCHAR(42),
  nonce DECIMAL(65) NOT NULL,
  input TEXT NOT NULL,
  value DECIMAL(65) NOT NULL,
  logs LONGBLOB,
  FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
);
