# Paqus Node

Rust full node for the Paqus testnet. It handles LMDB chain storage, mining,
RPC, peer sync, gateway discovery, mempool validation, transaction indexing, and
wallet commands.

## Quick Start

```bash
cargo run -- wallet new wallet.json
cargo run -- node init ./data/paqus
cargo run -- node run ./data/paqus --wallet wallet.json
```

Check the node from another terminal:

```bash
curl http://127.0.0.1:9933/status
```

Run with mining:

```bash
cargo run -- node run ./data/paqus --wallet wallet.json --mine
```

Stop the default node:

```bash
touch ./data/paqus/STOP
```

## Files

Wallet files contain `secret_key`. Do not commit or share files such as
`wallet.json` or accidentally named wallet files like `8`.

Node storage uses LMDB:

```text
./data/paqus/
  data.mdb
  lock.mdb
```

If upgrading from an old database format, start fresh:

```bash
rm -rf ./data/paqus
```

## Menu

```bash
cargo run
```

Equivalent explicit command:

```bash
cargo run -- menu
```

## Wallet

Create a wallet:

```bash
cargo run -- wallet new wallet.json
```

Print the secret key too:

```bash
cargo run -- wallet new wallet.json --show-secret
```

Derive address from a secret key:

```bash
cargo run -- wallet address <secret-key-hex>
```

Check balance:

```bash
cargo run -- wallet balance <address-hex> [db-path]
```

Send a transaction:

```bash
cargo run -- wallet send <recipient-address-hex> 10
```

Useful `wallet send` options:

```text
--wallet <path>
--fee <units>
--nonce <n>
--rpc <host:port>
```

Advanced form for printing signed transaction hex without broadcasting:

```bash
cargo run -- wallet send --wallet wallet.json --to <recipient-address-hex> --amount 10
```

## Node

Show protocol and network info:

```bash
cargo run -- node info
```

Create the default config file:

```bash
cargo run -- node config
```

Run from `./data/paqus/node.json`:

```bash
cargo run -- node run
```

Run with explicit addresses:

```bash
cargo run -- node run ./data/paqus \
  --listen '[::]:30333' \
  --rpc-listen 127.0.0.1:9933 \
  --wallet wallet.json
```

Common `node run` options:

```text
--mine
--mine-interval-secs <seconds>
--mine-attempts <count>
--peer <host:port>
--peers-file <path>
--gateway <host:port>
--public-addr <host:port>
--miner <address-hex>
--miner-secret-key <secret-key-hex>
```

## Gateway And Peers

Run a public node that registers itself with a gateway:

```bash
cargo run -- node run ./data/paqus \
  --listen '[::]:30333' \
  --rpc-listen 127.0.0.1:9933 \
  --gateway '[GATEWAY_HOST]:8080' \
  --public-addr '[YOUR_PUBLIC_IPV6]:30333' \
  --wallet wallet.json \
  --mine
```

`--listen` is the local bind address. `[::]:30333` listens on all IPv6
interfaces and is usually enough. `--public-addr` is the reachable address that
the node announces to the gateway and peers, so it must use your public IP or
DNS name and the P2P port `30333`.

Run the gateway service from the sibling crate:

```bash
cd ../paqus-gateway
cargo run -- \
  --listen '[::]:8080' \
  --node-rpc 127.0.0.1:9933 \
  --allow-private-peers
```

Join through a gateway:

```bash
cargo run -- node run ./data/paqus \
  --listen '[::]:30333' \
  --rpc-listen 127.0.0.1:9933 \
  --gateway '[GATEWAY_HOST]:8080' \
  --public-addr '[YOUR_PUBLIC_IPV6]:30333' \
  --wallet wallet.json
```

Join with a manual peer:

```bash
cargo run -- node run ./data/paqus \
  --peer '[PEER_HOST]:30333' \
  --wallet wallet.json
```

## Mining

Mining uses the current node timestamp when preparing candidate blocks. Blocks
are validated against parent timestamp, local future-time tolerance, proof of
work, state root, coinbase, checkpoint policy, and transaction validity.

If mining is skipped because the mempool is empty, submit a transaction through
RPC or connect to peers where transactions are flowing.

## RPC

```bash
curl http://127.0.0.1:9933/health
curl http://127.0.0.1:9933/status
curl http://127.0.0.1:9933/peers
curl http://127.0.0.1:9933/chain
curl http://127.0.0.1:9933/balance/<address-hex>
curl http://127.0.0.1:9933/blocks/latest
curl http://127.0.0.1:9933/blocks/<height>
curl http://127.0.0.1:9933/blocks/hash/<block-hash>
curl http://127.0.0.1:9933/tx/<tx-hash>
curl http://127.0.0.1:9933/address/<address-hex>
curl http://127.0.0.1:9933/accounts
curl http://127.0.0.1:9933/mempool
```

Submit signed transaction hex:

```bash
curl -X POST http://127.0.0.1:9933/tx \
  -H 'content-type: application/json' \
  -d '{"tx":"<signed-transaction-hex>"}'
```

`POST /transaction` accepts the same body as `POST /tx`.

## Recent Changes

- Uses the local `../paqus-core` crate path.
- Exposes `confirmation_depth` and `finality_depth` separately through node info.
- Uses `CONFIRMATION_DEPTH` for available balance, while hard finality remains a reorg boundary.
- Stores canonical blocks, accounts, state snapshots, transaction indexes, and address transaction indexes in LMDB.
- Supports gateway-based peer discovery and manual bootstrap peers.
- Supports wallet transaction creation, signing, and RPC submission.
