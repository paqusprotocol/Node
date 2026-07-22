# Paqus Node

Rust full node for the Paqus proof-of-work network. It handles LMDB storage,
mining, P2P networking, peer sync, mempool validation, RPC, explorer endpoints,
and QCash transaction submission.

The node is a normal console application. Daemon/service installers are not
part of this repository at the moment.

## Quick Start

Repository layout example:

```text
MyPaqus/
  Node/
  Wallet/
```

Create a wallet in the node directory:

```bash
cd MyPaqus/Wallet
cargo run -- new ../Node/wallet.json
```

Run the node:

```bash
cd ../Node
cargo run
```

When `wallet.json` exists in the node directory, the default `cargo run` starts
mining automatically. The node stores chain data in:

```text
./data/paqus
```

## Explicit Run

```bash
cargo run -- node run ./data/paqus \
  --wallet wallet.json \
  --mine
```

Useful options:

```text
--listen <host:port>
--public-addr <host:port>
--rpc-listen <host:port>
--peer <host:port>
--wallet <path>
--mine
--mine-attempts <count>
--mine-interval-secs <seconds>
--miner-min-fee-rate <paqus-per-vbyte>
```

`--mine-attempts 0` means continuous mining. Internally the node rebuilds the
candidate every `--mine-interval-secs` seconds so new mempool transactions can
be included.

## P2P Online

Only expose the P2P port publicly. Keep RPC local unless you deliberately put it
behind your own authentication/proxy layer.

Example IPv6 public node:

```bash
cargo run -- node run ./data/paqus \
  --listen '[::]:5555' \
  --public-addr '[YOUR_PUBLIC_IPV6]:5555' \
  --rpc-listen 127.0.0.1:6666 \
  --wallet wallet.json \
  --mine \
  --mine-attempts 0 \
  --mine-interval-secs 10
```

For IPv6 socket addresses, brackets are required:

```text
[2404:8000:1044:4d8:822b:f9ff:fee2:365]:5555
```

A peer can join with:

```bash
cargo run -- node run ./data/paqus \
  --peer '[BOOTSTRAP_IPV6]:5555' \
  --wallet wallet.json \
  --mine
```

Check peers:

```bash
curl http://127.0.0.1:6666/peers
```

## Mining

Mining uses SHA3-512 proof of work. The first miner on fresh storage mines
height `0`, which is the dynamic genesis block. Before genesis exists, status
can show:

```text
height=0 tip=none
```

That means the node has not mined or synchronized genesis yet.

Run with larger bounded batches:

```bash
cargo run -- node run ./data/paqus \
  --wallet wallet.json \
  --mine \
  --mine-attempts 5000000 \
  --mine-interval-secs 1
```

Run continuous mining with periodic candidate rebuild:

```bash
cargo run -- node run ./data/paqus \
  --wallet wallet.json \
  --mine \
  --mine-attempts 0 \
  --mine-interval-secs 10
```

## Consensus Parameters

Current defaults are printed at startup and through RPC:

```bash
curl http://127.0.0.1:6666/chain
```

Important values:

```text
Protocol version:          1
Storage version:           2
Target block time:         5 minutes
Transaction confirmation:  5 blocks
Hard finality:             50 blocks
Block reward maturity:     100 blocks
QCash maturity:            50 blocks
Block subsidy:             25 XPQ
Smallest unit:             1 XPQ = 1,000,000 paqus
```

## RPC

Common local endpoints:

```text
GET /health
GET /status
GET /chain
GET /stats
GET /peers
GET /mempool
GET /qcash/mempool
GET /qcash/coin/{coin_id}
GET /blocks/latest
GET /blocks/{height}
GET /blocks/hash/{hash}
GET /tx/{hash}
GET /address/{address}
POST /tx
POST /qcash/tx
```

Example:

```bash
curl http://127.0.0.1:6666/status
```

## QCash

The node validates and indexes QCash withdraw/deposit transactions. QCash deposit
inputs reserve their full `CashCoinId` in the extension mempool, preventing a
second pending spend of the same bearer coin.

The node currently permits one pending QCash/extension transaction per signer in
mempool. If a wallet already has a pending QCash transaction, another QCash
transaction from the same signer can be rejected until the first one confirms.

See the wallet repository QCash documentation for user-facing QCash behavior and
wallet commands.

## Database

Default storage:

```text
./data/paqus/
  data.mdb
  lock.mdb
  peers.json
```

If protocol/genesis identity changed during development, old storage may fail
validation. Reset only local chain data with:

```bash
rm -rf ./data/paqus
```

This does not delete `wallet.json` or cash files.

## Build

```bash
cargo check --tests
cargo build --release
```

For serious mining, release builds are much faster than `cargo run` debug
builds.
