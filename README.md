# Paqus Node

Rust full node for the Paqus testnet. It handles LMDB chain storage, mining,
RPC, peer sync, gateway discovery, mempool validation, and transaction indexing.

## Quick Start

```bash
cd ../paqus-wallet && cargo run -- new wallet.json
cd ../paqus-node
cargo run -- node init ./data/paqus
cargo run -- node run ./data/paqus --wallet ../paqus-wallet/wallet.json
```

Check the node from another terminal:

```bash
curl http://127.0.0.1:6666/status
```

Run with mining:

```bash
cargo run -- node run ./data/paqus --wallet ../paqus-wallet/wallet.json --mine
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

Wallet CLI lives in `../paqus-wallet`.

Create a wallet:

```bash
cd ../paqus-wallet
cargo run -- new wallet.json
```

Print the secret key too:

```bash
cargo run -- new wallet.json --show-secret
```

Derive address from a secret key:

```bash
cargo run -- address <secret-key-hex>
```

Check balance:

```bash
cargo run -- balance <address> --rpc 127.0.0.1:6666
```

Send a transaction:

```bash
cargo run -- send <address> 10 --wallet wallet.json
```

Useful `wallet send` options:

```text
--wallet <path>
--fee <units>
--nonce <n>
--rpc <host:port>
```

The sender chooses the transaction fee with `--fee`. The node may reject or
expire transactions from its mempool based on local relay policy, but a low fee
does not make an otherwise valid transaction invalid by consensus.

Advanced form for printing signed transaction hex without broadcasting:

```bash
cargo run -- send --wallet wallet.json --to <address> --amount 10
```

Broadcast the advanced form to the node RPC with `--submit`:

```bash
cargo run -- send \
  --wallet wallet.json \
  --to <address> \
  --amount 10 \
  --submit
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
  --listen 0.0.0.0:5555 \
  --listen '[::]:5555' \
  --rpc-listen 127.0.0.1:6666 \
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
--miner <address>
--miner-secret-key <secret-key-hex>
```

`--listen` and `--public-addr` can be repeated. Use one IPv4 address and one
IPv6 address when the node should accept and announce both address families.

Addresses are normally displayed as uppercase `PX1...` wallet addresses.
Legacy 20-byte hex addresses are still accepted for older scripts.

## Peers

Paqus nodes do not need a gateway for a small network. Start with one known
peer, then let the node save and reuse the peer cache:

- `--peer <host:port>` manually connects to a known node.
- `./data/paqus/peers.json` stores manual and learned peers.
- `--gateway <host:port>` is optional bootstrap only, for later/public networks.

For IPv6 socket addresses, wrap the IP in brackets:

```text
[2001:db8::10]:5555
```

Run a public node without a gateway:

```bash
cargo run -- node run ./data/paqus \
  --listen 0.0.0.0:5555 \
  --listen '[::]:5555' \
  --rpc-listen 127.0.0.1:6666 \
  --public-addr 182.253.xxx.xxx:5555 \
  --public-addr '[YOUR_PUBLIC_IPV6]:5555' \
  --wallet wallet.json \
  --mine
```

`--listen` is the local bind address. `0.0.0.0:5555` listens on all IPv4
interfaces, and `[::]:5555` listens on all IPv6 interfaces. `--public-addr` is
the reachable address that the node announces to peers, so it must use your
public IPv4/IPv6 address or DNS name and the P2P port `5555`.

After the P2P version handshake, nodes advertise their configured
`--public-addr` values through peer exchange. A public bootstrap node can
therefore learn and cache reachable peers that connect to it, then share those
peers with later nodes through `GetPeers` without requiring `paqus-gateway`.

Join with a manual peer:

```bash
cargo run -- node run ./data/paqus \
  --peer '[PEER_HOST]:5555' \
  --wallet wallet.json
```

Run without a gateway after `peers.json` is populated:

```bash
cargo run -- node run ./data/paqus \
  --listen 0.0.0.0:5555 \
  --listen '[::]:5555' \
  --rpc-listen 127.0.0.1:6666 \
  --public-addr 182.253.xxx.xxx:5555 \
  --public-addr '[YOUR_PUBLIC_IPV6]:5555' \
  --wallet wallet.json \
  --mine
```

Nodes exchange peer lists over the P2P protocol. After a node starts with a
manual `--peer` or learns peers from another node, it caches them in
`./data/paqus/peers.json` by default:

```json
{
  "peers": [
    "[2001:db8::20]:5555"
  ]
}
```

On the next startup, the node loads this cache, reconnects to known peers, and
asks them for more peers. Use `--peers-file <path>` to choose another cache
path.

Peer sync keeps one outbound TCP connection open per known peer and reuses it
for version handshake, tip checks, peer discovery, and block requests. Inbound
connections can also serve multiple messages before closing, so normal peer sync
does not create a new TCP connection for every individual request.

Gateway discovery is still available with `--gateway <host:port>`, but it is
off by default and not required while the network is still operated with known
manual peers.

## Mining

When `--mine` is used together with `--peer` or `--gateway`, mining is gated by
network sync. The node must complete at least one successful peer handshake, must
not see a peer with a higher tip, and must have no pending sync/orphan work
before it can produce a block. While waiting, logs show reasons such as
`handshake_pending`, `peer_ahead`, or `sync_pending`.

Mining uses the current node timestamp when preparing candidate blocks. Blocks
are validated against parent timestamp, local future-time tolerance, proof of
work, state root, coinbase, checkpoint policy, and transaction validity.

If mining is skipped because the mempool is empty, submit a transaction through
RPC or connect to peers where transactions are flowing.

## Mempool Fee Policy

Default relay policy:

```text
min_relay_fee = 1
market_fee = 2
low_fee_expiry_secs = 1800
mempool_expiry_secs = 86400
```

Transactions with fee below `min_relay_fee` are rejected by this node. The
effective floor is always at least `1`, so fee `0` is not relayed. Transactions
with fee below `market_fee` can stay pending for up to `low_fee_expiry_secs`
(30 minutes by default). Transactions at or above `market_fee` can stay pending
for up to `mempool_expiry_secs` (1 day by default).

Operators can tune the policy without changing consensus:

```text
--min-relay-fee <units>
--market-fee <units>
--low-fee-expiry-secs <seconds>
--mempool-expiry-secs <seconds>
```

## RPC

By default, keep RPC local:

```bash
curl http://127.0.0.1:6666/health
curl http://127.0.0.1:6666/status
curl http://127.0.0.1:6666/peers
curl http://127.0.0.1:6666/chain
curl http://127.0.0.1:6666/balance/<address>
curl http://127.0.0.1:6666/blocks/latest
curl http://127.0.0.1:6666/blocks/<height>
curl http://127.0.0.1:6666/blocks/hash/<block-hash>
curl http://127.0.0.1:6666/tx/<tx-hash>
curl http://127.0.0.1:6666/address/<address>
curl http://127.0.0.1:6666/accounts
curl http://127.0.0.1:6666/mempool
```

To expose RPC on IPv6 for a remote wallet, bind to all IPv6 interfaces:

```bash
paqus-node node run ./data/paqus --rpc-listen '[::]:6666'
```

Check that the node is listening publicly:

```bash
ss -ltnp | grep 6666
```

Expected output should show `*:6666` or `[::]:6666`.

From another machine, use the server's real IPv6 address in brackets:

```bash
curl 'http://[2404:8000:1044:4d8:1202:b5ff:feb0:7020]:6666/health'
```

Then point `paqus-wallet` at the same endpoint:

```bash
PAQUS_RPC_ADDR='[2404:8000:1044:4d8:1202:b5ff:feb0:7020]:6666' cargo run
```

Keep public RPC access limited when possible.

Submit signed transaction hex:

```bash
curl -X POST http://127.0.0.1:6666/tx \
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
