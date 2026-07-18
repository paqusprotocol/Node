# Paqus Node

Rust full node for the Paqus testnet. It handles LMDB chain storage, mining,
RPC, peer sync, gateway discovery, mempool validation, and transaction indexing.

## Quick Start

```bash
cd ../wallet-cli && cargo run -- new wallet.json
cd ../full-node
cargo run -- node init ./data/paqus
cargo run -- node run ./data/paqus --miner <PX1_MINER_ADDRESS>
```

Check the node from another terminal:

```bash
curl http://127.0.0.1:6666/status
```

Run with mining:

```bash
cargo run -- node run ./data/paqus --miner <PX1_MINER_ADDRESS> --mine
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

## Database maintenance

Stop every process using the database before backup or restore. Check integrity
and create a validated LMDB snapshot with:

```bash
full-node node db check ./data/paqus
full-node node db backup ./data/paqus ./backup/paqus-$(date +%Y%m%d)
```

Restore always targets a new, non-existent directory and validates the restored
chain before reporting success:

```bash
full-node node db restore ./backup/paqus-20260717 ./data/paqus-restored
```

For an upgrade, back up first, deploy the new binary, run `node db check`, and
start against the existing database only when its storage version is supported.
For rollback, stop the node and restore the pre-upgrade backup into a new path;
never copy files over a live or existing LMDB environment.

## eCash Mempool Reservations

`runtime::mempool::ExtensionMempool` accepts eCash transactions. eCash deposit
inputs reserve their complete
32-byte `CashCoinId`; a second pending transaction using the same bearer coin is
rejected even when signed by a different wallet. Removing or confirming the
transaction releases its reservation. Reservations are node policy, while
`PendingRedeem` in the ledger is the consensus lock after block inclusion.

## Unified Transaction Pipeline

P2P and inventory messages carry `SignedProtocolTransaction`, a canonical
envelope covering transfers and eCash. The existing
transfer pool retains nonce-aware fee ordering. The extension pool permits one
pending transaction per signer, preventing ambiguous cross-family nonce order,
and rejects a signer already present in the transfer pool.

Miner candidate construction selects both pools by fee per virtual byte, fills
the matching block-v2 lanes without exceeding serialized-size or weight limits,
recomputes combined fees and both Merkle roots, and stages the complete protocol
state root. Confirmed and disconnected transactions are removed or reinserted
through the same family-aware path. LMDB keeps logical `txid` and exact-witness
`wtxid` indexes; persisted protocol state includes eCash journals and events.

## Protocol Event RPC

Successful canonical state transitions are persisted as domain-separated
`ProtocolEvent` receipts. LMDB indexes events by ID, block, transaction, and
affected address. Reorgs atomically rebuild these indexes from the winning
ledger state.

```text
GET /events/{event_id}
GET /blocks/{height}/events
GET /tx/{transaction_hash}/events
GET /address/{address}/events
```

Event-list routes accept optional query parameters:

```text
offset=0
limit=100                  # 1..500
kind=ecash_deposited
from_height=100
to_height=200
```

Filters may be combined. List responses contain `total`, `offset`, `limit`, and
the paginated `events`; each event includes its canonical ID and typed payload.
Unknown event kinds, invalid height ranges, and out-of-range limits return HTTP
400. Storage schema version 1 is required.

### Finalized event stream

Explorers and wallets can subscribe with Server-Sent Events:

```text
GET /events/stream
GET /events/stream?from_height=100
GET /events/stream?kind=ecash_deposited
GET /events/stream?address=PAQUS1...
```

`from_height`, `kind`, and `address` may be combined. Without `from_height`, the
connection starts after the current finalized height and receives only new
events. The stream intentionally waits for `FINALITY_DEPTH` confirmations so
consumers never treat a short-lived fork as a final receipt.

Each SSE message uses the canonical event ID as `id`, the snake-case protocol
event kind as `event`, and the same JSON event receipt as `data`. A heartbeat is
sent every 15 seconds, while finalized blocks are polled once per second.

## Menu

```bash
cargo run
```

Equivalent explicit command:

```bash
cargo run -- menu
```

## Wallet

Wallet CLI lives in `../wallet-cli`.

Create a wallet:

```bash
cd ../wallet-cli
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
  --miner <PX1_MINER_ADDRESS>
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
  --miner <PX1_MINER_ADDRESS> \
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
  --miner <PX1_MINER_ADDRESS>
```

Run without a gateway after `peers.json` is populated:

```bash
cargo run -- node run ./data/paqus \
  --listen 0.0.0.0:5555 \
  --listen '[::]:5555' \
  --rpc-listen 127.0.0.1:6666 \
  --public-addr 182.253.xxx.xxx:5555 \
  --public-addr '[YOUR_PUBLIC_IPV6]:5555' \
  --miner <PX1_MINER_ADDRESS> \
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

Mining uses SHA3-512 and continuously scans nonce ranges in bounded batches so
the node can keep processing peers between batches. Mining uses the current node
timestamp when preparing candidate blocks. Blocks
are validated against parent timestamp, local future-time tolerance, proof of
work, state root, coinbase, checkpoint policy, and transaction validity.

Coinbase-only blocks are valid, so mining continues when the mempool is empty.
The default initial difficulty is calibrated for roughly one reference CPU core;
per-block ASERT adjusts it toward the one-minute target.

External miners can request and submit canonical block jobs through RPC:

```text
GET  /mining/template?miner=<PX1_MINER_ADDRESS>
POST /mining/submit  {"block":"<canonical-block-hex>"}
```

The template response includes `job_id`, canonical block bytes, height, parent,
difficulty, and `sha3-512` algorithm identity. A submitted block is fully
validated, stored, and announced to peers.

Run the workspace reference miner against the node RPC:

```bash
cd /home/debian/PaqusBlockchain
cargo run -p miner-cli --release -- \
  --backend auto \
  --miner <PX1_MINER_ADDRESS> \
  --rpc 127.0.0.1:6666
```

It uses all available CPU threads by default. Use `--threads` and `--batch` to
tune worker count and the nonce range scanned before refreshing a potentially
stale job.

For a machine with a vendor OpenCL runtime:

```bash
cargo run -p miner-cli --release -- \
  --backend opencl \
  --miner <PX1_MINER_ADDRESS> \
  --rpc 127.0.0.1:6666
```

Start a pool gateway in front of the node:

```bash
cargo run -p pool-server --release -- \
  --listen 0.0.0.0:3333 \
  --rpc 127.0.0.1:6666 \
  --pool-address <PX1_POOL_COINBASE_ADDRESS> \
  --share-difficulty 20
```

Workers connect with `miner-cli --pool <host>:3333 --worker <name>`. The
`paqus-stratum/1` gateway validates lower-difficulty shares locally and forwards
network-difficulty blocks to the full-node.

## Mempool Fee Policy

Default relay policy:

```text
min_relay_fee = 1   # units per KiB
market_fee = 2      # units per KiB
low_fee_expiry_secs = 1800
mempool_expiry_secs = 86400
```

`min_relay_fee` and `market_fee` are fee rates in units per KiB of serialized
transaction virtual size. The required fee is
`ceil(virtual_size * rate / 1024)`.
Transactions below the required relay fee are rejected by this node. The
effective relay rate floor is always at least `1`, so fee `0` is not relayed.
The market fee is dynamic: the configured `market_fee` is the base rate, and the
effective market rate rises with local mempool pressure. Pressure is the higher
of byte occupancy (`mempool_bytes / max_mempool_bytes`) and transaction-count
occupancy (`mempool_txs / max_mempool_txs`). At full pressure, the effective
market rate can rise by up to `8x` over the base rate. Transactions below the
current dynamic market fee can stay pending for up to `low_fee_expiry_secs`
(30 minutes by default). Transactions at or above the dynamic market fee can
stay pending for up to `mempool_expiry_secs` (1 day by default). Candidate block
selection prioritizes transaction fee rate while preserving sender nonce order.
Miners may set their own candidate-block floor with `miner_min_fee_rate` or
`--miner-min-fee-rate`; when omitted, mining follows the current dynamic market
fee.

Operators can tune the policy without changing consensus:

```text
--min-relay-fee <units-per-kib>
--market-fee <units-per-kib>
--miner-min-fee-rate <units-per-kib>
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
curl http://127.0.0.1:6666/tx/<txid-or-wtxid>
curl http://127.0.0.1:6666/address/<address>
curl http://127.0.0.1:6666/accounts
curl http://127.0.0.1:6666/mempool
```

To expose RPC on IPv6 for a remote wallet, bind to all IPv6 interfaces:

```bash
full-node node run ./data/paqus --rpc-listen '[::]:6666'
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

Then point `wallet-cli` at the same endpoint:

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

Block, address, mempool, and transaction responses use one family-aware
transaction shape. It includes `family`, `operation`, `txid`, `wtxid`, signer,
witness addresses, validity window, and SegWit size accounting for transfers
and eCash transactions.

## Recent Changes

- Uses the local `../core` crate path.
- Exposes `confirmation_depth` and `finality_depth` separately through node info.
- Uses `CONFIRMATION_DEPTH` for available balance, while hard finality remains a reorg boundary.
- Validates canonical blocks again when storing or loading them from LMDB.
- Stores both `txid` and `wtxid` transaction indexes plus address indexes.
- Supports gateway-based peer discovery and manual bootstrap peers.
- Supports wallet transaction creation, signing, and RPC submission.
# Protocol Transaction RPC

Submit a Borsh-encoded `SignedProtocolTransaction` envelope as hex:

```text
POST /protocol/transaction
```

The accepted envelope is announced through the unified P2P transaction gossip
message. Existing `/tx`, `/transaction`, and `/ecash/tx` routes remain available
for compatibility.

# eCash RPC

The node validates signed eCash v1 transactions in the unified extension pool, which
reserves every deposit `coin_id` against competing deposits:

```text
POST /ecash/tx       {"tx":"signed-ecash-transaction-hex"}
GET  /ecash/mempool
```

Deposit authorization is verified by `core` and is bound to the intended
recipient. The bearer-file opening secret is never accepted by these RPCs.
