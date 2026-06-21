# Paqus Node Commands

## Wallet

Create a new wallet:

```bash
cargo run -- wallet new wallet.json
```

Create a wallet and print the secret key:

```bash
cargo run -- wallet new wallet.json --show-secret
```

Get address from a secret key:

```bash
cargo run -- wallet address <secret-key-hex>
```

Check wallet balance:

```bash
cargo run -- wallet balance <address-hex>
```

Check balance from a specific node database:

```bash
cargo run -- wallet balance <address-hex> ./data/paqus
```

## Node

Show network info:

```bash
cargo run -- node info
```

Initialize the default node database:

```bash
cargo run -- node init ./data/paqus
```

Run node with the default database:

```bash
cargo run -- node run --wallet wallet.json
```

Run node with mining enabled:

```bash
cargo run -- node run ./data/paqus --wallet wallet.json --mine
```

Run node with custom P2P and RPC addresses:

```bash
cargo run -- node run ./data/paqus \
  --listen 0.0.0.0:30333 \
  --rpc-listen 127.0.0.1:9933 \
  --wallet wallet.json \
  --mine
```

Connect to a peer:

```bash
cargo run -- node run ./data/paqus \
  --peer <peer-ip:30333> \
  --wallet wallet.json
```

Use a peers file:

```bash
cargo run -- node run ./data/paqus \
  --peers-file ./data/paqus/peers.txt \
  --wallet wallet.json
```

Stop the default node:

```bash
touch ./data/paqus/STOP
```

## Transactions

Create a signed transaction and print it as hex:

```bash
cargo run -- wallet send \
  --wallet wallet.json \
  --to <address-tujuan-hex> \
  --amount 10 \
  --nonce 0
```

Create and submit a signed transaction to the default RPC:

```bash
cargo run -- wallet send \
  --wallet wallet.json \
  --to <address-tujuan-hex> \
  --amount 10 \
  --nonce 0 \
  --submit
```

Create and submit to a custom RPC address:

```bash
cargo run -- wallet send \
  --wallet wallet.json \
  --to <address-tujuan-hex> \
  --amount 10 \
  --nonce 0 \
  --submit \
  --rpc 127.0.0.1:9933
```

Set a custom fee:

```bash
cargo run -- wallet send \
  --wallet wallet.json \
  --to <address-tujuan-hex> \
  --amount 10 \
  --fee 2 \
  --nonce 0 \
  --submit
```

## RPC

Health check:

```bash
curl http://127.0.0.1:9933/health
```

Node status:

```bash
curl http://127.0.0.1:9933/status
```

Peer list:

```bash
curl http://127.0.0.1:9933/peers
```

Chain metadata:

```bash
curl http://127.0.0.1:9933/chain
```

Balance:

```bash
curl http://127.0.0.1:9933/balance/<address-hex>
```

Latest blocks:

```bash
curl http://127.0.0.1:9933/blocks/latest
```

Block by height:

```bash
curl http://127.0.0.1:9933/blocks/<height>
```

Block by hash:

```bash
curl http://127.0.0.1:9933/blocks/hash/<block-hash>
```

Transaction by hash:

```bash
curl http://127.0.0.1:9933/tx/<tx-hash>
```

Address page data:

```bash
curl http://127.0.0.1:9933/address/<address-hex>
```

All accounts and balances:

```bash
curl http://127.0.0.1:9933/accounts
```

Mempool:

```bash
curl http://127.0.0.1:9933/mempool
```

Submit signed transaction hex:

```bash
curl -X POST http://127.0.0.1:9933/tx \
  -H 'content-type: application/json' \
  -d '{"tx":"<signed-transaction-hex>"}'
```

Alternative transaction endpoint:

```bash
curl -X POST http://127.0.0.1:9933/transaction \
  -H 'content-type: application/json' \
  -d '{"tx":"<signed-transaction-hex>"}'
```

## Quick Local Flow

```bash
cargo run -- wallet new wallet.json
cargo run -- node init ./data/paqus
cargo run -- node run ./data/paqus --wallet wallet.json --mine
```

From another terminal:

```bash
curl http://127.0.0.1:9933/status
```
