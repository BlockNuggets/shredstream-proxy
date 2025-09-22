# Deshred Example

This example demonstrates how to consume transactions from
the shredstream proxy in real-time.

## Prerequisites

- Running shredstream proxy
```bash
RUST_LOG=info jito-shredstream-proxy -- shredstream \
            --block-engine-url https://mainnet.block-engine.jito.wtf \
            --auth-keypair token.json \
            --desired-regions ${regions} \
            --dest-ip-ports 127.0.0.1:8001 \
            --grpc-service-port 9999
```

## Usage

```bash
cargo run --example deshred
```
