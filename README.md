<div align="center">

# Lumina

<a href="https://github.com/celestiaorg/lumina/actions/workflows/ci.yml?query=branch%3Amain"><img src="https://github.com/celestiaorg/lumina/actions/workflows/ci.yml/badge.svg?branch=main" alt="Build Status" /></a>
<a href="https://lumina.rs"><img src="https://img.shields.io/website?url=https%3A%2F%2Flumina.rs&label=lumina.rs" alt="lumina.rs"></a>
<a href="https://crates.io/crates/lumina-cli"><img src="https://img.shields.io/crates/v/lumina-cli?label=lumina-cli" alt="crates.io lumina-cli"></a>
<a href="https://crates.io/crates/lumina-node"><img src="https://img.shields.io/crates/v/lumina-node?label=lumina-node" alt="crates.io lumina-node"></a>
<a href="https://crates.io/crates/lumina-node-uniffi"><img src="https://img.shields.io/crates/v/lumina-node-uniffi?label=lumina-node-uniffi" alt="crates.io lumina-node-uniffi"></a>
<a href="https://crates.io/crates/lumina-node-wasm"><img src="https://img.shields.io/crates/v/lumina-node-wasm?label=lumina-node-wasm" alt="crates.io lumina-node-wasm"></a>
<a href="https://crates.io/crates/celestia-types"><img src="https://img.shields.io/crates/v/celestia-types?label=celestia-types" alt="crates.io celestia-types"></a>
<a href="https://crates.io/crates/celestia-proto"><img src="https://img.shields.io/crates/v/celestia-proto?label=celestia-proto" alt="crates.io celestia-proto"></a>
<a href="https://crates.io/crates/celestia-rpc"><img src="https://img.shields.io/crates/v/celestia-rpc?label=celestia-rpc" alt="crates.io celestia-rpc"></a>
<a href="https://crates.io/crates/celestia-grpc"><img src="https://img.shields.io/crates/v/celestia-grpc?label=celestia-grpc" alt="crates.io celestia-grpc"></a>

</div>


Rust implementation of Celestia's [data availability node](https://github.com/celestiaorg/celestia-node) able to run natively and in browser-based environments.

Run Lumina now at [lumina.rs](https://lumina.rs/) and directly verify Celestia.

Supported features:
- Backward and forward synchronization of block headers within sampling window
- Header exchange (`header-ex`) client and server
- Listening for, verifying and redistributing extended headers on gossip protocol (`header-sub`)
- Listening for, verifying and redistributing fraud proofs on gossip protocol (`fraud-sub`)
- Backward and forward Data Availability Sampling
- Native and browser persistent storage
- Streaming events happening on the node
- Native, Wasm and UniFFI libraries, embed the node anywhere
- Integration tests with Go implementation

## Installing the node

### Installing with cargo

Install the node. Note that currently to serve lumina to run it from the browser, you need to compile `lumina-cli` manually.
```bash
cargo install lumina-cli --locked
```
Run the node
```bash
lumina node --network mocha
```

### Building from source

Install common dependencies

```bash
# install dependencies
sudo apt-get install -y build-essential curl git

# install rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# open a new terminal or run
source "$HOME/.cargo/env"

# clone the repository
git clone https://github.com/celestiaorg/lumina
cd lumina

# install lumina
cargo install --path cli
```

### Building wasm-node

To build `lumina-cli` with support for serving wasm-node to browsers, currently
you need to compile wasm node manually. Follow these additional steps:

```bash
# install npm and wasm-pack
sudo apt-get install -y npm
cargo install wasm-pack

# compile lumina to wasm
wasm-pack build node-wasm

# build the local webpage
cd cli/js
npm i && npm run build
cd -

# install lumina-cli
cargo install --path cli --features browser-node
```

> [!WARNING]
> Since `getrandom v0.3` it is required to configure the randomness source via `RUSTFLAGS`.
> If you depend on any of the crates and build for `wasm32-unknown-unknown`, it may be required to set
> `'--cfg getrandom_backend="wasm_js"'` either through an environment variable or through `.cargo/config.toml`.
> [See `getrandom` docs for more info](https://docs.rs/getrandom/0.3.3/getrandom/#webassembly-support).

## Running the node

### Running the node natively

```bash
# run lumina node
lumina node --network mocha

# check out help for more configuration options
lumina node --help
```

### Serving node-wasm

```bash
# serve lumina node on default localhost:9876
lumina browser

# check out help from more configuration options
lumina browser --help
```

#### WebTransport and Secure Contexts

For security reasons, browsers only allow WebTransport to be used in [Secure Context](https://developer.mozilla.org/en-US/docs/Web/Security/Secure_Contexts). When running Lumina in a browser make sure to access it either locally or over HTTPS.

## Running Go Celestia node for integration

Follow [this guide](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-with-a-personal-access-token-classic)
to authorize yourself in GitHub's container registry.

Starting a Celestia network with single validator and some DA nodes
```bash
docker compose -f ci/docker-compose.yml up --build --force-recreate -d
# and to stop it
docker compose -f ci/docker-compose.yml down
```
> **Note:**
> You can run more DA nodes by uncommenting/copying the node service definition in `ci/docker-compose.yml`.

To get a JWT token for a topped up account (coins will be transferred in block 2):
```bash
export CELESTIA_NODE_AUTH_TOKEN=$(docker compose -f ci/docker-compose.yml exec node-1 celestia bridge auth admin --p2p.network private)
```

Accessing json RPC api with Go `celestia` cli:
```bash
docker compose -f ci/docker-compose.yml exec node-1 \
    celestia blob submit 0x0c204d39600fddd3 '"Hello world"' --token "$CELESTIA_NODE_AUTH_TOKEN"
```

Extracting blocks for test cases:
```bash
docker compose -f ci/docker-compose.yml exec node-1 \
    celestia header get-by-height 27 --token "$CELESTIA_NODE_AUTH_TOKEN" | jq .result
```

## Running integration tests with Celestia node

Make sure you have the Celestia network running inside docker compose from the section above.

Generate authentication tokens
```bash
./tools/gen_auth_tokens.sh
```

Run tests
```bash
cargo test
```

Run tests for javascript bindings. Those test are written in typescript, import wasm packages,
and are executed in all major browsers (chrome, firefox, safari) using vitest and playwright.

```bash
npm ci
npm test
```

## Frontend

Check out the frontend at [celestiaorg/lumina-front](https://github.com/celestiaorg/lumina-front)

## Contact

Contact us at security@celestia.org
