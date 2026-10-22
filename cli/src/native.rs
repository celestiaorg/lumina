use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use blockstore::EitherBlockstore;
use celestia_rpc::Client;
use celestia_rpc::prelude::*;
use clap::{Parser, value_parser};
use directories::ProjectDirs;
use libp2p::multiaddr::{Multiaddr, Protocol};
use lumina_node::blockstore::{InMemoryBlockstore, RedbBlockstore};
use lumina_node::events::NodeEvent;
use lumina_node::network::Network;
use lumina_node::node::{DEFAULT_PRUNING_WINDOW_IN_MEMORY, Node};
use lumina_node::store::{EitherStore, InMemoryStore, RedbStore, Store as _};
use tokio::task::spawn_blocking;
use tracing::{info, warn};

const CELESTIA_LOCAL_BRIDGE_RPC_ADDR: &str = "ws://localhost:36658";

type Blockstore = EitherBlockstore<InMemoryBlockstore, RedbBlockstore>;
type Store = EitherStore<InMemoryStore, RedbStore>;

#[derive(Debug, Parser)]
pub(crate) struct Params {
    /// Network to connect.
    #[arg(short, long)]
    #[clap(value_parser = value_parser!(Network))]
    pub(crate) network: Network,

    /// Listening addresses. Can be used multiple times.
    #[arg(short, long = "listen")]
    pub(crate) listen_addrs: Vec<Multiaddr>,

    /// Bootnode multiaddr, including peer id. Can be used multiple times.
    #[arg(short, long = "bootnode")]
    pub(crate) bootnodes: Vec<Multiaddr>,

    /// Persistent header store path.
    #[arg(short, long)]
    pub(crate) store: Option<PathBuf>,

    /// Use in-memory store.
    #[arg(long)]
    pub(crate) in_memory_store: bool,

    /// Pruning window defines maximum age of a block for it to be retained in store.
    #[arg(long)]
    #[clap(value_parser = humantime::parse_duration)]
    pub(crate) pruning_window: Option<Duration>,
}

pub(crate) async fn run(args: Params) -> Result<()> {
    let (blockstore, store) = if args.in_memory_store {
        open_in_memory_stores()
    } else {
        open_db_stores(args.store, args.network.id()).await?
    };

    let mut node_builder = Node::builder()
        .store(store)
        .blockstore(blockstore)
        .network(args.network.clone());

    if let Some(pruning_window) = args.pruning_window {
        node_builder = node_builder.pruning_window(pruning_window);
    } else if args.in_memory_store {
        // In-memory stores are memory hungry, so we prune blocks as soon as possible.
        node_builder = node_builder.pruning_window(DEFAULT_PRUNING_WINDOW_IN_MEMORY);
    }

    if args.bootnodes.is_empty() {
        if args.network.is_custom() {
            let bootnodes = fetch_bridge_multiaddrs(CELESTIA_LOCAL_BRIDGE_RPC_ADDR).await?;
            node_builder = node_builder.bootnodes(bootnodes);
        }
    } else {
        node_builder = node_builder.bootnodes(args.bootnodes);
    }

    if !args.listen_addrs.is_empty() {
        node_builder = node_builder.listen(args.listen_addrs);
    }

    let (_node, mut events) = node_builder
        .start_subscribed()
        .await
        .context("Failed to start node")?;

    while let Ok(ev) = events.recv().await {
        match ev.event {
            // Skip noisy events
            NodeEvent::ShareSamplingResult { .. } => continue,
            event if event.is_error() => warn!("{event}"),
            event => info!("{event}"),
        }
    }

    Ok(())
}

fn open_in_memory_stores() -> (Blockstore, Store) {
    info!("Initializing in-memory store");
    let store = InMemoryStore::new();
    let blockstore = InMemoryBlockstore::new();
    (EitherBlockstore::Left(blockstore), EitherStore::Left(store))
}

async fn open_db_stores(path: Option<PathBuf>, network_id: &str) -> Result<(Blockstore, Store)> {
    info!("Initializing store");
    let db = open_db(path, network_id).await?;
    let store = RedbStore::new(db.clone()).await?;
    let blockstore = RedbBlockstore::new(db);

    let stored_ranges = store.get_stored_header_ranges().await?;
    if stored_ranges.is_empty() {
        info!("Initialised new store",);
    } else {
        info!("Initialised store, present headers: {stored_ranges}");
    }

    Ok((
        EitherBlockstore::Right(blockstore),
        EitherStore::Right(store),
    ))
}

async fn open_db(path: Option<PathBuf>, network_id: &str) -> Result<Arc<redb::Database>> {
    let network_id = network_id.to_owned();

    spawn_blocking(move || {
        use std::fs;

        if let Some(path) = path {
            let db = redb::Database::create(path)?;
            return Ok(Arc::new(db));
        }

        let cache_dir = ProjectDirs::from("co", "eiger", "lumina")
            .context("failed to construct project path")?
            .cache_dir()
            .join(&network_id)
            .to_owned();

        let old_cache_dir = ProjectDirs::from("co", "eiger", "celestia")
            .context("failed to construct project path")?
            .cache_dir()
            .join(&network_id)
            .to_owned();

        if is_sled_db(&old_cache_dir) {
            warn!("Removing deprecated store {}", old_cache_dir.display());
            fs::remove_dir_all(&old_cache_dir)?;
        }

        if is_sled_db(&cache_dir) {
            warn!("Removing deprecated store {}", cache_dir.display());
            fs::remove_dir_all(&cache_dir)?;
        }

        // Directories need to pre-exist
        fs::create_dir_all(&cache_dir)?;

        let path = cache_dir.join("db");
        let db = redb::Database::create(path)?;

        Ok(Arc::new(db))
    })
    .await?
}

fn is_sled_db(path: impl AsRef<Path>) -> bool {
    let path = path.as_ref();
    path.join("blobs").is_dir() && path.join("conf").is_file() && path.join("db").is_file()
}

/// Get the address of the local bridge node
async fn fetch_bridge_multiaddrs(ws_url: &str) -> Result<Vec<Multiaddr>> {
    let auth_token = env::var("CELESTIA_NODE_AUTH_TOKEN_ADMIN")
        .context("Missing CELESTIA_NODE_AUTH_TOKEN_ADMIN environment variable")?;
    let client = Client::new(ws_url, Some(&auth_token), None, None).await?;
    let bridge_info = client.p2p_info().await?;

    info!("bridge id: {:?}", bridge_info.id);
    info!("bridge listens on: {:?}", bridge_info.addrs);

    let addrs = bridge_info
        .addrs
        .into_iter()
        .filter(|ma| ma.protocol_stack().any(|protocol| protocol == "tcp"))
        .map(|mut ma| {
            if !ma.protocol_stack().any(|protocol| protocol == "p2p") {
                ma.push(Protocol::P2p(bridge_info.id.into()))
            }
            ma
        })
        .collect::<Vec<_>>();

    if addrs.is_empty() {
        bail!("Bridge doesn't listen on tcp");
    }

    Ok(addrs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_pruning_window() {
        let params = Params::try_parse_from([
            "lumina",
            "--network",
            "mainnet",
            "--pruning-window",
            "1h 30m",
        ])
        .unwrap();

        assert_eq!(params.pruning_window, Some(Duration::from_secs(90 * 60)));
    }

    #[test]
    fn rejects_invalid_pruning_window() {
        let result = Params::try_parse_from([
            "lumina",
            "--network",
            "mainnet",
            "--pruning-window",
            "invalid",
        ]);

        assert!(result.is_err());
    }
}
