//! In-memory mock validator network for fibre load testing.
//!
//! Serves the control plane (BlockAPI ValidatorSet + valaddr host registry)
//! on one port and one `celestia.fibre.v1.Fibre` service per simulated
//! validator on its own port, so a real `FibreClient::from_endpoint` pointed
//! at the control-plane port works end-to-end.
#![cfg(not(target_arch = "wasm32"))]
// tonic::Status is the natural error type for gRPC handlers and their helpers.
#![allow(clippy::result_large_err)]

pub mod app_node;
pub mod chain;
pub mod cli;
pub mod control_plane;
pub mod fibre_service;
pub mod promise;
pub mod store;
pub mod tls;
pub mod validator;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use anyhow::Context;
use celestia_proto::celestia::core::v1::gas_estimation::gas_estimator_server::GasEstimatorServer;
use celestia_proto::celestia::core::v1::tx::tx_server::TxServer as CelestiaTxServer;
use celestia_proto::celestia::fibre::v1::fibre_server::FibreServer;
use celestia_proto::celestia::valaddr::v1::query_server::QueryServer;
use celestia_proto::cosmos::auth::v1beta1::query_server::QueryServer as AuthQueryServer;
use celestia_proto::cosmos::base::tendermint::v1beta1::service_server::ServiceServer as TendermintServiceServer;
use celestia_proto::cosmos::tx::v1beta1::service_server::ServiceServer as CosmosTxServiceServer;
use celestia_proto::tendermint_celestia_mods::rpc::grpc::block_api_server::BlockApiServer;
use tonic::transport::server::TcpIncoming;

use crate::app_node::{
    MockAuthQuery, MockCelestiaTxStatus, MockCosmosTxService, MockGasEstimator,
    MockTendermintService,
};
use crate::chain::MockChain;
use crate::control_plane::{MockBlockApi, MockValaddrQuery};
use crate::fibre_service::MockFibreService;
use crate::validator::MockValidator;

/// Matches the fibre client's own encode/decode limits
/// (`grpc/grpc-macros/src/lib.rs`); tonic's server default of 4 MiB is too
/// small for v0 shards.
const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

/// Configuration for [`spawn_mock_network`].
pub struct MockNetworkConfig {
    /// Number of simulated validators, one Fibre gRPC port each.
    pub num_validators: usize,
    /// IP address all servers listen on.
    pub listen_ip: IpAddr,
    /// IP address advertised to clients. Defaults to `listen_ip` and must not
    /// be unspecified.
    pub advertise_ip: Option<IpAddr>,
    /// Control plane listens on `base_port`, validator `i` on
    /// `base_port + 1 + i`. 0 = ephemeral ports for everything.
    pub base_port: u16,
    /// Voting power of every validator.
    pub voting_power: i64,
    /// Height reported by ValidatorSet; ends up in payment promises.
    pub height: i64,
    /// Chain id: signed into the TLS identity bindings and served in the
    /// latest-block header. Must equal the clients' chain id.
    pub chain_id: String,
}

impl Default for MockNetworkConfig {
    fn default() -> Self {
        Self {
            num_validators: 3,
            listen_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            advertise_ip: None,
            base_port: 19000,
            voting_power: 100,
            height: 1,
            chain_id: "mock-1".to_string(),
        }
    }
}

/// A running mock network; all servers shut down on [`MockNetworkHandle::shutdown`].
pub struct MockNetworkHandle {
    /// Address of the control-plane server (BlockAPI + valaddr Query).
    pub control_addr: SocketAddr,
    /// The simulated validators, in port order.
    pub validators: Arc<Vec<MockValidator>>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl MockNetworkHandle {
    /// Signal all servers to shut down gracefully.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

/// Bind and spawn the control-plane server plus one Fibre server per validator.
pub async fn spawn_mock_network(cfg: MockNetworkConfig) -> anyhow::Result<MockNetworkHandle> {
    anyhow::ensure!(
        cfg.num_validators > 0,
        "num_validators must be greater than zero"
    );
    anyhow::ensure!(
        cfg.voting_power > 0,
        "voting_power must be greater than zero"
    );
    anyhow::ensure!(cfg.height > 0, "height must be greater than zero");

    let advertise_ip = cfg.advertise_ip.unwrap_or(cfg.listen_ip);
    anyhow::ensure!(
        !advertise_ip.is_unspecified(),
        "advertise_ip is required when listen_ip is unspecified"
    );

    // The fibre client dials validators with TLS 1.3 only; ring is the provider
    // both sides of the dep graph agree on.
    let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Bind all data-plane listeners first so the registry advertises real ports.
    let mut incomings = Vec::with_capacity(cfg.num_validators);
    let mut validators = Vec::with_capacity(cfg.num_validators);
    for i in 0..cfg.num_validators {
        let port = if cfg.base_port == 0 {
            0
        } else {
            u16::try_from(cfg.base_port as usize + 1 + i).context("validator port exceeds 65535")?
        };
        let incoming = TcpIncoming::bind(SocketAddr::new(cfg.listen_ip, port))
            .with_context(|| format!("failed to bind validator {i} on port {port}"))?;
        let listen_addr = incoming.local_addr()?;
        let advertised_addr = SocketAddr::new(advertise_ip, listen_addr.port());
        validators.push(MockValidator::new(
            i as u64,
            format!("http://{advertised_addr}"),
            cfg.voting_power,
        ));
        incomings.push(incoming);
    }

    for (incoming, validator) in incomings.into_iter().zip(&validators) {
        let svc = FibreServer::new(MockFibreService::new(validator.signing_key.clone()))
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_MESSAGE_SIZE);
        let identity = tls::endorsed_identity(&validator.signing_key, &cfg.chain_id)
            .with_context(|| format!("failed to build TLS identity for {}", validator.host))?;
        let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);
        let mut rx = shutdown_rx.clone();
        let host = validator.host.clone();
        tokio::spawn(async move {
            let server = tonic::transport::Server::builder()
                .tls_config(tls_config)
                .expect("static TLS config is valid")
                .add_service(svc)
                .serve_with_incoming_shutdown(incoming, async move {
                    let _ = rx.changed().await;
                });
            if let Err(e) = server.await {
                tracing::error!("validator server {host} exited: {e}");
            }
        });
    }

    let control_incoming = TcpIncoming::bind(SocketAddr::new(cfg.listen_ip, cfg.base_port))
        .with_context(|| format!("failed to bind control plane on port {}", cfg.base_port))?;
    let control_addr = SocketAddr::new(advertise_ip, control_incoming.local_addr()?.port());

    let validators = Arc::new(validators);
    let chain = Arc::new(MockChain::new(
        cfg.chain_id.clone(),
        cfg.height,
        validators[0].address,
    ));
    let block_api = BlockApiServer::new(MockBlockApi::new(validators.clone(), cfg.height));
    let valaddr = QueryServer::new(MockValaddrQuery::new(&validators));
    let tendermint_svc = TendermintServiceServer::new(MockTendermintService(chain.clone()));
    let auth = AuthQueryServer::new(MockAuthQuery);
    let gas = GasEstimatorServer::new(MockGasEstimator);
    let cosmos_tx = CosmosTxServiceServer::new(MockCosmosTxService(chain.clone()));
    let celestia_tx = CelestiaTxServer::new(MockCelestiaTxStatus(chain));
    let mut rx = shutdown_rx;
    tokio::spawn(async move {
        let server = tonic::transport::Server::builder()
            .add_service(block_api)
            .add_service(valaddr)
            .add_service(tendermint_svc)
            .add_service(auth)
            .add_service(gas)
            .add_service(cosmos_tx)
            .add_service(celestia_tx)
            .serve_with_incoming_shutdown(control_incoming, async move {
                let _ = rx.changed().await;
            });
        if let Err(e) = server.await {
            tracing::error!("control-plane server exited: {e}");
        }
    });

    Ok(MockNetworkHandle {
        control_addr,
        validators,
        shutdown_tx,
    })
}
