use libp2p::identity::Keypair;
use libp2p::swarm::{NetworkBehaviour, Swarm};
use std::time::Duration;

use crate::p2p::{P2pError, Result};

pub(crate) use self::imp::new_swarm;

#[cfg(not(target_arch = "wasm32"))]
mod imp {
    use std::env;
    use std::io::Cursor;
    use std::path::Path;

    use futures::future::Either;
    use libp2p::core::muxing::StreamMuxerBox;
    use libp2p::core::upgrade::Version;
    use libp2p::{PeerId, Transport, dns, noise, quic, swarm, tcp, websocket, yamux};
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use tokio::fs;

    use super::*;

    pub(crate) async fn new_swarm<B>(keypair: Keypair, behaviour: B) -> Result<Swarm<B>>
    where
        B: NetworkBehaviour,
    {
        let tls_key = match env::var("LUMINA_TLS_KEY_FILE") {
            Ok(path) => Some(read_tls_key(path).await?),
            Err(_) => None,
        };

        let tls_certs = match env::var("LUMINA_TLS_CERT_FILE") {
            Ok(path) => Some(read_tls_certs(path).await?),
            Err(_) => None,
        };

        // We do not use system's DNS because libp2p caches system DNS
        // servers when `Swarm` get constructed, and doesn't update them
        // later. This can be a problem, if device roams between networks
        // (and old DNS addresses may not be reachable from the new network).
        //
        // Similarly, if node is started when there's no Internet connection,
        // it won't use the DNS servers offered when Internet connectivity
        // is restored. Instead, we per-define globally-accessible public DNS servers.
        let dns_config = dns::ResolverConfig::udp_and_tcp(&hickory_resolver::config::CLOUDFLARE);

        let noise_config =
            noise::Config::new(&keypair).map_err(|e| P2pError::NoiseInit(e.to_string()))?;

        let wss_transport = {
            let config = if let (Some(key), Some(certs)) = (tls_key, tls_certs) {
                let key = websocket::tls::PrivateKey::new(key.secret_der().to_vec());
                let certs = certs
                    .iter()
                    .map(|cert| websocket::tls::Certificate::new(cert.to_vec()));

                websocket::tls::Config::new(key, certs)
                    .map_err(|e| P2pError::TlsInit(format!("server config: {e}")))?
            } else {
                websocket::tls::Config::client()
            };

            let mut wss_transport = websocket::Config::new(dns::tokio::Transport::custom(
                tcp::tokio::Transport::new(tcp::Config::default()),
                dns_config.clone(),
                dns::ResolverOpts::default(),
            ));

            wss_transport.set_tls_config(config);

            wss_transport
                .upgrade(Version::V1Lazy)
                .authenticate(noise_config.clone())
                .multiplex(yamux::Config::default())
        };

        let tcp_transport = tcp::tokio::Transport::new(tcp::Config::default())
            .upgrade(Version::V1Lazy)
            .authenticate(noise_config)
            .multiplex(yamux::Config::default());

        let quic_transport = quic::tokio::Transport::new(quic::Config::new(&keypair));

        // WSS must be before TCP transport and must not be wrapped in DNS transport.
        let transport = wss_transport
            .or_transport(dns::tokio::Transport::custom(
                tcp_transport
                    .or_transport(quic_transport)
                    .map(|either, _| match either {
                        Either::Left((peer_id, conn)) => (peer_id, StreamMuxerBox::new(conn)),
                        Either::Right((peer_id, conn)) => (peer_id, StreamMuxerBox::new(conn)),
                    }),
                dns_config,
                dns::ResolverOpts::default(),
            ))
            .map(|either, _| match either {
                Either::Left((peer_id, conn)) => (peer_id, StreamMuxerBox::new(conn)),
                Either::Right((peer_id, conn)) => (peer_id, StreamMuxerBox::new(conn)),
            })
            .boxed();

        let local_peer_id = PeerId::from_public_key(&keypair.public());

        Ok(Swarm::new(
            transport,
            behaviour,
            local_peer_id,
            swarm::Config::with_tokio_executor()
                // This closes any connection that is idle for 15 seconds and not protected
                // by `p2p::Worker::protect` (check p2p.rs for more info).
                .with_idle_connection_timeout(Duration::from_secs(15)),
        ))
    }

    impl From<noise::Error> for P2pError {
        fn from(e: noise::Error) -> Self {
            P2pError::NoiseInit(e.to_string())
        }
    }

    async fn read_tls_key(path: impl AsRef<Path>) -> Result<PrivateKeyDer<'static>, P2pError> {
        let path = path.as_ref();

        // TODO: read key in a pre-allocated memory and zero it after use
        let data = fs::read(&path)
            .await
            .map_err(|e| P2pError::TlsInit(format!("{}: {e}", path.display())))?;

        let mut data = Cursor::new(data);

        rustls_pemfile::private_key(&mut data)
            .map_err(|e| P2pError::TlsInit(format!("{}: {e}", path.display())))?
            .ok_or_else(|| P2pError::TlsInit(format!("{}: Key not found in file", path.display())))
    }

    async fn read_tls_certs(
        path: impl AsRef<Path>,
    ) -> Result<Vec<CertificateDer<'static>>, P2pError> {
        let path = path.as_ref();

        let data = fs::read(path)
            .await
            .map_err(|e| P2pError::TlsInit(format!("{}: {e}", path.display())))?;

        let mut data = Cursor::new(data);
        let certs = rustls_pemfile::certs(&mut data)
            .collect::<Result<Vec<_>, std::io::Error>>()
            .map_err(|e| P2pError::TlsInit(format!("{}: {e}", path.display())))?;

        if certs.is_empty() {
            let e = format!("{}: Certificate not found in file", path.display());
            Err(P2pError::TlsInit(e))
        } else {
            Ok(certs)
        }
    }
}

#[cfg(target_arch = "wasm32")]
mod imp {
    use super::*;
    use libp2p::core::upgrade::Version;
    use libp2p::{SwarmBuilder, Transport, noise, websocket_websys, webtransport_websys, yamux};

    pub(crate) async fn new_swarm<B>(keypair: Keypair, behaviour: B) -> Result<Swarm<B>>
    where
        B: NetworkBehaviour,
    {
        let noise_config =
            noise::Config::new(&keypair).map_err(|e| P2pError::NoiseInit(e.to_string()))?;

        Ok(SwarmBuilder::with_existing_identity(keypair)
            .with_wasm_bindgen()
            .with_other_transport(move |_| {
                Ok(websocket_websys::Transport::default()
                    .upgrade(Version::V1Lazy)
                    .authenticate(noise_config)
                    .multiplex(yamux::Config::default()))
            })
            .expect("websocket_websys::Transport is infallible")
            .with_other_transport(|local_keypair| {
                let config = webtransport_websys::Config::new(local_keypair);
                webtransport_websys::Transport::new(config)
            })
            .expect("webtransport_websys::Transport is infallible")
            .with_behaviour(|_| behaviour)
            .expect("Moving behaviour doesn't fail")
            .with_swarm_config(|config| {
                // This closes any connection that is idle for 15 seconds and not protected
                // by `p2p::Worker::protect` (check p2p.rs for more info).
                config.with_idle_connection_timeout(Duration::from_secs(15))
            })
            .build())
    }
}
