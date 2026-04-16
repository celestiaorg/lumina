use std::borrow::Cow;
use std::net::{IpAddr, Ipv4Addr};

use js_sys::Promise;
use libp2p::multiaddr::Protocol;
use libp2p::{Multiaddr, PeerId};
use serde::Deserialize;
use tracing::warn;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{Request, RequestInit, RequestMode, Response};

// cloudflare dns
const DEFAULT_DNS_ADDR: IpAddr = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));

#[derive(thiserror::Error, Debug)]
pub(crate) enum Error {
    #[error("Failed fetching: {0}")]
    CouldNotFetch(String),
    #[error("Could not parse response: {0}")]
    CouldNotParseResponse(String),
}

pub(crate) async fn resolve_bootnode_addresses(addrs: Vec<Multiaddr>) -> Vec<Multiaddr> {
    let mut bootnodes = Vec::with_capacity(addrs.len());

    for addr in addrs {
        match resolve_dnsaddr_multiaddress(&addr, DEFAULT_DNS_ADDR).await {
            Ok(resolved_addrs) => bootnodes.extend(resolved_addrs),
            Err(e) => warn!("Failed to resolve {addr}: {e}"),
        }
    }

    bootnodes
}

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = fetch)]
    fn fetch_with_request(input: &Request) -> Promise;
}

async fn fetch(url: &str, opts: &RequestInit, headers: &[(&str, &str)]) -> Result<Response, Error> {
    let request = Request::new_with_str_and_init(url, opts)
        .map_err(|e| Error::CouldNotFetch(format!("failed to prepare request: {e:?}")))?;

    for (name, value) in headers {
        request.headers().set(name, value).map_err(|_| {
            Error::CouldNotFetch(format!("failed setting header: '{name}: {value}'"))
        })?;
    }

    let fetch_promise = fetch_with_request(&request);

    JsFuture::from(fetch_promise)
        .await
        .map_err(|e| Error::CouldNotFetch(format!("failed fetching {url}: {e:?}")))?
        .dyn_into()
        .map_err(|_| Error::CouldNotFetch("`response` is not `Response` type".to_string()))
}

fn get_peer_id(ma: &Multiaddr) -> Option<PeerId> {
    ma.iter().find_map(|protocol| {
        if let Protocol::P2p(peer_id) = protocol {
            Some(peer_id)
        } else {
            None
        }
    })
}

fn get_dnsaddr(ma: &Multiaddr) -> Option<Cow<'_, str>> {
    ma.iter().find_map(|protocol| {
        if let Protocol::Dnsaddr(addr) = protocol {
            Some(addr)
        } else {
            None
        }
    })
}

/// If provided multiaddress uses dnsaddr protocol, resolve it using dns-over-https.
/// Otherwise returns the provided address.
pub async fn resolve_dnsaddr_multiaddress(
    ma: &Multiaddr,
    dns_ip: IpAddr,
) -> Result<Vec<Multiaddr>, Error> {
    const TXT_TYPE: u16 = 16;

    #[derive(Debug, Deserialize)]
    struct DohEntry {
        r#type: u16,
        data: String,
    }

    #[derive(Debug, Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct DohResponse {
        answer: Vec<DohEntry>,
    }

    let Some(dnsaddr) = get_dnsaddr(ma) else {
        // not a dnsaddr multiaddr
        return Ok(vec![ma.to_owned()]);
    };
    let Some(peer_id) = get_peer_id(ma) else {
        return Err(Error::CouldNotFetch(
            "failed preparing request: PeerId missing".to_string(),
        ));
    };

    let opts = RequestInit::new();
    opts.set_method("GET");
    opts.set_mode(RequestMode::Cors);

    let url = format!("https://{dns_ip}/dns-query?type={TXT_TYPE}&name=_dnsaddr.{dnsaddr}");
    let response = fetch(&url, &opts, &[("Accept", "application/dns-json")]).await?;

    let json_promise = response
        .json()
        .map_err(|e| Error::CouldNotParseResponse(format!("Response::json() failed: {e:?}")))?;
    let json = JsFuture::from(json_promise).await.map_err(|e| {
        Error::CouldNotParseResponse(format!("Failed parsing response as json: {e:?}"))
    })?;

    let doh_response: DohResponse = serde_wasm_bindgen::from_value(json).map_err(|e| {
        Error::CouldNotParseResponse(format!("Failed deserializing DoH response: {e}"))
    })?;

    let mut resolved_addrs = Vec::with_capacity(3);
    for entry in doh_response.answer {
        if entry.r#type == TXT_TYPE {
            // we receive data as json encoded strings in this format:
            // "data": "\"dnsaddr=/dns/da-bridge-1.celestia-arabica-11.com/tcp/2121/p2p/12D3KooWGqwzdEqM54Dce6LXzfFr97Bnhvm6rN7KM7MFwdomfm4S\""
            let Ok(data) = serde_json::from_str::<String>(&entry.data) else {
                continue;
            };
            let Some((_, ma)) = data.split_once('=') else {
                continue;
            };
            let Ok(ma) = ma.parse() else {
                continue;
            };
            // only take results with the same peer id
            if Some(peer_id) == get_peer_id(&ma) {
                // TODO: handle recursive dnsaddr queries
                resolved_addrs.push(ma);
            }
        }
    }

    Ok(resolved_addrs)
}
