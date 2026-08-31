//! Transport abstractions and gRPC adapters.

pub(crate) mod grpc_validator_client;
pub(crate) mod host_registry;
pub(crate) mod proto_conv;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod tls;
pub(crate) mod validator_client;
