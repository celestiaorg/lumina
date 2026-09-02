//! Transport abstractions and gRPC adapters.

pub(crate) mod grpc_validator_client;
pub(crate) mod host_registry;
// Exposed for benchmarks only; not part of the public API.
#[doc(hidden)]
pub mod proto_conv;
pub(crate) mod validator_client;
