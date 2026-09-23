#![allow(clippy::all)]
#![allow(missing_docs)]
#![allow(rustdoc::invalid_rust_codeblocks)]
#![cfg(not(doctest))]
#![doc = include_str!("../README.md")]

pub mod serializers;

#[cfg(feature = "tonic")]
mod tonic_codec;

include!(concat!(env!("OUT_DIR"), "/mod.rs"));

#[cfg(feature = "uniffi")]
uniffi::setup_scaffolding!();
