use celestia_types::Blob;
use celestia_types::blob::RawBlob;
use celestia_types::nmt::Namespace;
use jsonrpsee::core::client::{BatchResponse, ClientT, Error};
use jsonrpsee::core::params::BatchRequestBuilder;
use jsonrpsee::core::traits::ToRpcParams;
use lumina_utils::test_utils::async_test;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{BlobClient, TxConfig};

struct TestClient {
    expected_request: Option<(&'static str, Value)>,
}

impl ClientT for TestClient {
    async fn notification<Params>(&self, _: &str, _: Params) -> Result<(), Error>
    where
        Params: ToRpcParams + Send,
    {
        panic!("unexpected notification")
    }

    async fn request<R, Params>(&self, method: &str, params: Params) -> Result<R, Error>
    where
        R: DeserializeOwned,
        Params: ToRpcParams + Send,
    {
        let (expected_method, expected_params) =
            self.expected_request.as_ref().expect("unexpected request");
        assert_eq!(method, *expected_method);
        let params = params.to_rpc_params().unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(params.get()).unwrap(),
            *expected_params
        );
        Err(Error::Custom("request forwarded".into()))
    }

    async fn batch_request<'a, R>(
        &self,
        _: BatchRequestBuilder<'a>,
    ) -> Result<BatchResponse<'a, R>, Error>
    where
        R: DeserializeOwned + std::fmt::Debug + 'a,
    {
        panic!("unexpected batch request")
    }
}

fn fibre_blob() -> Blob {
    let namespace = Namespace::new_v0(b"fibre").unwrap();
    Blob::from_raw(RawBlob {
        namespace_id: namespace.id().to_vec(),
        namespace_version: 0,
        share_version: 2,
        data: vec![0; 36],
        signer: vec![0xAA; 20],
    })
    .unwrap()
}

#[async_test]
async fn reject_fibre_blob_submission() {
    let client = TestClient {
        expected_request: None,
    };
    let fibre = fibre_blob();
    let unsigned = Blob::new(fibre.namespace, vec![1], None).unwrap();
    let signed = Blob::new(fibre.namespace, vec![2], fibre.signer).unwrap();
    let expected = celestia_types::Error::FibreBlobSubmission.to_string();

    for blobs in [
        vec![fibre.clone()],
        vec![fibre.clone(), unsigned.clone(), signed.clone()],
        vec![unsigned, signed, fibre],
    ] {
        let error = client
            .blob_submit(&blobs, TxConfig::default())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Custom(message) if message == expected));
    }
}

#[async_test]
async fn forward_regular_blob_submission() {
    let fibre = fibre_blob();
    let blobs = vec![
        Blob::new(fibre.namespace, vec![1], None).unwrap(),
        Blob::new(fibre.namespace, vec![2], fibre.signer).unwrap(),
    ];
    let config = TxConfig::default().with_gas_price(0.01).with_gas(12345);
    let client = TestClient {
        expected_request: Some(("blob.Submit", json!([blobs, config]))),
    };
    assert_eq!(
        client
            .blob_submit(&blobs, config)
            .await
            .unwrap_err()
            .to_string(),
        "Custom error: request forwarded"
    );
}
