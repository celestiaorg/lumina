use celestia_types::Blob;
use jsonrpsee::core::client::{BatchResponse, ClientT, Error};
use jsonrpsee::core::params::BatchRequestBuilder;
use jsonrpsee::core::traits::ToRpcParams;
use lumina_utils::test_utils::async_test;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::{BlobClient, StateClient, TxConfig};

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
    let fixture: Value =
        serde_json::from_str(include_str!("../../types/test_data/fibre_blob_v2.json")).unwrap();
    serde_json::from_value(fixture["blob"].clone()).unwrap()
}

#[async_test]
async fn reject_fibre_blob_submission() {
    let client = TestClient {
        expected_request: None,
    };
    let fibre = fibre_blob();
    let unsigned = Blob::new(fibre.namespace, vec![1], None).unwrap();
    let signed = Blob::new(fibre.namespace, vec![2], fibre.signer).unwrap();
    let expected = "Share version 2 is reserved for Fibre system blobs and cannot be submitted via PayForBlobs.";

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
        let error = client
            .state_submit_pay_for_blob(
                blobs.into_iter().map(Into::into).collect(),
                TxConfig::default(),
            )
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

    let raw: Vec<celestia_types::blob::RawBlob> = blobs.into_iter().map(Into::into).collect();
    let config = TxConfig::default().with_gas_price(0.01).with_gas(12345);
    let client = TestClient {
        expected_request: Some(("state.SubmitPayForBlob", json!([raw, config]))),
    };
    assert_eq!(
        client
            .state_submit_pay_for_blob(raw, config)
            .await
            .unwrap_err()
            .to_string(),
        "Custom error: request forwarded"
    );
}

#[async_test]
async fn forward_state_request() {
    let address = fibre_blob().signer.unwrap().into();
    let client = TestClient {
        expected_request: Some(("state.BalanceForAddress", json!([address]))),
    };
    assert_eq!(
        client
            .state_balance_for_address(address)
            .await
            .unwrap_err()
            .to_string(),
        "Custom error: request forwarded"
    );
}

#[async_test]
async fn preserve_raw_share_version_on_submission() {
    let mut raw: celestia_types::blob::RawBlob = fibre_blob().into();
    raw.share_version = 258;
    let config = TxConfig::default();
    let client = TestClient {
        expected_request: Some(("state.SubmitPayForBlob", json!([[raw], config]))),
    };
    assert_eq!(
        client
            .state_submit_pay_for_blob(vec![raw], config)
            .await
            .unwrap_err()
            .to_string(),
        "Custom error: request forwarded"
    );
}
