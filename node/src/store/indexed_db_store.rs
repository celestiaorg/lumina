use std::cell::RefCell;
use std::fmt::Display;
use std::pin::pin;

use async_trait::async_trait;
use celestia_types::ExtendedHeader;
use celestia_types::hash::Hash;
use cid::Cid;
use futures::Future;
use js_sys::Uint8Array;
use libp2p::identity::Keypair;
use rexie::{Direction, Index, KeyRange, ObjectStore, Rexie, Transaction, TransactionMode};
use send_wrapper::SendWrapper;
use serde::{Deserialize, Serialize};
use serde_wasm_bindgen::{from_value, to_value};
use smallvec::smallvec;
use tendermint_proto::Protobuf;
use tokio::sync::Notify;
use tracing::{debug, warn};
use wasm_bindgen::{JsCast, JsValue};

use crate::block_ranges::BlockRanges;
use crate::store::utils::VerifiedExtendedHeaders;
use crate::store::{Result, SamplingMetadata, Store, StoreError, StoreInsertionError};
use crate::utils::{NamedLock, NamedLockError};

/// IndexedDB version, needs to be incremented on every schema change
const DB_VERSION: u32 = 5;

// Data stores (SQL table analogue) used in IndexedDB
const HEADER_STORE_NAME: &str = "headers";
const SAMPLING_STORE_NAME: &str = "sampling";
const RANGES_STORE_NAME: &str = "ranges";
const SCHEMA_STORE_NAME: &str = "schema";
const IDENTITY_STORE_NAME: &str = "identity";

// Additional indexes set on HEADER_STORE, for querying by height and hash
const HASH_INDEX_NAME: &str = "hash";
const HEIGHT_INDEX_NAME: &str = "height";

const HEADER_RANGES_KEY: &str = "header_ranges";
const SAMPLED_RANGES_KEY: &str = "sampled_ranges";
const PRUNED_RANGES_KEY: &str = "pruned_ranges";
const VERSION_KEY: &str = "version";
const LIBP2P_IDENTITY_KEY: &str = "libp2p_identity";

#[derive(Debug, Serialize, Deserialize)]
struct ExtendedHeaderEntry {
    // We use those fields as indexes, names need to match ones in `add_index`
    height: u64,
    #[serde(with = "celestia_types::serializers::hash")]
    hash: Hash,
    header: Vec<u8>,
}

/// A [`Store`] implementation based on a `IndexedDB` browser database.
#[derive(Debug)]
pub struct IndexedDbStore {
    // SendWrapper usage is safe in wasm because we're running on a single thread
    head: SendWrapper<RefCell<Option<ExtendedHeader>>>,
    db: SendWrapper<Rexie>,
    header_added_notifier: Notify,
    // locks the db instance using Web Locks API until `IndexedDbStore` instance is dropped
    _db_lock: NamedLock,
}

impl IndexedDbStore {
    /// Create or open a persistent store.
    ///
    /// <div class="warning">
    /// IndexedDB is shared between all tabs from the same origin, but we enforce
    /// that each store is opened exclusively. Subsequent open requests with the same name,
    /// presumably coming from multiple tabs running Lumina, will each create extra stores
    /// with "-extra-tab-N" suffix.
    /// </div>
    pub async fn new(name: &str) -> Result<IndexedDbStore> {
        let (db_lock, real_name) = lock_db_name(name).await?;
        let rexie = Rexie::builder(&real_name)
            .version(DB_VERSION)
            .add_object_store(
                ObjectStore::new(HEADER_STORE_NAME)
                    .key_path("id")
                    .auto_increment(true)
                    // These need to match names in `ExtendedHeaderEntry`
                    .add_index(Index::new(HASH_INDEX_NAME, "hash").unique(true))
                    .add_index(Index::new(HEIGHT_INDEX_NAME, "height").unique(true)),
            )
            .add_object_store(ObjectStore::new(RANGES_STORE_NAME))
            .add_object_store(ObjectStore::new(SAMPLING_STORE_NAME))
            .add_object_store(ObjectStore::new(IDENTITY_STORE_NAME))
            .add_object_store(ObjectStore::new(SCHEMA_STORE_NAME))
            .build()
            .await
            .map_err(|e| StoreError::OpenFailed(e.to_string()))?;

        // NOTE: Rexie does not expose any migration functionality, so we
        // write our version in the store in order to handle it properly.
        match detect_schema_version(&rexie).await? {
            Some(schema_version) => {
                if schema_version > DB_VERSION {
                    let e = format!(
                        "Incompatible database schema; found {schema_version}, expected {DB_VERSION}."
                    );
                    return Err(StoreError::OpenFailed(e));
                }

                migrate_older_to_v4(&rexie).await?;
                migrate_v4_to_v5(&rexie).await?;
            }
            None => {
                // New database
                let tx = rexie.transaction(&[SCHEMA_STORE_NAME], TransactionMode::ReadWrite)?;

                let schema_store = tx.store(SCHEMA_STORE_NAME)?;
                set_schema_version(&schema_store, DB_VERSION).await?;

                tx.commit().await?;
            }
        }

        init_identity(&rexie).await?;

        // Force us to write migrations!
        debug_assert_eq!(
            detect_schema_version(&rexie).await?,
            Some(DB_VERSION),
            "Some migrations are missing"
        );

        let db_head = match get_head_from_database(&rexie).await {
            Ok(v) => Some(v),
            Err(StoreError::NotFound) => None,
            Err(e) => return Err(e),
        };

        Ok(IndexedDbStore {
            head: SendWrapper::new(RefCell::new(db_head)),
            db: SendWrapper::new(rexie),
            header_added_notifier: Notify::new(),
            _db_lock: db_lock,
        })
    }

    async fn write_tx<F, T, Args>(&self, stores: &[&str], f: F, args: Args) -> Result<T>
    where
        for<'a> F: TransactionOperationFn<'a, Args, Output = Result<T>>,
    {
        let tx = self.db.transaction(stores, TransactionMode::ReadWrite)?;
        let res = f(&tx, args).await;

        if res.is_ok() {
            tx.commit().await?;
        } else {
            tx.abort().await?;
        }

        res
    }

    /// Delete the persistent store.
    pub async fn delete_db(self) -> rexie::Result<()> {
        let name = self.db.name();
        self.db.take().close();
        Rexie::delete(&name).await
    }

    fn get_head(&self) -> Result<ExtendedHeader> {
        // this shouldn't panic, we don't borrow across await points and wasm is single threaded
        self.head.borrow().clone().ok_or(StoreError::NotFound)
    }

    fn get_head_height(&self) -> Result<u64> {
        // this shouldn't panic, we don't borrow across await points and wasm is single threaded
        self.head
            .borrow()
            .as_ref()
            .map(|h| h.height())
            .ok_or(StoreError::NotFound)
    }

    async fn get_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        let tx = self
            .db
            .transaction(&[HEADER_STORE_NAME], TransactionMode::ReadOnly)?;

        get_by_height(&tx.store(HEADER_STORE_NAME)?, height).await
    }

    async fn get_by_hash(&self, hash: &Hash) -> Result<ExtendedHeader> {
        let tx = self
            .db
            .transaction(&[HEADER_STORE_NAME], TransactionMode::ReadOnly)?;
        let header_store = tx.store(HEADER_STORE_NAME)?;
        let hash_index = header_store.index(HASH_INDEX_NAME)?;

        let hash_key = to_value(&hash)?;
        let Some(header_entry) = hash_index.get(hash_key).await? else {
            return Err(StoreError::NotFound);
        };

        let serialized_header = from_value::<ExtendedHeaderEntry>(header_entry)?.header;
        ExtendedHeader::decode(serialized_header.as_ref())
            .map_err(|e| StoreError::StoredDataError(e.to_string()))
    }

    async fn get_stored_header_ranges(&self) -> Result<BlockRanges> {
        let tx = self
            .db
            .transaction(&[RANGES_STORE_NAME], TransactionMode::ReadOnly)?;
        let store = tx.store(RANGES_STORE_NAME)?;

        get_ranges(&store, HEADER_RANGES_KEY).await
    }

    async fn insert<R>(&self, headers: R) -> Result<()>
    where
        R: TryInto<VerifiedExtendedHeaders> + Send,
        <R as TryInto<VerifiedExtendedHeaders>>::Error: Display,
    {
        let headers = headers
            .try_into()
            .map_err(|e| StoreInsertionError::HeadersVerificationFailed(e.to_string()))?;

        if headers.as_ref().is_empty() {
            return Ok(());
        }

        let tail = self
            .write_tx(
                &[HEADER_STORE_NAME, RANGES_STORE_NAME],
                insert_tx_op,
                headers,
            )
            .await?;

        if tail.height() > self.head.borrow().as_ref().map(|h| h.height()).unwrap_or(0) {
            self.head.replace(Some(tail));
        }

        self.header_added_notifier.notify_waiters();

        Ok(())
    }

    async fn contains_hash(&self, hash: &Hash) -> Result<bool> {
        let tx = self
            .db
            .transaction(&[HEADER_STORE_NAME], TransactionMode::ReadOnly)?;

        let header_store = tx.store(HEADER_STORE_NAME)?;
        let hash_index = header_store.index(HASH_INDEX_NAME)?;

        let hash_key = KeyRange::only(&to_value(&hash)?).map_err(rexie::Error::IdbError)?;

        let hash_count = hash_index.count(Some(hash_key)).await?;

        Ok(hash_count > 0)
    }

    async fn contains_height(&self, height: u64) -> bool {
        let Ok(stored_ranges) = self.get_stored_header_ranges().await else {
            return false;
        };
        stored_ranges.contains(height)
    }

    async fn update_sampling_metadata(&self, height: u64, cids: Vec<Cid>) -> Result<()> {
        self.write_tx(
            &[SAMPLING_STORE_NAME, RANGES_STORE_NAME],
            update_sampling_metadata_tx_op,
            (height, cids),
        )
        .await
    }

    async fn get_sampling_metadata(&self, height: u64) -> Result<Option<SamplingMetadata>> {
        if !self.contains_height(height).await {
            return Err(StoreError::NotFound);
        }

        let tx = self
            .db
            .transaction(&[SAMPLING_STORE_NAME], TransactionMode::ReadOnly)?;
        let store = tx.store(SAMPLING_STORE_NAME)?;

        let height_key = to_value(&height)?;
        let Some(sampling_entry) = store.get(height_key).await? else {
            return Ok(None);
        };

        Ok(Some(from_value(sampling_entry)?))
    }

    async fn mark_as_sampled(&self, height: u64) -> Result<()> {
        self.write_tx(&[RANGES_STORE_NAME], mark_as_sampled_tx_op, height)
            .await
    }

    async fn get_sampled_ranges(&self) -> Result<BlockRanges> {
        let tx = self
            .db
            .transaction(&[RANGES_STORE_NAME], TransactionMode::ReadOnly)?;
        let store = tx.store(RANGES_STORE_NAME)?;

        get_ranges(&store, SAMPLED_RANGES_KEY).await
    }

    async fn get_pruned_ranges(&self) -> Result<BlockRanges> {
        let tx = self
            .db
            .transaction(&[RANGES_STORE_NAME], TransactionMode::ReadOnly)?;
        let store = tx.store(RANGES_STORE_NAME)?;

        get_ranges(&store, PRUNED_RANGES_KEY).await
    }

    async fn remove_height(&self, height: u64) -> Result<()> {
        self.write_tx(
            &[HEADER_STORE_NAME, SAMPLING_STORE_NAME, RANGES_STORE_NAME],
            remove_height_tx_op,
            height,
        )
        .await
    }

    async fn get_identity(&self) -> Result<Keypair> {
        let tx = self
            .db
            .transaction(&[IDENTITY_STORE_NAME], TransactionMode::ReadOnly)?;
        let store = tx.store(IDENTITY_STORE_NAME)?;

        let key = to_value(LIBP2P_IDENTITY_KEY)?;
        let value = store
            .get(key.clone())
            .await?
            .expect("identity should be initialised here");
        let keypair = decode_keypair(value)?;
        Ok(keypair)
    }
}

trait TransactionOperationFn<'a, Arg>:
    FnOnce(&'a Transaction, Arg) -> <Self as TransactionOperationFn<'a, Arg>>::Fut
{
    type Fut: Future<Output = <Self as TransactionOperationFn<'a, Arg>>::Output>;
    type Output;
}

impl<'a, Arg, F, Fut> TransactionOperationFn<'a, Arg> for F
where
    F: FnOnce(&'a Transaction, Arg) -> Fut,
    Fut: Future,
{
    type Fut = Fut;
    type Output = Fut::Output;
}

#[async_trait]
impl Store for IndexedDbStore {
    async fn get_head(&self) -> Result<ExtendedHeader> {
        self.get_head()
    }

    async fn get_by_hash(&self, hash: &Hash) -> Result<ExtendedHeader> {
        let fut = SendWrapper::new(self.get_by_hash(hash));
        fut.await
    }

    async fn get_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        let fut = SendWrapper::new(self.get_by_height(height));
        fut.await
    }

    async fn wait_new_head(&self) -> u64 {
        let head = self.get_head_height().unwrap_or(0);
        let mut notifier = pin!(self.header_added_notifier.notified());

        loop {
            let new_head = self.get_head_height().unwrap_or(0);

            if head != new_head {
                return new_head;
            }

            // Await for a notification
            notifier.as_mut().await;

            // Reset notifier
            notifier.set(self.header_added_notifier.notified());
        }
    }

    async fn wait_height(&self, height: u64) -> Result<()> {
        let mut notifier = pin!(self.header_added_notifier.notified());

        loop {
            let fut = SendWrapper::new(self.contains_height(height));
            if fut.await {
                return Ok(());
            }

            // Await for a notification
            notifier.as_mut().await;

            // Reset notifier
            notifier.set(self.header_added_notifier.notified());
        }
    }

    async fn head_height(&self) -> Result<u64> {
        self.get_head_height()
    }

    async fn has(&self, hash: &Hash) -> bool {
        let fut = SendWrapper::new(self.contains_hash(hash));
        fut.await.unwrap_or(false)
    }

    async fn has_at(&self, height: u64) -> bool {
        let fut = SendWrapper::new(self.contains_height(height));
        fut.await
    }

    async fn update_sampling_metadata(&self, height: u64, cids: Vec<Cid>) -> Result<()> {
        let fut = SendWrapper::new(self.update_sampling_metadata(height, cids));
        fut.await
    }

    async fn get_sampling_metadata(&self, height: u64) -> Result<Option<SamplingMetadata>> {
        let fut = SendWrapper::new(self.get_sampling_metadata(height));
        fut.await
    }

    async fn insert<R>(&self, header: R) -> Result<()>
    where
        R: TryInto<VerifiedExtendedHeaders> + Send,
        <R as TryInto<VerifiedExtendedHeaders>>::Error: Display,
    {
        let fut = SendWrapper::new(self.insert(header));
        fut.await
    }

    async fn mark_as_sampled(&self, height: u64) -> Result<()> {
        let fut = SendWrapper::new(self.mark_as_sampled(height));
        fut.await
    }

    async fn get_stored_header_ranges(&self) -> Result<BlockRanges> {
        let fut = SendWrapper::new(self.get_stored_header_ranges());
        fut.await
    }

    async fn get_sampled_ranges(&self) -> Result<BlockRanges> {
        let fut = SendWrapper::new(self.get_sampled_ranges());
        fut.await
    }

    async fn get_pruned_ranges(&self) -> Result<BlockRanges> {
        let fut = SendWrapper::new(self.get_pruned_ranges());
        fut.await
    }

    async fn remove_height(&self, height: u64) -> Result<()> {
        let fut = SendWrapper::new(self.remove_height(height));
        fut.await
    }

    async fn get_identity(&self) -> Result<Keypair> {
        let fut = SendWrapper::new(self.get_identity());
        fut.await
    }

    async fn close(self) -> Result<()> {
        self.db.take().close();
        Ok(())
    }
}

impl From<rexie::Error> for StoreError {
    fn from(error: rexie::Error) -> StoreError {
        StoreError::FatalDatabaseError(error.to_string())
    }
}

impl From<serde_wasm_bindgen::Error> for StoreError {
    fn from(error: serde_wasm_bindgen::Error) -> StoreError {
        StoreError::StoredDataError(format!("Error de/serializing: {error}"))
    }
}

async fn get_ranges(store: &rexie::Store, name: &str) -> Result<BlockRanges> {
    let key = JsValue::from_str(name);
    let Some(raw_ranges) = store.get(key).await? else {
        // Ranges not set yet
        return Ok(BlockRanges::default());
    };

    Ok(from_value(raw_ranges)?)
}

async fn set_ranges(store: &rexie::Store, name: &str, ranges: &BlockRanges) -> Result<()> {
    let key = JsValue::from_str(name);
    let val = to_value(ranges)?;

    store.put(&val, Some(&key)).await?;

    Ok(())
}

async fn get_head_from_database(db: &Rexie) -> Result<ExtendedHeader> {
    let tx = db.transaction(
        &[HEADER_STORE_NAME, RANGES_STORE_NAME],
        TransactionMode::ReadOnly,
    )?;
    let header_store = tx.store(HEADER_STORE_NAME)?;
    let ranges_store = tx.store(RANGES_STORE_NAME)?;

    let ranges = get_ranges(&ranges_store, HEADER_RANGES_KEY).await?;
    let head_height = ranges.head().ok_or(StoreError::NotFound)?;

    get_by_height(&header_store, head_height).await
}

async fn get_by_height(header_store: &rexie::Store, height: u64) -> Result<ExtendedHeader> {
    let height_index = header_store.index(HEIGHT_INDEX_NAME)?;

    let height_key = to_value(&height)?;
    let Some(header_entry) = height_index.get(height_key).await? else {
        return Err(StoreError::NotFound);
    };

    let serialized_header = from_value::<ExtendedHeaderEntry>(header_entry)?.header;
    ExtendedHeader::decode(serialized_header.as_ref())
        .map_err(|e| StoreError::StoredDataError(e.to_string()))
}

async fn verify_against_neighbours(
    header_store: &rexie::Store,
    lowest_header: Option<&ExtendedHeader>,
    highest_header: Option<&ExtendedHeader>,
) -> Result<()> {
    if let Some(lowest_header) = lowest_header {
        let prev = get_by_height(header_store, lowest_header.height() - 1)
            .await
            .map_err(|e| {
                if let StoreError::NotFound = e {
                    StoreError::StoredDataError(
                        "inconsistency between headers and ranges table".into(),
                    )
                } else {
                    e
                }
            })?;

        prev.verify(lowest_header)
            .map_err(|e| StoreInsertionError::NeighborsVerificationFailed(e.to_string()))?;
    }

    if let Some(highest_header) = highest_header {
        let next = get_by_height(header_store, highest_header.height() + 1)
            .await
            .map_err(|e| {
                if let StoreError::NotFound = e {
                    StoreError::StoredDataError(
                        "inconsistency between headers and ranges table".into(),
                    )
                } else {
                    e
                }
            })?;

        highest_header
            .verify(&next)
            .map_err(|e| StoreInsertionError::NeighborsVerificationFailed(e.to_string()))?;
    }

    Ok(())
}

/// Get schema version from the db, or perform a heuristic to try to determine
/// version used (for versions <4).
async fn detect_schema_version(db: &Rexie) -> Result<Option<u32>> {
    let tx = db.transaction(
        &[HEADER_STORE_NAME, RANGES_STORE_NAME, SCHEMA_STORE_NAME],
        TransactionMode::ReadOnly,
    )?;
    let schema_store = tx.store(SCHEMA_STORE_NAME)?;
    let ranges_store = tx.store(RANGES_STORE_NAME)?;
    let header_store = tx.store(HEADER_STORE_NAME)?;

    // Schema version exists from v4 and above.
    if let Ok(version) = get_schema_version(&schema_store).await {
        return Ok(Some(version));
    }

    // If schema version does not exist in db but ranges store
    // has values then assume we are in version 3.
    if !store_is_empty(&ranges_store).await? {
        return Ok(Some(3));
    }

    // If ranges store does not have any values but header for height 1
    // exists, then we assume we are in version 2.
    let height_key = to_value(&1)?;
    let height_index = header_store.index(HEIGHT_INDEX_NAME)?;
    if height_index.get(height_key).await?.is_some() {
        return Ok(Some(2));
    }

    // Otherwise we assume we have a new db.
    Ok(None)
}

async fn lock_db_name(name: &str) -> Result<(NamedLock, String)> {
    let mut try_name = name.to_string();
    let mut i = 1;
    loop {
        match NamedLock::try_lock(&try_name).await {
            Ok(lock) => return Ok((lock, try_name)),
            Err(NamedLockError::WouldBlock) => (),
            Err(e) => return Err(e.into()),
        }
        try_name = format!("{name}-extra-tab-{i}");
        i += 1;
    }
}

async fn init_identity(db: &Rexie) -> Result<()> {
    let tx = db.transaction(&[IDENTITY_STORE_NAME], TransactionMode::ReadWrite)?;
    let identity_store = tx.store(IDENTITY_STORE_NAME)?;

    let key = to_value(LIBP2P_IDENTITY_KEY)?;
    if identity_store.get(key.clone()).await?.is_some() {
        return Ok(());
    }

    let keypair = Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    debug!("Initialised new identity: {peer_id}");

    let value = encode_keypair(&keypair);
    identity_store.put(&value, Some(&key)).await?;

    Ok(())
}

fn encode_keypair(keypair: &Keypair) -> JsValue {
    let bytes = keypair.to_protobuf_encoding().expect("valid key encoding");
    let array = Uint8Array::from(bytes.as_ref());
    array.into()
}

fn decode_keypair(value: JsValue) -> Result<Keypair> {
    let array = value
        .dyn_into::<Uint8Array>()
        .map_err(|_| StoreError::StoredDataError("invalid key data type".to_string()))?;
    Keypair::from_protobuf_encoding(&array.to_vec())
        .map_err(|e| StoreError::StoredDataError(e.to_string()))
}

async fn store_is_empty(store: &rexie::Store) -> Result<bool> {
    let vals = store.get_all_keys(None, Some(1)).await?;
    Ok(vals.is_empty())
}

async fn get_schema_version(store: &rexie::Store) -> Result<u32> {
    let key = to_value(VERSION_KEY)?;
    let Some(val) = store.get(key).await? else {
        return Err(StoreError::NotFound);
    };
    Ok(from_value(val)?)
}

async fn set_schema_version(store: &rexie::Store, version: u32) -> Result<()> {
    let key = to_value(VERSION_KEY)?;
    let val = to_value(&version)?;
    store.put(&val, Some(&key)).await?;
    Ok(())
}

async fn insert_tx_op(
    tx: &Transaction,
    headers: VerifiedExtendedHeaders,
) -> Result<ExtendedHeader> {
    let head = headers.as_ref().first().expect("headers to not be empty");
    let tail = headers
        .as_ref()
        .last()
        .cloned()
        .expect("headers to not be empty");

    let header_store = tx.store(HEADER_STORE_NAME)?;
    let ranges_store = tx.store(RANGES_STORE_NAME)?;

    let mut header_ranges = get_ranges(&ranges_store, HEADER_RANGES_KEY).await?;
    let mut sampled_ranges = get_ranges(&ranges_store, SAMPLED_RANGES_KEY).await?;
    let mut pruned_ranges = get_ranges(&ranges_store, PRUNED_RANGES_KEY).await?;

    let headers_range = head.height()..=tail.height();
    let (prev_exists, next_exists) = header_ranges
        .check_insertion_constraints(&headers_range)
        .map_err(StoreInsertionError::ConstraintsNotMet)?;

    // header range is already internally verified against itself in `P2p::get_unverified_header_ranges`
    verify_against_neighbours(
        &header_store,
        prev_exists.then_some(head),
        next_exists.then_some(&tail),
    )
    .await?;

    for header in headers {
        let hash = header.hash();
        let hash_index = header_store.index(HASH_INDEX_NAME)?;
        let jsvalue_hash_key = KeyRange::only(&to_value(&hash)?).map_err(rexie::Error::IdbError)?;

        if hash_index.count(Some(jsvalue_hash_key)).await.unwrap_or(0) != 0 {
            // TODO: Replace this with `StoredDataError` when we implement
            // type-safe validation on insertion.
            return Err(StoreInsertionError::HashExists(hash).into());
        }

        let height = header.height();
        let header_entry = ExtendedHeaderEntry {
            height,
            hash,
            header: header.encode_vec(),
        };

        let jsvalue_header = to_value(&header_entry)?;

        header_store.add(&jsvalue_header, None).await?;
    }

    header_ranges
        .insert_relaxed(&headers_range)
        .expect("invalid range");
    sampled_ranges
        .remove_relaxed(&headers_range)
        .expect("invalid range");
    pruned_ranges
        .remove_relaxed(&headers_range)
        .expect("invalid range");

    set_ranges(&ranges_store, HEADER_RANGES_KEY, &header_ranges).await?;
    set_ranges(&ranges_store, SAMPLED_RANGES_KEY, &sampled_ranges).await?;
    set_ranges(&ranges_store, PRUNED_RANGES_KEY, &pruned_ranges).await?;

    Ok(tail)
}

async fn update_sampling_metadata_tx_op(
    tx: &Transaction,
    (height, cids): (u64, Vec<Cid>),
) -> Result<()> {
    let sampling_store = tx.store(SAMPLING_STORE_NAME)?;
    let ranges_store = tx.store(RANGES_STORE_NAME)?;
    let header_ranges = get_ranges(&ranges_store, HEADER_RANGES_KEY).await?;

    if !header_ranges.contains(height) {
        return Err(StoreError::NotFound);
    }

    let height_key = to_value(&height)?;
    let new_entry = match sampling_store.get(height_key.clone()).await? {
        Some(previous_entry) => {
            let mut value: SamplingMetadata = from_value(previous_entry)?;

            for cid in cids {
                if !value.cids.contains(&cid) {
                    value.cids.push(cid);
                }
            }

            value
        }
        None => SamplingMetadata { cids },
    };

    let metadata_jsvalue = to_value(&new_entry)?;
    sampling_store
        .put(&metadata_jsvalue, Some(&height_key))
        .await?;

    Ok(())
}

async fn mark_as_sampled_tx_op(tx: &Transaction, height: u64) -> Result<()> {
    let ranges_store = tx.store(RANGES_STORE_NAME)?;
    let header_ranges = get_ranges(&ranges_store, HEADER_RANGES_KEY).await?;
    let mut sampled_ranges = get_ranges(&ranges_store, SAMPLED_RANGES_KEY).await?;

    if !header_ranges.contains(height) {
        return Err(StoreError::NotFound);
    }

    sampled_ranges
        .insert_relaxed(height..=height)
        .expect("invalid height");

    set_ranges(&ranges_store, SAMPLED_RANGES_KEY, &sampled_ranges).await?;

    Ok(())
}

async fn remove_height_tx_op(tx: &Transaction, height: u64) -> Result<()> {
    let header_store = tx.store(HEADER_STORE_NAME)?;
    let height_index = header_store.index(HEIGHT_INDEX_NAME)?;
    let ranges_store = tx.store(RANGES_STORE_NAME)?;
    let sampling_store = tx.store(SAMPLING_STORE_NAME)?;

    let mut header_ranges = get_ranges(&ranges_store, HEADER_RANGES_KEY).await?;
    let mut sampled_ranges = get_ranges(&ranges_store, SAMPLED_RANGES_KEY).await?;
    let mut pruned_ranges = get_ranges(&ranges_store, PRUNED_RANGES_KEY).await?;

    if !header_ranges.contains(height) {
        return Err(StoreError::NotFound);
    }

    let jsvalue_height = to_value(&height).expect("to create jsvalue");

    let Some(header) = height_index.get(jsvalue_height.clone()).await? else {
        return Err(StoreError::StoredDataError(
            "inconsistency between headers and ranges table".into(),
        ));
    };

    let id = js_sys::Reflect::get(&header, &to_value("id")?)
        .map_err(|_| StoreError::StoredDataError("could not get header's DB id".into()))?;

    header_store.delete(id).await?;

    sampling_store.delete(jsvalue_height).await?;

    header_ranges
        .remove_relaxed(height..=height)
        .expect("invalid range");
    sampled_ranges
        .remove_relaxed(height..=height)
        .expect("invalid range");
    pruned_ranges
        .insert_relaxed(height..=height)
        .expect("invalid range");

    set_ranges(&ranges_store, HEADER_RANGES_KEY, &header_ranges).await?;
    set_ranges(&ranges_store, SAMPLED_RANGES_KEY, &sampled_ranges).await?;
    set_ranges(&ranges_store, PRUNED_RANGES_KEY, &pruned_ranges).await?;

    Ok(())
}

async fn migrate_older_to_v4(db: &Rexie) -> Result<()> {
    let version = detect_schema_version(db)
        .await?
        .expect("migrations never run on new db");

    if version >= 4 {
        // Nothing to migrate.
        return Ok(());
    }

    warn!("Migrating DB schema from v{version} to v4");

    let tx = db.transaction(
        &[HEADER_STORE_NAME, RANGES_STORE_NAME, SCHEMA_STORE_NAME],
        TransactionMode::ReadWrite,
    )?;
    let header_store = tx.store(HEADER_STORE_NAME)?;
    let ranges_store = tx.store(RANGES_STORE_NAME)?;
    let schema_store = tx.store(SCHEMA_STORE_NAME)?;

    let ranges = if version <= 2 {
        match v2::get_head_header(&header_store).await {
            // On v2 there were no gaps between headers.
            Ok(head) => BlockRanges::from_vec(smallvec![1..=head.height()])
                .map_err(|e| StoreError::StoredDataError(e.to_string()))?,
            Err(StoreError::NotFound) => BlockRanges::new(),
            Err(e) => return Err(e),
        }
    } else {
        // On v3 ranges existed but in different format.
        v3::get_header_ranges(&ranges_store).await?
    };

    ranges_store.clear().await?;
    set_ranges(&ranges_store, HEADER_RANGES_KEY, &ranges).await?;

    // Migrated to version 4
    set_schema_version(&schema_store, 4).await?;

    tx.commit().await?;

    Ok(())
}

async fn migrate_v4_to_v5(db: &Rexie) -> Result<()> {
    let Some(version) = detect_schema_version(db).await? else {
        // New database.
        return Ok(());
    };

    if version >= 5 {
        // Nothing to migrate.
        return Ok(());
    }

    debug_assert_eq!(version, 4);
    warn!("Migrating DB schema from v4 to v5");

    let tx = db.transaction(
        &[SCHEMA_STORE_NAME, RANGES_STORE_NAME],
        TransactionMode::ReadWrite,
    )?;
    let schema_store = tx.store(SCHEMA_STORE_NAME)?;
    let ranges_store = tx.store(RANGES_STORE_NAME)?;

    // There are two changes in v5:
    //
    // * Removal of `SamplingStatus` in `SamplingMetadata`
    // * Rename of sampled ranges database key
    //
    // For the first one we don't need to take any actions because it will
    // be ignored by the deserializer.
    let sampled_ranges = get_ranges(&ranges_store, v4::SAMPLED_RANGES_KEY).await?;
    set_ranges(&ranges_store, SAMPLED_RANGES_KEY, &sampled_ranges).await?;
    ranges_store
        .delete(JsValue::from_str(v4::SAMPLED_RANGES_KEY))
        .await?;

    // Migrated to v5
    set_schema_version(&schema_store, 5).await?;

    tx.commit().await?;

    Ok(())
}

mod v2 {
    use super::*;

    pub(super) async fn get_head_header(store: &rexie::Store) -> Result<ExtendedHeader> {
        let store_head = store
            .scan(None, Some(1), None, Some(Direction::Prev))
            .await?
            .first()
            .ok_or(StoreError::NotFound)?
            .1
            .to_owned();

        let serialized_header = from_value::<ExtendedHeaderEntry>(store_head)?.header;

        ExtendedHeader::decode(serialized_header.as_ref())
            .map_err(|e| StoreError::StoredDataError(e.to_string()))
    }
}

mod v3 {
    use super::*;

    pub(super) async fn get_header_ranges(store: &rexie::Store) -> Result<BlockRanges> {
        let mut ranges = BlockRanges::default();

        for raw_range in store.get_all(None, None).await? {
            let (start, end) = from_value::<(u64, u64)>(raw_range)?;
            ranges
                .insert_relaxed(start..=end)
                .map_err(|e| StoreError::StoredDataError(e.to_string()))?;
        }

        Ok(ranges)
    }
}

mod v4 {
    pub(super) const SAMPLED_RANGES_KEY: &str = "accepted_sampling_ranges";
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::test_utils::ExtendedHeaderGeneratorExt;
    use celestia_types::test_utils::ExtendedHeaderGenerator;
    use wasm_bindgen_test::wasm_bindgen_test;

    use crate::test_utils::new_indexed_db_store_name;

    #[wasm_bindgen_test]
    async fn test_large_db() {
        let store_name = new_indexed_db_store_name().await;
        let s = IndexedDbStore::new(&store_name)
            .await
            .expect("creating test store failed");

        let mut generator = ExtendedHeaderGenerator::new();
        let expected_height = 1_000;

        s.insert(generator.next_many_verified(expected_height))
            .await
            .expect("inserting test data failed");

        for h in 1..=expected_height {
            s.mark_as_sampled(h).await.expect("marking sampled failed");
            s.update_sampling_metadata(h, vec![])
                .await
                .expect("updating sampling metadata failed");
        }

        assert_eq!(s.get_head().unwrap().height(), expected_height);

        drop(s);
        // re-open the store, to force re-calculation of the cached heights
        let s = IndexedDbStore::new(&store_name)
            .await
            .expect("re-opening large test store failed");

        assert_eq!(s.get_head().unwrap().height(), expected_height);
    }

    #[wasm_bindgen_test]
    async fn test_header_persistence() {
        let store_name = new_indexed_db_store_name().await;

        let (original_store, mut generator) = gen_filled_store(0, &store_name).await;
        let original_headers = generator.next_many_verified(20);

        original_store
            .insert(original_headers.clone())
            .await
            .expect("inserting test data failed");
        drop(original_store);

        let reopened_store = IndexedDbStore::new(&store_name)
            .await
            .expect("failed to reopen store");

        assert_eq!(
            original_headers.as_ref().last().unwrap().height(),
            reopened_store.head_height().await.unwrap()
        );
        for original_header in original_headers.as_ref() {
            let stored_header = reopened_store
                .get_by_height(original_header.height())
                .await
                .unwrap();
            assert_eq!(original_header, &stored_header);
        }

        let new_headers = generator.next_many_verified(10);
        reopened_store
            .insert(new_headers.clone())
            .await
            .expect("failed to insert data");
        drop(reopened_store);

        let final_headers = original_headers
            .into_iter()
            .chain(new_headers)
            .collect::<Vec<_>>();

        let reopened_store = IndexedDbStore::new(&store_name)
            .await
            .expect("failed to reopen store");

        assert_eq!(
            final_headers.last().unwrap().height(),
            reopened_store.head_height().await.unwrap()
        );
        for original_header in &final_headers {
            let stored_header = reopened_store
                .get_by_height(original_header.height())
                .await
                .unwrap();
            assert_eq!(original_header, &stored_header);
        }
    }

    #[wasm_bindgen_test]
    async fn test_delete_db() {
        let store_name = new_indexed_db_store_name().await;

        let (original_store, _) = gen_filled_store(3, &store_name).await;
        assert_eq!(original_store.get_head_height().unwrap(), 3);

        original_store.delete_db().await.unwrap();

        let same_name_store = IndexedDbStore::new(&store_name)
            .await
            .expect("creating test store failed");

        assert!(matches!(
            same_name_store.get_head_height(),
            Err(StoreError::NotFound)
        ));
    }

    #[wasm_bindgen_test]
    async fn test_identity_persistence() {
        let store_name = new_indexed_db_store_name().await;

        let (original_store, _gen) = gen_filled_store(0, &store_name).await;
        let original_identity = original_store.get_identity().await.unwrap().public();
        drop(original_store);

        let reopened_store = IndexedDbStore::new(&store_name)
            .await
            .expect("failed to reopen store");
        let reopened_identity = reopened_store.get_identity().await.unwrap().public();

        assert_eq!(original_identity, reopened_identity);
    }

    mod migration_from_v1 {
        use super::*;

        const PREVIOUS_DB_VERSION: u32 = 1;

        // Set up the store manually with v1 schema (original, ExtendedHeader only), and fill
        // it with `hs` headers. It is a caller responsibility to make sure provided headers
        // are correct and in order.
        async fn init_store(name: &str, hs: Vec<ExtendedHeader>) {
            let rexie = Rexie::builder(name)
                .version(PREVIOUS_DB_VERSION)
                .add_object_store(
                    ObjectStore::new("headers")
                        .key_path("id")
                        .auto_increment(true)
                        .add_index(Index::new("hash", "hash").unique(true))
                        .add_index(Index::new("height", "height").unique(true)),
                )
                .build()
                .await
                .unwrap();

            let tx = rexie
                .transaction(&["headers"], TransactionMode::ReadWrite)
                .unwrap();
            let header_store = tx.store("headers").unwrap();

            for header in hs {
                let header_entry = ExtendedHeaderEntry {
                    height: header.height(),
                    hash: header.hash(),
                    header: header.encode_vec(),
                };

                header_store
                    .add(&to_value(&header_entry).unwrap(), None)
                    .await
                    .unwrap();
            }

            rexie.close();
        }

        #[wasm_bindgen_test]
        async fn migration_test() {
            let store_name = new_indexed_db_store_name().await;
            let mut generator = ExtendedHeaderGenerator::new();
            let headers = generator.next_many(20);

            init_store(&store_name, headers.clone()).await;

            let store = IndexedDbStore::new(&store_name)
                .await
                .expect("opening migrated store failed");

            for header in headers {
                let height = header.height();

                let header_by_hash = store.get_by_hash(&header.hash()).await.unwrap();
                assert_eq!(header, header_by_hash);

                let header_by_height = store.get_by_height(height).await.unwrap();
                assert_eq!(header, header_by_height);

                // migrated headers should be marked as not sampled
                let sampling_data = store.get_sampling_metadata(height).await.unwrap();
                assert!(sampling_data.is_none());
            }
        }
    }

    mod migration_from_v4 {
        use super::*;

        async fn init_store(name: &str) {
            let rexie = Rexie::builder(name)
                .version(4)
                .add_object_store(ObjectStore::new("ranges"))
                .add_object_store(ObjectStore::new("schema"))
                .build()
                .await
                .unwrap();

            let tx = rexie
                .transaction(&["schema", "ranges"], TransactionMode::ReadWrite)
                .unwrap();

            let schema_store = tx.store("schema").unwrap();
            schema_store
                .put(&to_value(&4).unwrap(), Some(&JsValue::from_str("version")))
                .await
                .unwrap();

            let ranges_store = tx.store("ranges").unwrap();
            ranges_store
                .put(
                    &to_value(&BlockRanges::try_from([123..=124]).unwrap()).unwrap(),
                    Some(&JsValue::from_str(v4::SAMPLED_RANGES_KEY)),
                )
                .await
                .unwrap();

            tx.commit().await.unwrap();
            rexie.close();
        }

        #[wasm_bindgen_test]
        async fn migration_test() {
            let store_name = new_indexed_db_store_name().await;

            // Prepare the store
            init_store(&store_name).await;

            // Migrate and check
            let store = IndexedDbStore::new(&store_name).await.unwrap();
            let ranges = store.get_sampled_ranges().await.unwrap();
            assert_eq!(ranges, BlockRanges::try_from([123..=124]).unwrap());
            store.close().await.unwrap();

            // Check that old ranges were deleted
            let rexie = Rexie::builder(&store_name)
                .version(5)
                .add_object_store(ObjectStore::new("ranges"))
                .build()
                .await
                .unwrap();
            let tx = rexie
                .transaction(&[RANGES_STORE_NAME], TransactionMode::ReadOnly)
                .unwrap();
            let store = tx.store(RANGES_STORE_NAME).unwrap();
            assert!(
                store
                    .get(JsValue::from_str(v4::SAMPLED_RANGES_KEY))
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }

    // open IndexedDB with unique per-test name to avoid interference and make cleanup easier
    pub async fn gen_filled_store(
        amount: u64,
        name: &str,
    ) -> (IndexedDbStore, ExtendedHeaderGenerator) {
        Rexie::delete(name).await.unwrap();
        let s = IndexedDbStore::new(name)
            .await
            .expect("creating test store failed");
        let mut generator = ExtendedHeaderGenerator::new();

        s.insert(generator.next_many_verified(amount))
            .await
            .expect("inserting test data failed");

        (s, generator)
    }
}
