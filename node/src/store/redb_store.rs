use std::fmt::Display;
use std::ops::RangeInclusive;
use std::path::Path;
use std::pin::pin;
use std::sync::Arc;

use async_trait::async_trait;
use celestia_types::ExtendedHeader;
use celestia_types::hash::Hash;
use cid::Cid;
use libp2p::identity::Keypair;
use redb::{
    CommitError, Database, ReadTransaction, ReadableTable, ReadableTableMetadata, StorageError,
    Table, TableDefinition, TableError, TransactionError, WriteTransaction,
};
use tendermint_proto::Protobuf;
use tokio::sync::Notify;
use tokio::task::spawn_blocking;
use tracing::{debug, trace, warn};

use crate::block_ranges::BlockRanges;
use crate::store::utils::VerifiedExtendedHeaders;
use crate::store::{Result, SamplingMetadata, Store, StoreError, StoreInsertionError};
use crate::utils::Counter;

use super::utils::{deserialize_extended_header, deserialize_sampling_metadata};

const SCHEMA_VERSION: u64 = 3;

const HEIGHTS_TABLE: TableDefinition<'static, &[u8], u64> = TableDefinition::new("STORE.HEIGHTS");
const HEADERS_TABLE: TableDefinition<'static, u64, &[u8]> = TableDefinition::new("STORE.HEADERS");
const SAMPLING_METADATA_TABLE: TableDefinition<'static, u64, &[u8]> =
    TableDefinition::new("STORE.SAMPLING_METADATA");
const SCHEMA_VERSION_TABLE: TableDefinition<'static, (), u64> =
    TableDefinition::new("STORE.SCHEMA_VERSION");
const RANGES_TABLE: TableDefinition<'static, &str, Vec<(u64, u64)>> =
    TableDefinition::new("STORE.RANGES");
const LIBP2P_IDENTITY_TABLE: TableDefinition<'static, (), &[u8]> =
    TableDefinition::new("LIBP2P.IDENTITY");

const SAMPLED_RANGES_KEY: &str = "KEY.SAMPLED_RANGES";
const HEADER_RANGES_KEY: &str = "KEY.HEADER_RANGES";
const PRUNED_RANGES_KEY: &str = "KEY.PRUNED_RANGES";

/// A [`Store`] implementation based on a [`redb`] database.
#[derive(Debug)]
pub struct RedbStore {
    inner: Arc<Inner>,
    task_counter: Counter,
}

#[derive(Debug)]
struct Inner {
    /// Reference to the entire redb database
    db: Arc<Database>,
    /// Notify when a new header is added
    header_added_notifier: Notify,
}

impl RedbStore {
    /// Open a persistent [`redb`] store.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();

        let db = spawn_blocking(|| Database::create(path))
            .await?
            .map_err(|e| StoreError::OpenFailed(e.to_string()))?;

        RedbStore::new(Arc::new(db)).await
    }

    /// Open an in-memory [`redb`] store.
    pub async fn in_memory() -> Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(|e| StoreError::OpenFailed(e.to_string()))?;

        RedbStore::new(Arc::new(db)).await
    }

    /// Create new `RedbStore` with an already opened [`redb::Database`].
    pub async fn new(db: Arc<Database>) -> Result<Self> {
        let store = RedbStore {
            inner: Arc::new(Inner {
                db,
                header_added_notifier: Notify::new(),
            }),
            task_counter: Counter::new(),
        };

        store
            .write_tx(|tx| {
                let mut schema_version_table = tx.open_table(SCHEMA_VERSION_TABLE)?;
                let schema_version = schema_version_table.get(())?.map(|guard| guard.value());

                match schema_version {
                    Some(schema_version) => {
                        if schema_version > SCHEMA_VERSION {
                            let e = format!(
                                "Incompatible database schema; found {schema_version}, expected {SCHEMA_VERSION}."
                            );
                            return Err(StoreError::OpenFailed(e));
                        }

                        // Do migrations
                        migrate_v1_to_v2(tx, &mut schema_version_table)?;
                        migrate_v2_to_v3(tx, &mut schema_version_table)?;
                    }
                    None => {
                        // New database
                        schema_version_table.insert((), SCHEMA_VERSION)?;
                    }
                }

                // Force us to write migrations!
                debug_assert_eq!(
                    schema_version_table.get(())?.map(|guard| guard.value()),
                    Some(SCHEMA_VERSION),
                    "Some migrations are missing"
                );

                // create tables, so that reads later don't complain
                let _heights_table = tx.open_table(HEIGHTS_TABLE)?;
                let _headers_table = tx.open_table(HEADERS_TABLE)?;
                let _ranges_table = tx.open_table(RANGES_TABLE)?;
                let _sampling_table = tx.open_table(SAMPLING_METADATA_TABLE)?;
                let mut identity_table = tx.open_table(LIBP2P_IDENTITY_TABLE)?;

                if identity_table.is_empty()? {
                    let keypair = Keypair::generate_ed25519();

                    let peer_id = keypair.public().to_peer_id();
                    let keypair_bytes = keypair.to_protobuf_encoding()?;
                    debug!("Initialised new identity: {peer_id}");
                    identity_table.insert((), &*keypair_bytes)?;
                }

                Ok(())
            })
            .await
            .map_err(|e| match e {
                e @ StoreError::OpenFailed(_) => e,
                e => StoreError::OpenFailed(e.to_string()),
            })?;

        Ok(store)
    }

    /// Returns the raw [`redb::Database`].
    ///
    /// This is useful if you want to pass the database handle to any other
    /// stores (e.g. [`blockstore`]).
    pub fn raw_db(&self) -> Arc<Database> {
        self.inner.db.clone()
    }

    /// Execute a read transaction.
    async fn read_tx<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut ReadTransaction) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        let guard = self.task_counter.guard();

        spawn_blocking(move || {
            let _guard = guard;

            {
                let mut tx = inner.db.begin_read()?;
                f(&mut tx)
            }
        })
        .await?
    }

    /// Execute a write transaction.
    ///
    /// If closure returns an error the transaction is aborted, otherwise committed.
    async fn write_tx<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut WriteTransaction) -> Result<T> + Send + 'static,
        T: Send + 'static,
    {
        let inner = self.inner.clone();
        let guard = self.task_counter.guard();

        spawn_blocking(move || {
            let _guard = guard;

            {
                let mut tx = inner.db.begin_write()?;
                let res = f(&mut tx);

                if res.is_ok() {
                    tx.commit()?;
                } else {
                    tx.abort()?;
                }

                res
            }
        })
        .await?
    }

    async fn head_height(&self) -> Result<u64> {
        self.read_tx(|tx| {
            let table = tx.open_table(RANGES_TABLE)?;
            let header_ranges = get_ranges(&table, HEADER_RANGES_KEY)?;

            header_ranges.head().ok_or(StoreError::NotFound)
        })
        .await
    }

    async fn get_by_hash(&self, hash: &Hash) -> Result<ExtendedHeader> {
        let hash = *hash;

        self.read_tx(move |tx| {
            let heights_table = tx.open_table(HEIGHTS_TABLE)?;
            let headers_table = tx.open_table(HEADERS_TABLE)?;

            let height = get_height(&heights_table, hash.as_bytes())?;
            get_header(&headers_table, height)
        })
        .await
    }

    async fn get_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        self.read_tx(move |tx| {
            let table = tx.open_table(HEADERS_TABLE)?;
            get_header(&table, height)
        })
        .await
    }

    async fn get_head(&self) -> Result<ExtendedHeader> {
        self.read_tx(|tx| {
            let ranges_table = tx.open_table(RANGES_TABLE)?;
            let headers_table = tx.open_table(HEADERS_TABLE)?;

            let header_ranges = get_ranges(&ranges_table, HEADER_RANGES_KEY)?;
            let head = header_ranges.head().ok_or(StoreError::NotFound)?;

            get_header(&headers_table, head)
        })
        .await
    }

    async fn contains_hash(&self, hash: &Hash) -> bool {
        let hash = *hash;

        self.read_tx(move |tx| {
            let heights_table = tx.open_table(HEIGHTS_TABLE)?;
            let headers_table = tx.open_table(HEADERS_TABLE)?;

            let height = get_height(&heights_table, hash.as_bytes())?;
            Ok(headers_table.get(height)?.is_some())
        })
        .await
        .unwrap_or(false)
    }

    async fn contains_height(&self, height: u64) -> bool {
        self.read_tx(move |tx| {
            let headers_table = tx.open_table(HEADERS_TABLE)?;
            Ok(headers_table.get(height)?.is_some())
        })
        .await
        .unwrap_or(false)
    }

    async fn insert<R>(&self, headers: R) -> Result<()>
    where
        R: TryInto<VerifiedExtendedHeaders> + Send,
        <R as TryInto<VerifiedExtendedHeaders>>::Error: Display,
    {
        let headers = headers
            .try_into()
            .map_err(|e| StoreInsertionError::HeadersVerificationFailed(e.to_string()))?;

        self.write_tx(move |tx| {
            let (Some(head), Some(tail)) = (headers.as_ref().first(), headers.as_ref().last())
            else {
                return Ok(());
            };
            let mut heights_table = tx.open_table(HEIGHTS_TABLE)?;
            let mut headers_table = tx.open_table(HEADERS_TABLE)?;
            let mut ranges_table = tx.open_table(RANGES_TABLE)?;

            let mut header_ranges = get_ranges(&ranges_table, HEADER_RANGES_KEY)?;
            let mut sampled_ranges = get_ranges(&ranges_table, SAMPLED_RANGES_KEY)?;
            let mut pruned_ranges = get_ranges(&ranges_table, PRUNED_RANGES_KEY)?;

            let headers_range = head.height()..=tail.height();

            let (prev_exists, next_exists) = header_ranges
                .check_insertion_constraints(&headers_range)
                .map_err(StoreInsertionError::ConstraintsNotMet)?;

            verify_against_neighbours(
                &headers_table,
                prev_exists.then_some(head),
                next_exists.then_some(tail),
            )?;

            for header in headers {
                let height = header.height();
                let hash = header.hash();
                let serialized_header = header.encode_vec();

                if headers_table
                    .insert(height, &serialized_header[..])?
                    .is_some()
                {
                    return Err(StoreError::StoredDataError(
                        "inconsistency between headers table and ranges table".into(),
                    ));
                }

                if heights_table.insert(hash.as_bytes(), height)?.is_some() {
                    // TODO: Replace this with `StoredDataError` when we implement
                    // type-safe validation on insertion.
                    return Err(StoreInsertionError::HashExists(hash).into());
                }

                trace!("Inserted header {hash} with height {height}");
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

            set_ranges(&mut ranges_table, HEADER_RANGES_KEY, &header_ranges)?;
            set_ranges(&mut ranges_table, SAMPLED_RANGES_KEY, &sampled_ranges)?;
            set_ranges(&mut ranges_table, PRUNED_RANGES_KEY, &pruned_ranges)?;

            debug!("Inserted header range {headers_range:?}",);

            Ok(())
        })
        .await?;

        self.inner.header_added_notifier.notify_waiters();

        Ok(())
    }

    async fn update_sampling_metadata(&self, height: u64, cids: Vec<Cid>) -> Result<()> {
        self.write_tx(move |tx| {
            let mut sampling_metadata_table = tx.open_table(SAMPLING_METADATA_TABLE)?;
            let ranges_table = tx.open_table(RANGES_TABLE)?;

            let header_ranges = get_ranges(&ranges_table, HEADER_RANGES_KEY)?;

            if !header_ranges.contains(height) {
                return Err(StoreError::NotFound);
            }

            let previous = get_sampling_metadata(&sampling_metadata_table, height)?;

            let entry = match previous {
                Some(mut previous) => {
                    for cid in cids {
                        if !previous.cids.contains(&cid) {
                            previous.cids.push(cid);
                        }
                    }

                    previous
                }
                None => SamplingMetadata { cids },
            };

            let serialized = entry.encode_vec();
            sampling_metadata_table.insert(height, &serialized[..])?;

            Ok(())
        })
        .await
    }

    async fn mark_as_sampled(&self, height: u64) -> Result<()> {
        self.write_tx(move |tx| {
            let mut ranges_table = tx.open_table(RANGES_TABLE)?;
            let header_ranges = get_ranges(&ranges_table, HEADER_RANGES_KEY)?;
            let mut sampled_ranges = get_ranges(&ranges_table, SAMPLED_RANGES_KEY)?;

            if !header_ranges.contains(height) {
                return Err(StoreError::NotFound);
            }

            sampled_ranges
                .insert_relaxed(height..=height)
                .expect("invalid height");

            set_ranges(&mut ranges_table, SAMPLED_RANGES_KEY, &sampled_ranges)?;

            Ok(())
        })
        .await
    }

    async fn get_sampling_metadata(&self, height: u64) -> Result<Option<SamplingMetadata>> {
        self.read_tx(move |tx| {
            let headers_table = tx.open_table(HEADERS_TABLE)?;
            let sampling_metadata_table = tx.open_table(SAMPLING_METADATA_TABLE)?;

            if headers_table.get(height)?.is_none() {
                return Err(StoreError::NotFound);
            }

            get_sampling_metadata(&sampling_metadata_table, height)
        })
        .await
    }

    async fn get_stored_ranges(&self) -> Result<BlockRanges> {
        self.read_tx(|tx| {
            let table = tx.open_table(RANGES_TABLE)?;
            get_ranges(&table, HEADER_RANGES_KEY)
        })
        .await
    }

    async fn get_sampled_ranges(&self) -> Result<BlockRanges> {
        self.read_tx(|tx| {
            let table = tx.open_table(RANGES_TABLE)?;
            get_ranges(&table, SAMPLED_RANGES_KEY)
        })
        .await
    }

    async fn get_pruned_ranges(&self) -> Result<BlockRanges> {
        self.read_tx(|tx| {
            let table = tx.open_table(RANGES_TABLE)?;
            get_ranges(&table, PRUNED_RANGES_KEY)
        })
        .await
    }

    async fn remove_height(&self, height: u64) -> Result<()> {
        self.write_tx(move |tx| {
            let mut heights_table = tx.open_table(HEIGHTS_TABLE)?;
            let mut headers_table = tx.open_table(HEADERS_TABLE)?;
            let mut ranges_table = tx.open_table(RANGES_TABLE)?;
            let mut sampling_metadata_table = tx.open_table(SAMPLING_METADATA_TABLE)?;

            let mut header_ranges = get_ranges(&ranges_table, HEADER_RANGES_KEY)?;
            let mut sampled_ranges = get_ranges(&ranges_table, SAMPLED_RANGES_KEY)?;
            let mut pruned_ranges = get_ranges(&ranges_table, PRUNED_RANGES_KEY)?;

            if !header_ranges.contains(height) {
                return Err(StoreError::NotFound);
            }

            let Some(header) = headers_table.remove(height)? else {
                return Err(StoreError::StoredDataError(format!(
                    "inconsistency between ranges and height_to_hash tables, height {height}"
                )));
            };

            let hash = ExtendedHeader::decode(header.value())
                .map_err(|e| StoreError::StoredDataError(e.to_string()))?
                .hash();

            if heights_table.remove(hash.as_bytes())?.is_none() {
                return Err(StoreError::StoredDataError(format!(
                    "inconsistency between header and height_to_hash tables, hash {hash}"
                )));
            }

            sampling_metadata_table.remove(height)?;

            header_ranges
                .remove_relaxed(height..=height)
                .expect("valid range never fails");
            sampled_ranges
                .remove_relaxed(height..=height)
                .expect("valid range never fails");
            pruned_ranges
                .insert_relaxed(height..=height)
                .expect("valid range never fails");

            set_ranges(&mut ranges_table, HEADER_RANGES_KEY, &header_ranges)?;
            set_ranges(&mut ranges_table, SAMPLED_RANGES_KEY, &sampled_ranges)?;
            set_ranges(&mut ranges_table, PRUNED_RANGES_KEY, &pruned_ranges)?;

            Ok(())
        })
        .await
    }

    async fn get_identity(&self) -> Result<Keypair> {
        self.read_tx(move |tx| {
            let identity_table = tx.open_table(LIBP2P_IDENTITY_TABLE)?;

            let (_, key_bytes) = identity_table
                .first()?
                .expect("identity_table should be non empty");

            Ok(Keypair::from_protobuf_encoding(key_bytes.value())?)
        })
        .await
    }
}

#[async_trait]
impl Store for RedbStore {
    async fn get_head(&self) -> Result<ExtendedHeader> {
        self.get_head().await
    }

    async fn get_by_hash(&self, hash: &Hash) -> Result<ExtendedHeader> {
        self.get_by_hash(hash).await
    }

    async fn get_by_height(&self, height: u64) -> Result<ExtendedHeader> {
        self.get_by_height(height).await
    }

    async fn wait_new_head(&self) -> u64 {
        let head = self.head_height().await.unwrap_or(0);
        let mut notifier = pin!(self.inner.header_added_notifier.notified());

        loop {
            let new_head = self.head_height().await.unwrap_or(0);

            if head != new_head {
                return new_head;
            }

            // Await for a notification
            notifier.as_mut().await;

            // Reset notifier
            notifier.set(self.inner.header_added_notifier.notified());
        }
    }

    async fn wait_height(&self, height: u64) -> Result<()> {
        let mut notifier = pin!(self.inner.header_added_notifier.notified());

        loop {
            if self.contains_height(height).await {
                return Ok(());
            }

            // Await for a notification
            notifier.as_mut().await;

            // Reset notifier
            notifier.set(self.inner.header_added_notifier.notified());
        }
    }

    async fn head_height(&self) -> Result<u64> {
        self.head_height().await
    }

    async fn has(&self, hash: &Hash) -> bool {
        self.contains_hash(hash).await
    }

    async fn has_at(&self, height: u64) -> bool {
        self.contains_height(height).await
    }

    async fn insert<R>(&self, headers: R) -> Result<()>
    where
        R: TryInto<VerifiedExtendedHeaders> + Send,
        <R as TryInto<VerifiedExtendedHeaders>>::Error: Display,
    {
        self.insert(headers).await
    }

    async fn update_sampling_metadata(&self, height: u64, cids: Vec<Cid>) -> Result<()> {
        self.update_sampling_metadata(height, cids).await
    }

    async fn mark_as_sampled(&self, height: u64) -> Result<()> {
        self.mark_as_sampled(height).await
    }

    async fn get_sampling_metadata(&self, height: u64) -> Result<Option<SamplingMetadata>> {
        self.get_sampling_metadata(height).await
    }

    async fn get_stored_header_ranges(&self) -> Result<BlockRanges> {
        Ok(self.get_stored_ranges().await?)
    }

    async fn get_sampled_ranges(&self) -> Result<BlockRanges> {
        self.get_sampled_ranges().await
    }

    async fn get_pruned_ranges(&self) -> Result<BlockRanges> {
        self.get_pruned_ranges().await
    }

    async fn remove_height(&self, height: u64) -> Result<()> {
        self.remove_height(height).await
    }

    async fn get_identity(&self) -> Result<Keypair> {
        self.get_identity().await
    }

    async fn close(mut self) -> Result<()> {
        // Wait all ongoing `spawn_blocking` tasks to finish.
        self.task_counter.wait_guards().await;
        Ok(())
    }
}

fn verify_against_neighbours<R>(
    headers_table: &R,
    lowest_header: Option<&ExtendedHeader>,
    highest_header: Option<&ExtendedHeader>,
) -> Result<()>
where
    R: ReadableTable<u64, &'static [u8]>,
{
    if let Some(lowest_header) = lowest_header {
        let prev = get_header(headers_table, lowest_header.height() - 1).map_err(|e| {
            if let StoreError::NotFound = e {
                StoreError::StoredDataError("inconsistency between headers and ranges table".into())
            } else {
                e
            }
        })?;

        prev.verify(lowest_header)
            .map_err(|e| StoreInsertionError::NeighborsVerificationFailed(e.to_string()))?;
    }

    if let Some(highest_header) = highest_header {
        let next = get_header(headers_table, highest_header.height() + 1).map_err(|e| {
            if let StoreError::NotFound = e {
                StoreError::StoredDataError("inconsistency between headers and ranges table".into())
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

fn get_ranges<R>(ranges_table: &R, name: &str) -> Result<BlockRanges>
where
    R: ReadableTable<&'static str, Vec<(u64, u64)>>,
{
    let raw_ranges = ranges_table
        .get(name)?
        .map(|guard| {
            guard
                .value()
                .iter()
                .map(|(start, end)| *start..=*end)
                .collect()
        })
        .unwrap_or_default();

    BlockRanges::from_vec(raw_ranges).map_err(|e| {
        let s = format!("Stored BlockRanges for {name} are invalid: {e}");
        StoreError::StoredDataError(s)
    })
}

fn set_ranges(
    ranges_table: &mut Table<&str, Vec<(u64, u64)>>,
    name: &str,
    ranges: &BlockRanges,
) -> Result<()> {
    let raw_ranges: &[RangeInclusive<u64>] = ranges.as_ref();
    let raw_ranges = raw_ranges
        .iter()
        .map(|range| (*range.start(), *range.end()))
        .collect::<Vec<_>>();

    ranges_table.insert(name, raw_ranges)?;

    Ok(())
}

#[inline]
fn get_height<R>(heights_table: &R, key: &[u8]) -> Result<u64>
where
    R: ReadableTable<&'static [u8], u64>,
{
    heights_table
        .get(key)?
        .map(|guard| guard.value())
        .ok_or(StoreError::NotFound)
}

#[inline]
fn get_header<R>(headers_table: &R, key: u64) -> Result<ExtendedHeader>
where
    R: ReadableTable<u64, &'static [u8]>,
{
    let serialized = headers_table.get(key)?.ok_or(StoreError::NotFound)?;
    deserialize_extended_header(serialized.value())
}

#[inline]
fn get_sampling_metadata<R>(
    sampling_metadata_table: &R,
    key: u64,
) -> Result<Option<SamplingMetadata>>
where
    R: ReadableTable<u64, &'static [u8]>,
{
    sampling_metadata_table
        .get(key)?
        .map(|guard| deserialize_sampling_metadata(guard.value()))
        .transpose()
}

impl From<TransactionError> for StoreError {
    fn from(e: TransactionError) -> Self {
        match e {
            TransactionError::ReadTransactionStillInUse(_) => {
                unreachable!("redb::ReadTransaction::close is never used")
            }
            e => StoreError::FatalDatabaseError(format!("TransactionError: {e}")),
        }
    }
}

impl From<TableError> for StoreError {
    fn from(e: TableError) -> Self {
        match e {
            TableError::Storage(e) => e.into(),
            TableError::TableAlreadyOpen(table, location) => {
                panic!("Table {table} already opened from: {location}")
            }
            TableError::TableDoesNotExist(table) => {
                panic!("Table {table} was not created on initialization")
            }
            e => StoreError::StoredDataError(format!("TableError: {e}")),
        }
    }
}

impl From<StorageError> for StoreError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::ValueTooLarge(_) => {
                unreachable!("redb::Table::insert_reserve is never used")
            }
            e => StoreError::FatalDatabaseError(format!("StorageError: {e}")),
        }
    }
}

impl From<CommitError> for StoreError {
    fn from(e: CommitError) -> Self {
        StoreError::FatalDatabaseError(format!("CommitError: {e}"))
    }
}

fn migrate_v1_to_v2(
    tx: &WriteTransaction,
    schema_version_table: &mut Table<(), u64>,
) -> Result<()> {
    const HEADER_HEIGHT_RANGES: TableDefinition<'static, u64, (u64, u64)> =
        TableDefinition::new("STORE.HEIGHT_RANGES");

    let version = schema_version_table
        .get(())?
        .map(|guard| guard.value())
        .expect("migrations never run on new db");

    if version >= 2 {
        // Nothing to migrate.
        return Ok(());
    }

    debug_assert_eq!(version, 1);
    warn!("Migrating DB schema from v1 to v2");

    let header_ranges_table = tx.open_table(HEADER_HEIGHT_RANGES)?;
    let mut ranges_table = tx.open_table(RANGES_TABLE)?;

    let raw_ranges = header_ranges_table
        .iter()?
        .map(|range_guard| {
            let range = range_guard?.1.value();
            Ok((range.0, range.1))
        })
        .collect::<Result<Vec<_>>>()?;

    tx.delete_table(header_ranges_table)?;
    ranges_table.insert(HEADER_RANGES_KEY, raw_ranges)?;

    // Migrated to v2
    schema_version_table.insert((), 2)?;

    Ok(())
}

fn migrate_v2_to_v3(
    tx: &WriteTransaction,
    schema_version_table: &mut Table<(), u64>,
) -> Result<()> {
    let version = schema_version_table
        .get(())?
        .map(|guard| guard.value())
        .expect("migrations never run on new db");

    if version >= 3 {
        // Nothing to migrate.
        return Ok(());
    }

    debug_assert_eq!(version, 2);
    warn!("Migrating DB schema from v2 to v3");

    // There are two changes in v3:
    //
    // * Removal of `SamplingStatus` in `SamplingMetadata`
    // * Rename of sampled ranges database key
    //
    // For the first one we don't need to take any actions because it will
    // be ignored by the deserializer.
    let mut ranges_table = tx.open_table(RANGES_TABLE)?;
    let sampled_ranges = get_ranges(&ranges_table, v2::SAMPLED_RANGES_KEY)?;
    set_ranges(&mut ranges_table, SAMPLED_RANGES_KEY, &sampled_ranges)?;
    ranges_table.remove(v2::SAMPLED_RANGES_KEY)?;

    // Migrated to v3
    schema_version_table.insert((), 3)?;

    Ok(())
}

mod v2 {
    pub(super) const SAMPLED_RANGES_KEY: &str = "KEY.ACCEPTED_SAMPING_RANGES";
}

#[cfg(test)]
// One-element arrays here really are arrays of ranges, not a shorthand for a range of values.
#[allow(clippy::single_range_in_vec_init)]
pub mod tests {
    use super::*;
    use crate::test_utils::ExtendedHeaderGeneratorExt;
    use celestia_types::test_utils::ExtendedHeaderGenerator;
    use std::path::Path;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_store_persistence() {
        let db_dir = TempDir::with_prefix("lumina.store.test").unwrap();
        let db = db_dir.path().join("db");

        let (original_store, mut generator) = gen_filled_store(0, Some(&db)).await;
        let mut original_headers = generator.next_many(20);

        original_store
            .insert(original_headers.clone())
            .await
            .expect("inserting test data failed");
        drop(original_store);

        let reopened_store = create_store(Some(&db)).await;

        assert_eq!(
            original_headers.last().unwrap().height(),
            reopened_store.head_height().await.unwrap()
        );
        for original_header in &original_headers {
            let stored_header = reopened_store
                .get_by_height(original_header.height())
                .await
                .unwrap();
            assert_eq!(original_header, &stored_header);
        }

        let mut new_headers = generator.next_many(10);
        reopened_store
            .insert(new_headers.clone())
            .await
            .expect("failed to insert data");
        drop(reopened_store);

        original_headers.append(&mut new_headers);

        let reopened_store = create_store(Some(&db)).await;
        assert_eq!(
            original_headers.last().unwrap().height(),
            reopened_store.head_height().await.unwrap()
        );
        for original_header in &original_headers {
            let stored_header = reopened_store
                .get_by_height(original_header.height())
                .await
                .unwrap();
            assert_eq!(original_header, &stored_header);
        }
    }

    #[tokio::test]
    async fn test_separate_stores() {
        let (store0, mut generator0) = gen_filled_store(0, None).await;
        let store1 = create_store(None).await;

        let headers = generator0.next_many(10);
        store0.insert(headers.clone()).await.unwrap();
        store1.insert(headers).await.unwrap();

        let mut generator1 = generator0.fork();

        store0
            .insert(generator0.next_many_verified(5))
            .await
            .unwrap();
        store1
            .insert(generator1.next_many_verified(6))
            .await
            .unwrap();

        assert_eq!(
            store0.get_by_height(10).await.unwrap(),
            store1.get_by_height(10).await.unwrap()
        );
        assert_ne!(
            store0.get_by_height(11).await.unwrap(),
            store1.get_by_height(11).await.unwrap()
        );

        assert_eq!(store0.head_height().await.unwrap(), 15);
        assert_eq!(store1.head_height().await.unwrap(), 16);
    }

    #[tokio::test]
    async fn test_identity_persistance() {
        let db_dir = TempDir::with_prefix("lumina.store.test").unwrap();
        let db = db_dir.path().join("db");

        let original_store = create_store(Some(&db)).await;
        let original_identity = original_store.get_identity().await.unwrap().public();
        drop(original_store);

        let reopened_store = create_store(Some(&db)).await;
        let reopened_identity = reopened_store.get_identity().await.unwrap().public();

        assert_eq!(original_identity, reopened_identity);
    }

    #[tokio::test]
    async fn migration_from_v2() {
        const SCHEMA_VERSION_TABLE: TableDefinition<'static, (), u64> =
            TableDefinition::new("STORE.SCHEMA_VERSION");
        const RANGES_TABLE: TableDefinition<'static, &str, Vec<(u64, u64)>> =
            TableDefinition::new("STORE.RANGES");

        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .unwrap();
        let db = Arc::new(db);

        // Prepare a v2 db
        tokio::task::spawn_blocking({
            let db = db.clone();
            move || {
                let tx = db.begin_write().unwrap();

                {
                    let mut schema_version_table = tx.open_table(SCHEMA_VERSION_TABLE).unwrap();
                    schema_version_table.insert((), 2).unwrap();

                    let mut ranges_table = tx.open_table(RANGES_TABLE).unwrap();
                    ranges_table
                        .insert(v2::SAMPLED_RANGES_KEY, vec![(123, 124)])
                        .unwrap();
                }

                tx.commit().unwrap();
            }
        })
        .await
        .unwrap();

        // Migrate and check store
        let store = RedbStore::new(db.clone()).await.unwrap();
        let ranges = store.get_sampled_ranges().await.unwrap();
        assert_eq!(ranges, BlockRanges::try_from([123..=124]).unwrap());
        store.close().await.unwrap();

        // Check that old ranges were deleted
        tokio::task::spawn_blocking({
            let db = db.clone();
            move || {
                let tx = db.begin_read().unwrap();
                let ranges_table = tx.open_table(RANGES_TABLE).unwrap();
                assert!(ranges_table.get(v2::SAMPLED_RANGES_KEY).unwrap().is_none());
            }
        })
        .await
        .unwrap();
    }

    pub async fn create_store(path: Option<&Path>) -> RedbStore {
        match path {
            Some(path) => RedbStore::open(path).await.unwrap(),
            None => RedbStore::in_memory().await.unwrap(),
        }
    }

    pub async fn gen_filled_store(
        amount: u64,
        path: Option<&Path>,
    ) -> (RedbStore, ExtendedHeaderGenerator) {
        let s = create_store(path).await;
        let mut generator = ExtendedHeaderGenerator::new();
        let headers = generator.next_many(amount);

        s.insert(headers).await.expect("inserting test data failed");

        (s, generator)
    }
}
