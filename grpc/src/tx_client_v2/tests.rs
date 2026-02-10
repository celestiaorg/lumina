use super::*;
use async_trait::async_trait;
use celestia_types::state::RawTxBody;
use prost::Message;
use std::collections::VecDeque;
use std::sync::Once;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

type TestTxId = u64;
type TestConfirmInfo = ();
type TestConfirmResponse = ();
type TestSubmitError = ();
type TestStatus = TxStatus<TestConfirmInfo, TestConfirmResponse>;
type TestSigningResult =
    TxSigningResult<TestTxId, TestConfirmInfo, TestConfirmResponse, TestSubmitError>;
type TestSubmitResult = TxSubmitResult<TestTxId, TestSubmitError>;
type TestConfirmResult = ConfirmResult<TestConfirmInfo, TestSubmitError, TestConfirmResponse>;

fn status(kind: TxStatusKind<TestConfirmInfo>) -> TestStatus {
    TxStatus::new(kind, ())
}

fn submit_failure(mapped_error: SubmitError) -> SubmitFailure<TestSubmitError> {
    SubmitFailure {
        mapped_error,
        original_error: (),
    }
}

fn init_tracing() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let level = std::env::var("RUST_LOG")
            .ok()
            .map(|value| value.to_lowercase())
            .and_then(|value| match value.as_str() {
                "trace" => Some(tracing::Level::TRACE),
                "debug" => Some(tracing::Level::DEBUG),
                "info" => Some(tracing::Level::INFO),
                "warn" => Some(tracing::Level::WARN),
                "error" => Some(tracing::Level::ERROR),
                _ => None,
            })
            .unwrap_or(tracing::Level::WARN);
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .with_max_level(level)
            .try_init();
    });
}

#[derive(Debug)]
struct RoutedCall {
    node_id: NodeId,
    call: ServerCall,
}

impl RoutedCall {
    fn new(node_id: NodeId, call: ServerCall) -> Self {
        Self { node_id, call }
    }
}

type StatusBatchReply = oneshot::Sender<TxConfirmResult<Vec<(TestTxId, TestStatus)>>>;

#[derive(Debug)]
#[allow(dead_code)]
enum ServerCall {
    SimulateAndSign {
        #[allow(dead_code)]
        request: Arc<TxRequest>,
        sequence: u64,
        reply: oneshot::Sender<TestSigningResult>,
    },
    Submit {
        #[allow(dead_code)]
        bytes: Vec<u8>,
        sequence: u64,
        reply: oneshot::Sender<TestSubmitResult>,
    },
    StatusBatch {
        ids: Vec<TestTxId>,
        reply: StatusBatchReply,
    },
    Status {
        id: TestTxId,
        reply: oneshot::Sender<TxConfirmResult<TestStatus>>,
    },
    CurrentSequence {
        reply: oneshot::Sender<Result<u64>>,
    },
}

#[derive(Debug)]
struct MockTxServer {
    node_id: NodeId,
    calls: mpsc::Sender<RoutedCall>,
}

impl MockTxServer {
    fn new(node_id: NodeId, calls: mpsc::Sender<RoutedCall>) -> Self {
        Self { node_id, calls }
    }
    async fn send_call(&self, call: ServerCall, msg: &str) {
        self.calls
            .send(RoutedCall::new(self.node_id.clone(), call))
            .await
            .expect(msg);
    }
}

fn make_many_servers(
    num_servers: usize,
) -> (mpsc::Receiver<RoutedCall>, Vec<(NodeId, Arc<MockTxServer>)>) {
    let mut servers = Vec::with_capacity(num_servers);
    let (calls_tx, calls_rx) = mpsc::channel(64);
    for i in 0..num_servers {
        let node_name: NodeId = Arc::from(format!("node-{}", i));
        let server = Arc::new(MockTxServer::new(node_name.clone(), calls_tx.clone()));
        servers.push((node_name, server));
    }
    (calls_rx, servers)
}

#[derive(Debug, Clone)]
struct NodeState {
    confirmed_sequence: Option<u64>,
    mempool: VecDeque<TestStatus>,
}

impl NodeState {
    fn new() -> Self {
        Self {
            confirmed_sequence: Some(0),
            mempool: VecDeque::new(),
        }
    }

    fn max_sequence(&self) -> Option<u64> {
        if self.mempool.is_empty() {
            return self.confirmed_sequence;
        }
        let base = self
            .confirmed_sequence
            .map(|seq| seq.saturating_add(1))
            .unwrap_or(0);
        base.checked_add(self.mempool.len().saturating_sub(1) as u64)
    }

    fn next_submit_seq(&self) -> u64 {
        let last_non_evicted = self
            .mempool
            .iter()
            .rposition(|s| !matches!(s.kind, TxStatusKind::Evicted));

        let next_index = match last_non_evicted {
            Some(i) => i + 1,
            None => 0,
        };

        self.confirmed_sequence
            .map(|seq| seq.saturating_add(1))
            .unwrap_or(0)
            + next_index as u64
    }

    fn truncate_to_seq(&mut self, seq: u64) {
        // Keep mempool entries for sequences <= seq, drop the rest.
        let start = self
            .confirmed_sequence
            .map(|s| s.saturating_add(1))
            .unwrap_or(0);
        if seq < start {
            self.mempool.clear();
            return;
        }
        let keep_len = (seq - start + 1) as usize;
        if keep_len < self.mempool.len() {
            self.mempool.truncate(keep_len);
        }
    }

    fn status_of(&self, tx: TestTxId) -> TestStatus {
        if let Some(confirmed) = self.confirmed_sequence
            && tx <= confirmed
        {
            return status(TxStatusKind::Confirmed { info: () });
        }
        let start = self
            .confirmed_sequence
            .map(|s| s.saturating_add(1))
            .unwrap_or(0);
        let idx = (tx - start) as usize;
        self.mempool
            .get(idx)
            .cloned()
            .unwrap_or_else(|| status(TxStatusKind::Unknown))
    }
}

#[derive(Debug)]
struct NetworkState {
    node_states: HashMap<NodeId, NodeState>,
}

enum TxModifier {
    CorrectTx,
}

impl NetworkState {
    fn new(node_states: HashMap<NodeId, NodeState>) -> Self {
        Self { node_states }
    }

    fn submit(
        &mut self,
        node_id: NodeId,
        tx: TestTxId,
        tx_modifier: TxModifier,
    ) -> TestSubmitResult {
        let state = self.node_states.get_mut(&node_id).unwrap();

        let expected_seq = state.next_submit_seq();
        if tx != expected_seq {
            return Err(submit_failure(SubmitError::SequenceMismatch {
                expected: expected_seq,
            }));
        }

        match tx_modifier {
            TxModifier::CorrectTx => {
                state.truncate_to_seq(tx - 1);
                state.mempool.push_back(status(TxStatusKind::Pending));
            }
        }

        Ok(tx)
    }

    fn confirm_one(&mut self, node_id: NodeId, tx: TestTxId) -> TestStatus {
        let state = self.node_states.get_mut(&node_id).unwrap();
        state.status_of(tx)
    }

    fn confirm_batch(&mut self, node_id: NodeId, txs: &[TestTxId]) -> Vec<(TestTxId, TestStatus)> {
        let state = self.node_states.get_mut(&node_id).unwrap();
        txs.iter().map(|&tx| (tx, state.status_of(tx))).collect()
    }

    fn sequence(&mut self, node_id: NodeId) -> u64 {
        let state = self.node_states.get_mut(&node_id).unwrap();
        state.next_submit_seq()
    }

    fn update_confirmed_sequence(&mut self, node_id: NodeId, seq_id: u64) {
        let state = self.node_states.get_mut(&node_id).unwrap();
        if let Some(confirmed) = state.confirmed_sequence
            && seq_id <= confirmed
        {
            return;
        }

        let start = state
            .confirmed_sequence
            .map(|s| s.saturating_add(1))
            .unwrap_or(0);
        if seq_id < start {
            return;
        }
        let drain = (seq_id - start + 1) as usize;
        let drain = drain.min(state.mempool.len());
        for _ in 0..drain {
            state.mempool.pop_front();
        }

        state.confirmed_sequence = Some(seq_id);
    }

    fn max_submitted_sequence(&self) -> u64 {
        self.node_states
            .values()
            .map(|state| state.next_submit_seq())
            .max()
            .unwrap_or(0)
    }

    fn update_confirmed_all(&mut self, seq_id: u64) {
        let nodes = self.node_states.keys().cloned().collect::<Vec<_>>();
        for node in nodes {
            self.update_confirmed_sequence(node, seq_id);
        }
    }

    fn evict(&mut self, node_id: NodeId, seq_id: u64) {
        let state = self.node_states.get_mut(&node_id).unwrap();

        let Some(max_seq) = state.max_sequence() else {
            return;
        };
        if let Some(confirmed) = state.confirmed_sequence {
            if seq_id <= confirmed || seq_id > max_seq {
                return;
            }
        } else if seq_id > max_seq {
            return;
        }

        let start = state
            .confirmed_sequence
            .map(|s| s.saturating_add(1))
            .unwrap_or(0);
        if seq_id < start {
            return;
        }
        let from_idx = (seq_id - start) as usize;
        for i in from_idx..state.mempool.len() {
            state.mempool[i] = status(TxStatusKind::Evicted);
        }
    }
}

#[derive(Debug)]
enum ServerReturn {
    Signing(TestSigningResult),
    Submit(TestSubmitResult),
    StatusBatch(TxConfirmResult<Vec<(TestTxId, TestStatus)>>),
    Status(TxConfirmResult<TestStatus>),
    CurrentSequence(Result<u64>),
}

impl ServerReturn {
    fn assert_submit(self) -> TestSubmitResult {
        match self {
            ServerReturn::Submit(result) => result,
            _ => panic!("expected Submit"),
        }
    }

    fn assert_signing(self) -> TestSigningResult {
        match self {
            ServerReturn::Signing(result) => result,
            _ => panic!("expected Signing"),
        }
    }

    fn assert_status_batch(self) -> TxConfirmResult<Vec<(TestTxId, TestStatus)>> {
        match self {
            ServerReturn::StatusBatch(result) => result,
            _ => panic!("expected StatusBatch"),
        }
    }

    fn assert_status(self) -> TxConfirmResult<TestStatus> {
        match self {
            ServerReturn::Status(result) => result,
            _ => panic!("expected Status"),
        }
    }

    fn assert_sequence(self) -> Result<u64> {
        match self {
            ServerReturn::CurrentSequence(result) => result,
            _ => panic!("expected CurrentSequence"),
        }
    }
}

type ActionFunc =
    dyn for<'a> Fn(&'a RoutedCall, &mut NetworkState) -> ActionResult + Send + Sync + 'static;
type MatchFunc = dyn for<'a> Fn(&'a RoutedCall) -> bool + Send + Sync + 'static;
type PeriodicFunc = dyn for<'a> Fn(&mut NetworkState) + Send + Sync + 'static;

struct ActionResult {
    ret: ServerReturn,
}

#[derive(Clone)]
struct Match {
    name: &'static str,
    check_func: Arc<MatchFunc>,
}

impl Match {
    fn new<F>(name: &'static str, f: F) -> Self
    where
        F: for<'a> Fn(&'a RoutedCall) -> bool + Send + Sync + 'static,
    {
        Self {
            name,
            check_func: Arc::new(f),
        }
    }

    fn check(&self, call: &RoutedCall) -> bool {
        (self.check_func)(call)
    }
}

impl std::fmt::Debug for Match {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Match").field("name", &self.name).finish()
    }
}

#[derive(Clone)]
struct Action {
    name: &'static str,
    action: Arc<ActionFunc>,
}

impl Action {
    fn new<F>(name: &'static str, f: F) -> Self
    where
        F: for<'a> Fn(&'a RoutedCall, &mut NetworkState) -> ActionResult + Send + Sync + 'static,
    {
        Self {
            name,
            action: Arc::new(f),
        }
    }
    fn call(&self, rc: &RoutedCall, state: &mut NetworkState) -> ActionResult {
        (self.action)(rc, state)
    }
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        f.debug_struct("Action").field("name", &self.name).finish()
    }
}

#[derive(Debug, Clone)]
enum Cardinality {
    Once,
    Times(u32),
    Any,
}

impl Cardinality {
    fn consume(&mut self) {
        match self {
            Cardinality::Once => *self = Cardinality::Times(0),
            Cardinality::Times(n) => {
                if *n > 0 {
                    *n -= 1;
                }
            }
            Cardinality::Any => {}
        }
    }
    fn exhausted(&self) -> bool {
        matches!(self, Cardinality::Times(0))
    }
    fn requires_consumption(&self) -> bool {
        !matches!(self, Cardinality::Any)
    }
}

struct PeriodicCall {
    call: Arc<PeriodicFunc>,
}

impl PeriodicCall {
    fn new<F>(call: F) -> Self
    where
        F: for<'a> Fn(&mut NetworkState) + Send + Sync + 'static,
    {
        Self {
            call: Arc::new(call),
        }
    }

    fn call(&self, states: &mut NetworkState) {
        (self.call)(states);
    }
}

#[derive(Debug, Clone)]
struct Rule {
    #[allow(dead_code)]
    name: &'static str,
    matcher: Match,
    action: Action,
    card: Cardinality,
    priority: usize,
}

impl Rule {
    fn new(
        name: &'static str,
        matcher: Match,
        action: Action,
        card: Cardinality,
        priority: usize,
    ) -> Self {
        Self {
            name,
            matcher,
            action,
            card,
            priority,
        }
    }

    fn matches(&self, call: &RoutedCall) -> bool {
        if self.card.exhausted() {
            return false;
        }
        self.matcher.check(call)
    }

    fn consume(&mut self) {
        self.card.consume();
    }

    fn call(&self, rc: &RoutedCall, states: &mut NetworkState) -> ActionResult {
        self.action.call(rc, states)
    }
}

#[async_trait]
impl TxServer for MockTxServer {
    type TxId = TestTxId;
    type ConfirmInfo = TestConfirmInfo;
    type TxRequest = TxRequest;
    type SubmitError = TestSubmitError;
    type ConfirmResponse = TestConfirmResponse;

    async fn submit(&self, bytes: Arc<Vec<u8>>, sequence: u64) -> TestSubmitResult {
        let (reply, rx) = oneshot::channel();
        let ret = ServerCall::Submit {
            bytes: bytes.to_vec(),
            sequence,
            reply,
        };
        self.send_call(ret, "submit call").await;
        rx.await.expect("submit reply")
    }

    async fn status_batch(
        &self,
        ids: Vec<TestTxId>,
    ) -> TxConfirmResult<Vec<(TestTxId, TestStatus)>> {
        let (reply, rx) = oneshot::channel();
        let ret = ServerCall::StatusBatch { ids, reply };
        self.send_call(ret, "status batch call").await;
        rx.await.expect("status batch reply")
    }

    async fn status(&self, id: Self::TxId) -> TxConfirmResult<TestStatus> {
        let (reply, rx) = oneshot::channel();
        let ret = ServerCall::Status { id, reply };
        self.send_call(ret, "status call").await;
        rx.await.expect("status reply")
    }

    async fn current_sequence(&self) -> Result<u64> {
        let (reply, rx) = oneshot::channel();
        let ret = ServerCall::CurrentSequence { reply };
        self.send_call(ret, "current sequence call").await;
        rx.await.expect("current sequence reply")
    }

    async fn simulate_and_sign(
        &self,
        req: Arc<Self::TxRequest>,
        sequence: u64,
    ) -> TestSigningResult {
        let (reply, rx) = oneshot::channel();
        let ret = ServerCall::SimulateAndSign {
            request: req,
            sequence,
            reply,
        };
        self.send_call(ret, "simulate and sign call").await;
        rx.await.expect("signing reply")
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct CallLogEntry {
    #[allow(dead_code)]
    node_id: NodeId,
    kind: &'static str,
    seq: Option<u64>,
    ids: Vec<TestTxId>,
}

#[allow(dead_code)]
impl CallLogEntry {
    fn new(node_id: NodeId, kind: &'static str, seq: Option<u64>, ids: Vec<TestTxId>) -> Self {
        Self {
            node_id,
            kind,
            seq,
            ids,
        }
    }

    fn println(&self) {
        println!(
            "logging call: {}, {:?}, {:?}",
            self.kind, self.seq, &self.ids
        );
    }
}

impl From<&RoutedCall> for CallLogEntry {
    fn from(rc: &RoutedCall) -> Self {
        let node_id = rc.node_id.clone();
        match &rc.call {
            ServerCall::CurrentSequence { .. } => {
                CallLogEntry::new(node_id, "CurrentSequence", None, vec![])
            }
            ServerCall::SimulateAndSign { sequence, .. } => {
                CallLogEntry::new(node_id, "Signing", Some(*sequence), vec![])
            }
            ServerCall::Status { id, .. } => CallLogEntry::new(node_id, "Status", None, vec![*id]),
            ServerCall::StatusBatch { ids, .. } => {
                CallLogEntry::new(node_id, "StatusBatch", None, ids.clone())
            }
            ServerCall::Submit { sequence, .. } => {
                CallLogEntry::new(node_id, "Submit", Some(*sequence), vec![])
            }
        }
    }
}

#[derive(Debug)]
struct Driver {
    inbox: mpsc::Receiver<RoutedCall>,
    rules: Vec<Rule>,
    network_state: NetworkState,
    log: Vec<CallLogEntry>,
}

impl Driver {
    fn new(
        inbox: mpsc::Receiver<RoutedCall>,
        rules: Vec<Rule>,
        states: HashMap<NodeId, NodeState>,
    ) -> Self {
        Self {
            inbox,
            rules,
            log: Vec::new(),
            network_state: NetworkState::new(states),
        }
    }

    fn log_call(&mut self, call: &RoutedCall) {
        self.log.push(call.into());
    }

    async fn process_inbox(
        &mut self,
        periodic: PeriodicCall,
        shutdown: CancellationToken,
    ) -> DriverResult {
        let mut timer = time::Interval::new(Duration::from_millis(10));
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    break;
                }
                _ = timer.tick() => {
                    periodic.call(&mut self.network_state);
                }
                msg = self.inbox.recv() => {
                    let Some(rc) = msg else {
                        continue;
                    };
                    self.log_call(&rc);
                    let res = self.rules.iter_mut().find(|rule| rule.matches(&rc));
                    if let Some(rule) = res {
                        rule.consume();
                        let ret = rule.call(&rc, &mut self.network_state);
                        match rc.call {
                            ServerCall::SimulateAndSign { reply, .. } => {
                                let _ = reply.send(ret.ret.assert_signing());
                            }
                            ServerCall::Submit { reply, .. } => {
                                let _ = reply.send(ret.ret.assert_submit());
                            }
                            ServerCall::Status { reply, .. } => {
                                let _ = reply.send(ret.ret.assert_status());
                            }
                            ServerCall::StatusBatch { reply, .. } => {
                                let _ = reply.send(ret.ret.assert_status_batch());
                            }
                            ServerCall::CurrentSequence { reply, .. } => {
                                let _ = reply.send(ret.ret.assert_sequence());
                            }
                        }
                    }
                }
            }
        }
        let mut unmet = vec![];
        for rule in self.rules.iter() {
            if rule.card.requires_consumption() && !rule.card.exhausted() {
                unmet.push(rule.clone());
            }
        }
        DriverResult {
            unmet,
            log: self.log.clone(),
        }
    }
}

#[derive(Debug)]
struct DriverResult {
    #[allow(dead_code)]
    log: Vec<CallLogEntry>,
    #[allow(dead_code)]
    unmet: Vec<Rule>,
}

struct Harness {
    shutdown: CancellationToken,
    manager_handle: Option<JoinHandle<Result<()>>>,
    driver_handle: JoinHandle<DriverResult>,
    #[allow(dead_code)]
    confirm_interval: Duration,
    manager:
        TxSubmitter<TestTxId, TestConfirmInfo, TestConfirmResponse, TestSubmitError, TxRequest>,
}

impl Harness {
    fn new(
        confirm_interval: Duration,
        max_status_batch: usize,
        add_tx_capacity: usize,
        num_servers: usize,
        mut rules: Vec<Rule>,
        periodic: PeriodicCall,
    ) -> (Self, TransactionWorker<MockTxServer>) {
        let (calls_rx, servers) = make_many_servers(num_servers);
        let mut node_map = HashMap::new();
        let mut node_states = HashMap::new();
        for (node_id, node_server) in servers {
            node_map.insert(node_id.clone(), node_server);
            node_states.insert(node_id.clone(), NodeState::new());
        }
        let (manager, worker) = TransactionWorker::new(
            node_map,
            confirm_interval,
            max_status_batch,
            Some(0),
            add_tx_capacity,
        );
        rules.sort_by_key(|rule| std::cmp::Reverse(rule.priority));
        let mut driver = Driver::new(calls_rx, rules, node_states);
        let shutdown = CancellationToken::new();
        let process_shutdown = shutdown.clone();
        let driver_handle =
            tokio::spawn(async move { driver.process_inbox(periodic, process_shutdown).await });
        (
            Self {
                shutdown,
                manager_handle: None,
                driver_handle,
                confirm_interval,
                manager,
            },
            worker,
        )
    }

    fn start(&mut self, mut manager: TransactionWorker<MockTxServer>) {
        let shutdown = self.shutdown.clone();
        self.manager_handle = Some(tokio::spawn(async move { manager.process(shutdown).await }));
    }

    async fn stop(self) -> DriverResult {
        self.shutdown.cancel();
        let handle = self.manager_handle.unwrap();
        _ = handle.await;
        self.driver_handle.await.unwrap()
    }

    async fn add_tx(
        &self,
        bytes: Vec<u8>,
    ) -> (
        oneshot::Receiver<Result<()>>,
        oneshot::Receiver<Result<TestTxId>>,
        oneshot::Receiver<TestConfirmResult>,
    ) {
        let body = RawTxBody {
            memo: format!("test-bytes:{:?}", bytes),
            ..RawTxBody::default()
        };
        let handle = self
            .manager
            .add_tx(TxRequest::tx(body, TxConfig::default()))
            .await
            .expect("add tx");
        (handle.signed, handle.submitted, handle.confirmed)
    }
}

#[tokio::test]
async fn test_eviction() {
    init_tracing();
    let evict_sequence = 15;
    let rules: Vec<Rule> = vec![
        // Signing rule
        Rule::new(
            "sign",
            Match::new("match sign", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::SimulateAndSign { .. })
            }),
            Action::new(
                "sign_action",
                |rc: &RoutedCall, _states: &mut NetworkState| match rc.call {
                    ServerCall::SimulateAndSign {
                        ref request,
                        sequence,
                        ..
                    } => {
                        let bytes = match &request.as_ref().tx {
                            TxPayload::Tx(body) => body.encode_to_vec(),
                            TxPayload::Blobs(_) => Vec::new(),
                        };
                        ActionResult {
                            ret: ServerReturn::Signing(Ok(Transaction {
                                sequence,
                                bytes: Arc::new(bytes),
                                callbacks: TxCallbacks::default(),
                                id: None,
                            })),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
        // Generic submit rule
        Rule::new(
            "submit",
            Match::new("match submit", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::Submit { .. })
            }),
            Action::new(
                "submit_action",
                move |rc: &RoutedCall, states: &mut NetworkState| match rc.call {
                    ServerCall::Submit { sequence, .. } => {
                        let result =
                            states.submit(rc.node_id.clone(), sequence, TxModifier::CorrectTx);
                        ActionResult {
                            ret: ServerReturn::Submit(result),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
        // Submit evict rule (higher priority)
        Rule::new(
            "submit evict",
            Match::new(
                "match submit evict",
                move |rc: &RoutedCall| matches!(&rc.call, ServerCall::Submit { sequence, .. } if *sequence == evict_sequence),
            ),
            Action::new(
                "submit_evict_action",
                move |rc: &RoutedCall, states: &mut NetworkState| match rc.call {
                    ServerCall::Submit { .. } => {
                        states.evict(rc.node_id.clone(), 2);
                        ActionResult {
                            ret: ServerReturn::Submit(Err(submit_failure(
                                SubmitError::SequenceMismatch { expected: 2 },
                            ))),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Times(2),
            1,
        ),
        // Status rule
        Rule::new(
            "status",
            Match::new("match status", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::Status { .. })
            }),
            Action::new(
                "status_action",
                |rc: &RoutedCall, states: &mut NetworkState| match rc.call {
                    ServerCall::Status { id, .. } => {
                        let status = states.confirm_one(rc.node_id.clone(), id);
                        ActionResult {
                            ret: ServerReturn::Status(TxConfirmResult::Ok(status)),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
        // Status batch rule
        Rule::new(
            "status batch",
            Match::new("match status batch", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::StatusBatch { .. })
            }),
            Action::new(
                "status_batch_action",
                |rc: &RoutedCall, states: &mut NetworkState| match &rc.call {
                    ServerCall::StatusBatch { ids, .. } => {
                        let results = states.confirm_batch(rc.node_id.clone(), ids);
                        ActionResult {
                            ret: ServerReturn::StatusBatch(TxConfirmResult::Ok(results)),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
        // Sequence rule
        Rule::new(
            "sequence",
            Match::new("match sequence", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::CurrentSequence { .. })
            }),
            Action::new(
                "sequence_action",
                |rc: &RoutedCall, states: &mut NetworkState| match rc.call {
                    ServerCall::CurrentSequence { .. } => ActionResult {
                        ret: ServerReturn::CurrentSequence(Ok(states.sequence(rc.node_id.clone()))),
                    },
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
    ];
    let update = PeriodicCall::new(|states: &mut NetworkState| {
        let max_seq = states.max_submitted_sequence() - 1;
        states.update_confirmed_all(max_seq);
    });
    let (mut harness, manager_worker) =
        Harness::new(Duration::from_millis(1), 10, 1000, 2, rules, update);
    harness.start(manager_worker);
    let mut add_handles = VecDeque::new();
    for _ in 0..100 {
        let handle = harness.add_tx(vec![0, 0]).await;
        add_handles.push_back(handle);
    }
    let mut seq = 0;
    while let Some(handle) = add_handles.pop_front() {
        seq += 1;
        let (signed, submit, confirm) = handle;
        let signed = signed.await.expect("signed channel closed");
        assert!(
            signed.is_ok(),
            "signing failed for seq {}: {:?}",
            seq,
            signed
        );
        let submitted = submit.await.expect("submit channel closed");
        assert!(
            submitted.is_ok(),
            "submit failed for seq {}: {:?}",
            seq,
            submitted
        );
        let confirmed = confirm.await.expect("confirm channel closed");
        match confirmed {
            Ok(_) => {}
            other => panic!("confirm failed for seq {}: {:?}", seq, other),
        }
    }
    let _results = harness.stop().await;
}

#[tokio::test]
async fn test_recovering() {
    init_tracing();
    let evict_sequence = 15;
    let is_evicted = Arc::new(AtomicBool::new(false));
    let is_evicted_submit = is_evicted.clone();
    let is_evicted_check = is_evicted.clone();
    let recovery_triggered = Arc::new(AtomicBool::new(false));
    let recovery_triggered_check = recovery_triggered.clone();
    let recovery_triggered_check2 = recovery_triggered.clone();

    let rules: Vec<Rule> = vec![
        // Signing rule
        Rule::new(
            "sign",
            Match::new("match sign", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::SimulateAndSign { .. })
            }),
            Action::new(
                "sign_action",
                |rc: &RoutedCall, _states: &mut NetworkState| match rc.call {
                    ServerCall::SimulateAndSign {
                        ref request,
                        sequence,
                        ..
                    } => {
                        let bytes = match &request.as_ref().tx {
                            TxPayload::Tx(body) => body.encode_to_vec(),
                            TxPayload::Blobs(_) => Vec::new(),
                        };
                        ActionResult {
                            ret: ServerReturn::Signing(Ok(Transaction {
                                sequence,
                                bytes: Arc::new(bytes),
                                callbacks: TxCallbacks::default(),
                                id: None,
                            })),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
        // Generic submit rule
        Rule::new(
            "submit",
            Match::new("match submit", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::Submit { .. })
            }),
            Action::new(
                "submit_action",
                move |rc: &RoutedCall, states: &mut NetworkState| match rc.call {
                    ServerCall::Submit { sequence, .. } => {
                        let result =
                            states.submit(rc.node_id.clone(), sequence, TxModifier::CorrectTx);
                        if is_evicted_submit.load(Ordering::Relaxed)
                            && !recovery_triggered.load(Ordering::Relaxed)
                        {
                            states.update_confirmed_all(evict_sequence);
                            recovery_triggered.store(true, Ordering::Relaxed);
                        }
                        ActionResult {
                            ret: ServerReturn::Submit(result),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
        // Submit evict rule (higher priority)
        Rule::new(
            "submit evict",
            Match::new(
                "match submit evict",
                move |rc: &RoutedCall| matches!(&rc.call, ServerCall::Submit { sequence, .. } if *sequence == evict_sequence),
            ),
            Action::new(
                "submit_evict_action",
                move |rc: &RoutedCall, states: &mut NetworkState| match rc.call {
                    ServerCall::Submit { .. } => {
                        println!("[EVICT] Evicting at sequence {}", evict_sequence);
                        states.evict(rc.node_id.clone(), 2);
                        is_evicted.store(true, Ordering::Relaxed);
                        ActionResult {
                            ret: ServerReturn::Submit(Err(submit_failure(
                                SubmitError::SequenceMismatch { expected: 2 },
                            ))),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Once,
            1,
        ),
        // Status rule
        Rule::new(
            "status",
            Match::new("match status", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::Status { .. })
            }),
            Action::new(
                "status_action",
                |rc: &RoutedCall, states: &mut NetworkState| match rc.call {
                    ServerCall::Status { id, .. } => {
                        let status = states.confirm_one(rc.node_id.clone(), id);
                        ActionResult {
                            ret: ServerReturn::Status(TxConfirmResult::Ok(status)),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
        // Status batch rule
        Rule::new(
            "status batch",
            Match::new("match status batch", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::StatusBatch { .. })
            }),
            Action::new(
                "status_batch_action",
                |rc: &RoutedCall, states: &mut NetworkState| match &rc.call {
                    ServerCall::StatusBatch { ids, .. } => {
                        let results = states.confirm_batch(rc.node_id.clone(), ids);
                        ActionResult {
                            ret: ServerReturn::StatusBatch(TxConfirmResult::Ok(results)),
                        }
                    }
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
        // Sequence rule
        Rule::new(
            "sequence",
            Match::new("match sequence", |rc: &RoutedCall| {
                matches!(&rc.call, ServerCall::CurrentSequence { .. })
            }),
            Action::new(
                "sequence_action",
                |rc: &RoutedCall, states: &mut NetworkState| match rc.call {
                    ServerCall::CurrentSequence { .. } => ActionResult {
                        ret: ServerReturn::CurrentSequence(Ok(states.sequence(rc.node_id.clone()))),
                    },
                    _ => panic!("unexpected call"),
                },
            ),
            Cardinality::Any,
            0,
        ),
    ];

    let update = PeriodicCall::new(move |states: &mut NetworkState| {
        if !recovery_triggered_check.load(Ordering::Relaxed) {
            return;
        }
        let max_seq = states.max_submitted_sequence() - 1;
        states.update_confirmed_all(max_seq);
    });
    let (mut harness, manager_worker) =
        Harness::new(Duration::from_millis(1), 10, 1000, 2, rules, update);
    harness.start(manager_worker);
    let mut add_handles = VecDeque::new();
    for _i in 0..100 {
        let handle = harness.add_tx(vec![0, 0]).await;
        add_handles.push_back(handle);
    }
    let mut seq = 0;
    while let Some(handle) = add_handles.pop_front() {
        seq += 1;
        let (signed, submit, confirm) = handle;
        let signed = signed.await.expect("signed channel closed");
        assert!(
            signed.is_ok(),
            "signing failed for seq {}: {:?}",
            seq,
            signed
        );
        let submitted = submit.await.expect("submit channel closed");
        assert!(
            submitted.is_ok(),
            "submit failed for seq {}: {:?}",
            seq,
            submitted
        );
        let confirmed = confirm.await.expect("confirm channel closed");
        match confirmed {
            Ok(_) => {}
            other => panic!("confirm failed for seq {}: {:?}", seq, other),
        }
    }
    assert!(
        is_evicted_check.load(Ordering::Relaxed),
        "expected eviction to be triggered"
    );
    assert!(
        recovery_triggered_check2.load(Ordering::Relaxed),
        "expected recovery to be triggered"
    );
    let _results = harness.stop().await;
}
