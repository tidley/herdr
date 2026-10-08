mod standalone;
mod types;

pub(crate) use standalone::StandaloneRuntimeAdapter;

pub use types::{
    AgentSession, CancellationToken, LogicalTarget, OpenCodeConfig, RuntimeConfig, RuntimeError,
    RuntimeEvent, RuntimeSubscription, SessionSelection, TargetExecutionConfig, TerminalConfig,
    TurnId, TurnResult,
};

use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{broadcast, Notify};

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);
/// Exact retries return their settled result for this long; later retries fail
/// without submitting the request again.
pub const TURN_RESULT_RETRY_WINDOW: Duration = Duration::from_secs(5 * 60);

tokio::task_local! {
    static LOCAL_RUNTIME_HOST: ();
}

/// Host helper for the non-`Send` embedded runtime.
///
/// Run every [`RuntimeHandle`] operation through [`LocalRuntime::run_until`]
/// on a Tokio [`tokio::task::LocalSet`]. The returned handle deliberately does
/// not implement `Send` or `Sync`.
#[derive(Default)]
pub struct LocalRuntime {
    _not_send_or_sync: PhantomData<Rc<()>>,
}

impl LocalRuntime {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn run_until<F: Future>(
        &self,
        local_set: &tokio::task::LocalSet,
        future: F,
    ) -> F::Output {
        local_set
            .run_until(LOCAL_RUNTIME_HOST.scope((), future))
            .await
    }
}

/// Embedded runtime facade with no ownership of host process resources.
#[derive(Clone)]
pub struct RuntimeHandle {
    runtime_id: u64,
    config: RuntimeConfig,
    state: Arc<Mutex<RuntimeState>>,
    events: broadcast::Sender<RuntimeEvent>,
    turn_changed: Arc<Notify>,
    _not_send_or_sync: PhantomData<Rc<()>>,
}

impl std::fmt::Debug for RuntimeHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeHandle")
            .field("runtime_id", &self.runtime_id)
            .finish_non_exhaustive()
    }
}

struct RuntimeState {
    next_agent_id: u64,
    agents: HashMap<AgentSession, LogicalTarget>,
    turns: HashMap<(AgentSession, TurnId), TurnRecord>,
    expired_turns: HashMap<(AgentSession, TurnId), Instant>,
    embedded: crate::app::embedded::EmbeddedApp,
    shutdown: bool,
}

impl RuntimeState {
    fn new(embedded: crate::app::embedded::EmbeddedApp) -> Self {
        Self {
            next_agent_id: 0,
            agents: HashMap::new(),
            turns: HashMap::new(),
            expired_turns: HashMap::new(),
            embedded,
            shutdown: false,
        }
    }
}

#[derive(Clone, Debug)]
struct TurnRecord {
    agent: AgentSession,
    result: Option<TurnResult>,
    terminal_id: Option<crate::terminal::TerminalId>,
    interrupt_requested: bool,
    settled_at: Option<Instant>,
}

impl RuntimeHandle {
    pub async fn start(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        tokio::runtime::Handle::try_current().map_err(|_| RuntimeError::HostRuntimeRequired)?;
        if config.cancellation.is_cancelled() {
            return Err(RuntimeError::Cancelled);
        }
        if config.event_buffer == 0 {
            return Err(RuntimeError::InvalidEventBuffer);
        }
        if config.max_active_sessions == 0 || config.max_retained_turns_per_session == 0 {
            return Err(RuntimeError::InvalidRetentionLimit);
        }
        if config.opencode.startup_timeout_ms
            <= crate::app::AGENT_START_SETTLE_DELAY.as_millis() as u64
            || config.opencode.startup_timeout_ms
                > crate::app::MAX_AGENT_START_TIMEOUT.as_millis() as u64
        {
            return Err(RuntimeError::InvalidStartupTimeout);
        }
        Self::ensure_local_host()?;
        let (events, _) = broadcast::channel(config.event_buffer);
        let embedded = crate::app::embedded::EmbeddedApp::new(&config)
            .map_err(|err| RuntimeError::StartupFailed(err.to_string()))?;
        Ok(Self {
            runtime_id: NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed),
            config,
            state: Arc::new(Mutex::new(RuntimeState::new(embedded))),
            events,
            turn_changed: Arc::new(Notify::new()),
            _not_send_or_sync: PhantomData,
        })
    }

    pub async fn open_agent(
        &self,
        target: LogicalTarget,
        execution: TargetExecutionConfig,
    ) -> Result<AgentSession, RuntimeError> {
        Self::ensure_local_host()?;
        self.pump()?;
        let events = {
            let mut state = self.lock_state()?;
            self.ensure_available(&state)?;
            if let Some(error) = state.embedded.shutdown_failure(&target) {
                return Err(RuntimeError::ShutdownFailed(error.into()));
            }
            if state.embedded.execution(&target) == Some(&execution) {
                if let Some(agent) = state
                    .agents
                    .keys()
                    .find(|agent| agent.target() == &target)
                    .cloned()
                {
                    return Ok(agent);
                }
                Vec::new()
            } else if state.embedded.contains_target(&target) {
                state.embedded.interrupt(&target);
                let events = Self::release_target(&mut state, &target)?;
                state
                    .embedded
                    .open(target.clone(), execution)
                    .map_err(|err| RuntimeError::StartupFailed(err.to_string()))?;
                events
            } else {
                if state.embedded.target_count() >= self.config.max_active_sessions {
                    return Err(RuntimeError::Unavailable);
                }
                state
                    .embedded
                    .open(target.clone(), execution)
                    .map_err(|err| RuntimeError::StartupFailed(err.to_string()))?;
                Vec::new()
            }
        };
        for event in events {
            self.publish(event);
        }
        let deadline =
            Instant::now() + Duration::from_millis(self.config.opencode.startup_timeout_ms);
        loop {
            self.pump()?;
            let mut state = self.lock_state()?;
            if let Err(error) = self.ensure_available(&state) {
                state.embedded.rollback(&target);
                return Err(error);
            }
            if state.embedded.ready(&target) {
                let agent = AgentSession::new(self.runtime_id, state.next_agent_id, target.clone());
                state.next_agent_id += 1;
                state.agents.insert(agent.clone(), target);
                drop(state);
                self.publish(RuntimeEvent::AgentOpened(agent.clone()));
                return Ok(agent);
            }
            if Instant::now() >= deadline {
                state.embedded.rollback(&target);
                return Err(RuntimeError::StartupFailed(
                    "OpenCode session did not become ready before the configured startup timeout"
                        .into(),
                ));
            }
            drop(state);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Closes all resources owned by `target` and removes its agent mappings.
    ///
    /// This method is idempotent. When the target is already absent, it returns
    /// `Ok(())` without emitting events.
    pub async fn close_target(&self, target: &LogicalTarget) -> Result<(), RuntimeError> {
        Self::ensure_local_host()?;
        let events = {
            let mut state = self.lock_state()?;
            if !state.embedded.contains_target(target) {
                return Ok(());
            }
            Self::release_target(&mut state, target)?
        };
        for event in events {
            self.publish(event);
        }
        self.turn_changed.notify_waiters();
        Ok(())
    }

    pub async fn submit_turn(
        &self,
        agent: &AgentSession,
        turn_id: TurnId,
        text: impl AsRef<str>,
        timeout: Duration,
    ) -> Result<TurnResult, RuntimeError> {
        Self::ensure_local_host()?;
        let now = Instant::now();
        let deadline = now.checked_add(timeout).unwrap_or(now);
        self.pump()?;
        let (result, finished) = {
            let mut state = self.lock_state()?;
            self.ensure_available(&state)?;
            self.ensure_agent(&state, agent)?;
            let key = (agent.clone(), turn_id.clone());
            Self::expire_turn_results(&mut state, now);
            if state.expired_turns.contains_key(&key) {
                return Err(RuntimeError::TurnRetryExpired);
            }
            if let Some(record) = state.turns.get(&key) {
                (record.result.clone(), None)
            } else {
                let retained_turns = state
                    .turns
                    .values()
                    .filter(|record| record.agent == *agent)
                    .count();
                if retained_turns >= self.config.max_retained_turns_per_session {
                    return Err(RuntimeError::TurnRetentionLimit);
                }
                match state
                    .embedded
                    .submit(agent.target(), &turn_id, text.as_ref(), deadline)
                {
                    crate::app::embedded::EmbeddedTurn::Unavailable => {
                        let result = TurnResult::Unavailable {
                            turn_id: turn_id.clone(),
                        };
                        state.turns.insert(
                            key.clone(),
                            TurnRecord {
                                agent: agent.clone(),
                                result: Some(result.clone()),
                                terminal_id: None,
                                interrupt_requested: false,
                                settled_at: Some(now),
                            },
                        );
                        (Some(result.clone()), Some(result))
                    }
                    crate::app::embedded::EmbeddedTurn::Settled(result) => {
                        state.turns.insert(
                            key.clone(),
                            TurnRecord {
                                agent: agent.clone(),
                                result: Some(result.clone()),
                                terminal_id: None,
                                interrupt_requested: false,
                                settled_at: Some(now),
                            },
                        );
                        (Some(result.clone()), Some(result))
                    }
                    crate::app::embedded::EmbeddedTurn::Pending { terminal_id, .. } => {
                        state.turns.insert(
                            key.clone(),
                            TurnRecord {
                                agent: agent.clone(),
                                result: None,
                                terminal_id: Some(terminal_id),
                                interrupt_requested: false,
                                settled_at: None,
                            },
                        );
                        (None, None)
                    }
                }
            }
        };
        let result = match result {
            Some(result) => result,
            None => loop {
                tokio::select! {
                    _ = self.turn_changed.notified() => {}
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
                self.pump()?;
                let state = self.lock_state()?;
                if let Some(result) = state
                    .turns
                    .get(&(agent.clone(), turn_id.clone()))
                    .and_then(|record| record.result.clone())
                {
                    break result;
                }
                self.ensure_available(&state)?;
            },
        };
        if matches!(result, TurnResult::TimedOut { .. }) {
            let events = {
                let mut state = self.lock_state()?;
                state.embedded.interrupt(agent.target());
                Self::retire_timed_out_target(&mut state, agent.target())
            };
            for event in events {
                self.publish(event);
            }
        }
        if let Some(finished) = finished {
            self.publish(RuntimeEvent::TurnFinished {
                turn_id: finished.turn_id().clone(),
                result: finished,
            });
        }
        Ok(result)
    }

    pub async fn interrupt_turn(
        &self,
        agent: &AgentSession,
        turn_id: &TurnId,
    ) -> Result<TurnResult, RuntimeError> {
        Self::ensure_local_host()?;
        self.pump()?;
        {
            let mut state = self.lock_state()?;
            self.ensure_available(&state)?;
            self.ensure_agent(&state, agent)?;
            let key = (agent.clone(), turn_id.clone());
            let record = state.turns.get(&key).ok_or(RuntimeError::UnknownTurn)?;
            let retained = record.result.clone();
            if let Some(result) = retained {
                return Ok(result);
            }
            if !state.embedded.interrupt(agent.target()) {
                return Err(RuntimeError::Unavailable);
            }
            state
                .turns
                .get_mut(&key)
                .expect("turn was validated")
                .interrupt_requested = true;
        }
        loop {
            tokio::select! {
                _ = self.turn_changed.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
            self.pump()?;
            let state = self.lock_state()?;
            if let Some(result) = state
                .turns
                .get(&(agent.clone(), turn_id.clone()))
                .and_then(|record| record.result.clone())
            {
                return Ok(result);
            }
            self.ensure_available(&state)?;
        }
    }

    /// Subscribes to a bounded, lossy event stream.
    ///
    /// Slow consumers receive `tokio::sync::broadcast::error::RecvError::Lagged`
    /// and must resynchronize from their own retained state.
    pub async fn subscribe(&self) -> Result<RuntimeSubscription, RuntimeError> {
        Self::ensure_local_host()?;
        self.pump()?;
        let state = self.lock_state()?;
        self.ensure_available(&state)?;
        Ok(self.events.subscribe())
    }

    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        Self::ensure_local_host()?;
        let should_publish = {
            let mut state = self.lock_state()?;
            if state.shutdown {
                false
            } else {
                state
                    .embedded
                    .shutdown()
                    .map_err(|err| RuntimeError::ShutdownFailed(err.to_string()))?;
                state.shutdown = true;
                true
            }
        };
        if should_publish {
            self.publish(RuntimeEvent::Shutdown);
        }
        Ok(())
    }

    fn lock_state(&self) -> Result<std::sync::MutexGuard<'_, RuntimeState>, RuntimeError> {
        self.state.lock().map_err(|_| RuntimeError::StatePoisoned)
    }

    fn ensure_local_host() -> Result<(), RuntimeError> {
        #[cfg(test)]
        {
            return Ok(());
        }
        #[cfg(not(test))]
        LOCAL_RUNTIME_HOST
            .try_with(|_| ())
            .map_err(|_| RuntimeError::LocalRuntimeRequired)
    }

    fn ensure_available(&self, state: &RuntimeState) -> Result<(), RuntimeError> {
        if state.shutdown {
            Err(RuntimeError::Closed)
        } else if self.config.cancellation.is_cancelled() {
            Err(RuntimeError::Cancelled)
        } else {
            Ok(())
        }
    }

    fn ensure_agent(&self, state: &RuntimeState, agent: &AgentSession) -> Result<(), RuntimeError> {
        if agent.runtime_id() != self.runtime_id {
            return Err(RuntimeError::CrossRuntimeSession);
        }
        state
            .agents
            .contains_key(agent)
            .then_some(())
            .ok_or(RuntimeError::UnknownAgent)
    }

    fn release_target(
        state: &mut RuntimeState,
        target: &LogicalTarget,
    ) -> Result<Vec<RuntimeEvent>, RuntimeError> {
        state
            .embedded
            .close(target)
            .map_err(|err| RuntimeError::ShutdownFailed(err.to_string()))?;
        let agents = state
            .agents
            .iter()
            .filter_map(|(agent, mapped_target)| (mapped_target == target).then_some(agent.clone()))
            .collect::<Vec<_>>();
        state
            .agents
            .retain(|_, mapped_target| mapped_target != target);
        let mut events = vec![RuntimeEvent::TargetLost {
            target: target.clone(),
        }];
        events.extend(
            agents
                .into_iter()
                .map(|agent| RuntimeEvent::AgentLost { agent }),
        );
        for ((_, turn_id), record) in state.turns.iter_mut() {
            if record.result.is_none() && record.agent.target() == target {
                let result = TurnResult::Unavailable {
                    turn_id: turn_id.clone(),
                };
                record.result = Some(result.clone());
                record.terminal_id = None;
                record.settled_at = Some(Instant::now());
                events.push(RuntimeEvent::TurnFinished {
                    turn_id: turn_id.clone(),
                    result,
                });
            }
        }
        Ok(events)
    }

    fn retire_timed_out_target(
        state: &mut RuntimeState,
        target: &LogicalTarget,
    ) -> Vec<RuntimeEvent> {
        match Self::release_target(state, target) {
            Ok(events) => events,
            Err(_) => {
                let agents = state
                    .agents
                    .iter()
                    .filter_map(|(agent, mapped_target)| {
                        (mapped_target == target).then_some(agent.clone())
                    })
                    .collect::<Vec<_>>();
                state
                    .agents
                    .retain(|_, mapped_target| mapped_target != target);
                agents
                    .into_iter()
                    .map(|agent| RuntimeEvent::AgentLost { agent })
                    .collect()
            }
        }
    }

    fn publish(&self, event: RuntimeEvent) {
        let _ = self.events.send(event);
    }

    fn expire_turn_results(state: &mut RuntimeState, now: Instant) {
        state.expired_turns.retain(|_, expires_at| *expires_at > now);
        let expired = state
            .turns
            .iter()
            .filter_map(|(key, record)| {
                record
                    .settled_at
                    .is_some_and(|settled_at| {
                        now.saturating_duration_since(settled_at) >= TURN_RESULT_RETRY_WINDOW
                    })
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for key in expired {
            state.turns.remove(&key);
            state
                .expired_turns
                .insert(key, now + TURN_RESULT_RETRY_WINDOW);
        }
    }

    fn pump(&self) -> Result<(), RuntimeError> {
        let (mut events, settled) = {
            let mut state = self.lock_state()?;
            if state.shutdown {
                return Ok(());
            }
            Self::expire_turn_results(&mut state, Instant::now());
            let agents = state.agents.clone();
            let events = state.embedded.pump(&agents);
            let lost_targets = events
                .iter()
                .filter_map(|event| match event {
                    RuntimeEvent::TargetLost { target } => Some(target.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let mut settled = Vec::new();
            for target in lost_targets {
                state
                    .agents
                    .retain(|_, mapped_target| mapped_target != &target);
                for ((_, turn_id), record) in state.turns.iter_mut() {
                    if record.result.is_none() && record.agent.target() == &target {
                        let result = TurnResult::Unavailable {
                            turn_id: turn_id.clone(),
                        };
                        record.result = Some(result.clone());
                        record.terminal_id = None;
                        record.settled_at = Some(Instant::now());
                        settled.push((turn_id.clone(), result));
                    }
                }
            }
            let pending = state
                .turns
                .iter()
                .filter_map(|((agent, turn_id), record)| {
                    record.result.is_none().then_some((
                        agent.clone(),
                        turn_id.clone(),
                        record.terminal_id.clone(),
                    ))
                })
                .collect::<Vec<_>>();
            for (agent, turn_id, terminal_id) in pending {
                if let Some(result) =
                    terminal_id.and_then(|id| state.embedded.turn_result(&id, &turn_id))
                {
                    if let Some(record) = state.turns.get_mut(&(agent, turn_id.clone())) {
                        let result =
                            resolve_turn_result(&turn_id, record.interrupt_requested, result);
                        record.result = Some(result.clone());
                        record.settled_at = Some(Instant::now());
                        settled.push((turn_id, result));
                    }
                }
            }
            (events, settled)
        };
        events.extend(
            settled
                .into_iter()
                .map(|(turn_id, result)| RuntimeEvent::TurnFinished { turn_id, result }),
        );
        for event in events {
            self.publish(event);
        }
        self.turn_changed.notify_waiters();
        Ok(())
    }
}

fn resolve_turn_result(turn_id: &TurnId, interrupted: bool, result: TurnResult) -> TurnResult {
    if interrupted {
        TurnResult::Interrupted {
            turn_id: turn_id.clone(),
        }
    } else {
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> RuntimeConfig {
        RuntimeConfig {
            state_path: std::env::temp_dir().join("herdr-runtime-test-state"),
            data_path: std::env::temp_dir().join("herdr-runtime-test-data"),
            cancellation: CancellationToken::new(),
            terminal: TerminalConfig::default(),
            opencode: OpenCodeConfig::default(),
            event_buffer: 4,
            max_active_sessions: 4,
            max_retained_turns_per_session: 8,
        }
    }

    fn execution() -> TargetExecutionConfig {
        TargetExecutionConfig {
            working_directory: "/work".into(),
            agent: "agent".into(),
            model: "model".into(),
            session: SessionSelection::New,
        }
    }

    fn runtime_with_ready_agent() -> (RuntimeHandle, AgentSession, TurnId) {
        let config = config();
        let (events, _) = broadcast::channel(config.event_buffer);
        let target = LogicalTarget::conversation("conversation").unwrap();
        let agent = AgentSession::new(1, 1, target.clone());
        let mut embedded = crate::app::embedded::EmbeddedApp::ready_for_test();
        embedded.add_ready_target_for_test(target.clone(), execution());
        let runtime = RuntimeHandle {
            runtime_id: 1,
            config,
            state: Arc::new(Mutex::new(RuntimeState {
                next_agent_id: 2,
                agents: HashMap::from([(agent.clone(), target)]),
                turns: HashMap::new(),
                expired_turns: HashMap::new(),
                embedded,
                shutdown: false,
            })),
            events,
            turn_changed: Arc::new(Notify::new()),
            _not_send_or_sync: PhantomData,
        };
        (runtime, agent, TurnId::new("turn").unwrap())
    }

    fn runtime_with_pending_target(
        config: RuntimeConfig,
    ) -> (
        RuntimeHandle,
        Arc<Mutex<Vec<tokio::sync::mpsc::Receiver<bytes::Bytes>>>>,
    ) {
        let (events, _) = broadcast::channel(config.event_buffer);
        let (embedded, inputs) = crate::app::embedded::EmbeddedApp::pending_for_test();
        (
            RuntimeHandle {
                runtime_id: 1,
                config,
                state: Arc::new(Mutex::new(RuntimeState::new(embedded))),
                events,
                turn_changed: Arc::new(Notify::new()),
                _not_send_or_sync: PhantomData,
            },
            inputs,
        )
    }

    fn assert_no_embedded_resources(runtime: &RuntimeHandle) {
        assert_eq!(
            runtime
                .lock_state()
                .unwrap()
                .embedded
                .resource_counts_for_test(),
            (0, 0, 0, 0)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_open_rolls_back_its_terminal_and_workspace() {
        tokio::task::LocalSet::new()
            .run_until(async {
                let config = config();
                let cancellation = config.cancellation.clone();
                let (runtime, _inputs) = runtime_with_pending_target(config);
                let cancelling_runtime = runtime.clone();
                tokio::task::spawn_local(async move {
                    let deadline = Instant::now() + Duration::from_secs(1);
                    while cancelling_runtime
                        .lock_state()
                        .unwrap()
                        .embedded
                        .target_count()
                        == 0
                    {
                        assert!(Instant::now() < deadline, "opening did not create a target");
                        tokio::task::yield_now().await;
                    }
                    cancellation.cancel();
                });

                assert_eq!(
                    runtime
                        .open_agent(
                            LogicalTarget::conversation("conversation").unwrap(),
                            TargetExecutionConfig {
                                working_directory: "/work".into(),
                                agent: "agent".into(),
                                model: "model".into(),
                                session: SessionSelection::New,
                            },
                        )
                        .await
                        .unwrap_err(),
                    RuntimeError::Cancelled
                );
                assert_no_embedded_resources(&runtime);
            })
            .await;
    }

    #[tokio::test]
    async fn timed_out_open_rolls_back_its_terminal_and_workspace() {
        let mut config = config();
        config.opencode.startup_timeout_ms = 3_001;
        let (runtime, _inputs) = runtime_with_pending_target(config);

        assert!(matches!(
            runtime
                .open_agent(
                    LogicalTarget::conversation("conversation").unwrap(),
                    TargetExecutionConfig {
                        working_directory: "/work".into(),
                        agent: "agent".into(),
                        model: "model".into(),
                        session: SessionSelection::New,
                    },
                )
                .await,
            Err(RuntimeError::StartupFailed(_))
        ));
        assert_no_embedded_resources(&runtime);
    }

    #[tokio::test]
    async fn opening_an_existing_target_reuses_matching_execution_or_replaces_changed_execution() {
        let config = config();
        let (runtime, _inputs) = runtime_with_pending_target(config);
        let target = LogicalTarget::conversation("conversation").unwrap();
        let initial = TargetExecutionConfig {
            working_directory: "/work/initial".into(),
            agent: "initial-agent".into(),
            model: "initial-model".into(),
            session: SessionSelection::New,
        };
        let changed = TargetExecutionConfig {
            working_directory: "/work/changed".into(),
            agent: "changed-agent".into(),
            model: "changed-model".into(),
            session: SessionSelection::Resume("previous".into()),
        };

        {
            let mut state = runtime.lock_state().unwrap();
            state
                .embedded
                .add_ready_target_for_test(target.clone(), initial.clone());
            let agent = AgentSession::new(runtime.runtime_id, 1, target.clone());
            state.agents.insert(agent.clone(), target.clone());
            let terminal_id = state.embedded.target_terminal_for_test(&target);
            state.turns.insert(
                (agent.clone(), TurnId::new("pending").unwrap()),
                TurnRecord {
                    agent,
                    result: None,
                    terminal_id: Some(terminal_id),
                    interrupt_requested: false,
                    settled_at: None,
                },
            );
        }

        let reused = runtime.open_agent(target.clone(), initial).await.unwrap();
        assert_eq!(
            reused,
            AgentSession::new(runtime.runtime_id, 1, target.clone())
        );

        let replacement = runtime.open_agent(target.clone(), changed.clone());
        tokio::pin!(replacement);
        tokio::select! {
            _ = &mut replacement => panic!("replacement should await readiness"),
            _ = tokio::time::sleep(Duration::from_millis(20)) => {}
        }
        let state = runtime.lock_state().unwrap();
        assert_eq!(state.embedded.execution_for_test(&target), Some(&changed));
        assert!(state.agents.is_empty());
        assert_eq!(
            state.turns[&(
                AgentSession::new(runtime.runtime_id, 1, target),
                TurnId::new("pending").unwrap()
            )]
                .result,
            Some(TurnResult::Unavailable {
                turn_id: TurnId::new("pending").unwrap()
            })
        );
    }

    #[tokio::test]
    async fn empty_turn_text_settles_as_failed() {
        let (runtime, agent, turn_id) = runtime_with_ready_agent();

        assert_eq!(
            runtime
                .submit_turn(&agent, turn_id.clone(), "", Duration::from_secs(30))
                .await
                .unwrap(),
            TurnResult::Failed {
                turn_id,
                message: "agent turn text must not be empty".into(),
            }
        );
    }

    #[tokio::test]
    async fn embedded_turn_times_out_when_opencode_never_reports_completion() {
        let (runtime, agent, turn_id) = runtime_with_ready_agent();

        assert_eq!(
            runtime
                .submit_turn(&agent, turn_id.clone(), "prompt", Duration::ZERO)
                .await
                .unwrap(),
            TurnResult::TimedOut { turn_id }
        );
        assert_no_embedded_resources(&runtime);
        assert_eq!(
            runtime
                .submit_turn(
                    &agent,
                    TurnId::new("next-turn").unwrap(),
                    "next prompt",
                    Duration::from_secs(30),
                )
                .await,
            Err(RuntimeError::UnknownAgent)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn close_target_removes_resources_and_is_idempotent_before_returning() {
        let host = LocalRuntime::new();
        let local_set = tokio::task::LocalSet::new();
        host.run_until(&local_set, async {
            let (runtime, agent, _) = runtime_with_ready_agent();
            let target = agent.target().clone();

            runtime.close_target(&target).await.unwrap();
            assert_no_embedded_resources(&runtime);
            assert_eq!(
                runtime
                    .submit_turn(
                        &agent,
                        TurnId::new("turn").unwrap(),
                        "prompt",
                        Duration::from_secs(30),
                    )
                    .await,
                Err(RuntimeError::UnknownAgent)
            );
            runtime.close_target(&target).await.unwrap();
            assert_no_embedded_resources(&runtime);
        })
        .await;
    }

    #[tokio::test]
    async fn close_target_preserves_mappings_when_child_shutdown_fails() {
        let (runtime, agent, _) = runtime_with_ready_agent();
        let target = agent.target().clone();
        runtime
            .lock_state()
            .unwrap()
            .embedded
            .fail_shutdown_for_test();

        assert_eq!(
            runtime.close_target(&target).await.unwrap_err(),
            RuntimeError::ShutdownFailed("test child remains alive".into())
        );
        let state = runtime.lock_state().unwrap();
        assert!(state.embedded.contains_target(&target));
        assert!(state.agents.contains_key(&agent));
        drop(state);
        runtime.close_target(&target).await.unwrap();
        assert_no_embedded_resources(&runtime);
    }

    #[tokio::test]
    async fn replacing_changed_execution_preserves_mappings_when_child_shutdown_fails() {
        let (runtime, agent, _) = runtime_with_ready_agent();
        let target = agent.target().clone();
        let original_execution = runtime
            .lock_state()
            .unwrap()
            .embedded
            .execution_for_test(&target)
            .unwrap()
            .clone();
        let changed_execution = TargetExecutionConfig {
            working_directory: "/work/changed".into(),
            agent: "changed-agent".into(),
            model: "changed-model".into(),
            session: SessionSelection::Resume("previous".into()),
        };
        runtime
            .lock_state()
            .unwrap()
            .embedded
            .fail_shutdown_for_test();

        assert_eq!(
            runtime
                .open_agent(target.clone(), changed_execution)
                .await
                .unwrap_err(),
            RuntimeError::ShutdownFailed("test child remains alive".into())
        );
        let state = runtime.lock_state().unwrap();
        assert_eq!(
            state.embedded.execution_for_test(&target),
            Some(&original_execution)
        );
        assert!(state.agents.contains_key(&agent));
        drop(state);
        assert_eq!(
            runtime
                .open_agent(
                    target,
                    TargetExecutionConfig {
                        working_directory: "/work/another-change".into(),
                        agent: "another-agent".into(),
                        model: "another-model".into(),
                        session: SessionSelection::New,
                    },
                )
                .await
                .unwrap_err(),
            RuntimeError::ShutdownFailed("test child remains alive".into())
        );
    }

    #[tokio::test]
    async fn target_loss_settles_each_pending_owned_turn_once() {
        let (runtime, agent, turn_id) = runtime_with_ready_agent();
        let mut events = runtime.events.subscribe();
        let terminal_id = {
            let state = runtime.lock_state().unwrap();
            state.embedded.target_terminal_for_test(agent.target())
        };
        {
            let mut state = runtime.lock_state().unwrap();
            state.turns.insert(
                (agent.clone(), turn_id.clone()),
                TurnRecord {
                    agent: agent.clone(),
                    result: None,
                    terminal_id: Some(terminal_id),
                    interrupt_requested: false,
                    settled_at: None,
                },
            );
            state.embedded.lose_target_for_test(agent.target());
        }

        runtime.pump().unwrap();
        let state = runtime.lock_state().unwrap();
        assert_eq!(
            state.turns[&(agent.clone(), turn_id.clone())].result,
            Some(TurnResult::Unavailable {
                turn_id: turn_id.clone(),
            })
        );
        drop(state);
        assert!(matches!(
            events.try_recv().unwrap(),
            RuntimeEvent::TargetLost { .. }
        ));
        assert!(matches!(
            events.try_recv().unwrap(),
            RuntimeEvent::AgentLost { .. }
        ));
        assert_eq!(
            events.try_recv().unwrap(),
            RuntimeEvent::TurnFinished {
                turn_id: turn_id.clone(),
                result: TurnResult::Unavailable { turn_id },
            }
        );
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn retrying_a_settled_turn_does_not_publish_a_second_completion() {
        let (runtime, agent, turn_id) = runtime_with_ready_agent();
        let mut events = runtime.events.subscribe();
        let result = TurnResult::Failed {
            turn_id: turn_id.clone(),
            message: "failed".into(),
        };
        runtime.lock_state().unwrap().turns.insert(
            (agent.clone(), turn_id.clone()),
            TurnRecord {
                agent: agent.clone(),
                result: Some(result.clone()),
                terminal_id: None,
                interrupt_requested: false,
                settled_at: Some(Instant::now()),
            },
        );

        assert_eq!(
            runtime
                .submit_turn(&agent, turn_id, "retry", Duration::from_secs(30))
                .await
                .unwrap(),
            result
        );
        assert!(matches!(
            events.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[tokio::test]
    async fn retained_turn_limit_rejects_new_ids_without_evicting_retries() {
        let (runtime, agent, _) = runtime_with_ready_agent();
        let retained_id = TurnId::new("retained").unwrap();
        let retained = TurnResult::Failed {
            turn_id: retained_id.clone(),
            message: "failed".into(),
        };
        {
            let mut state = runtime.lock_state().unwrap();
            for index in 0..runtime.config.max_retained_turns_per_session {
                let turn_id = if index == 0 {
                    retained_id.clone()
                } else {
                    TurnId::new(format!("turn-{index}")).unwrap()
                };
                state.turns.insert(
                    (agent.clone(), turn_id.clone()),
                    TurnRecord {
                        agent: agent.clone(),
                        result: Some(if turn_id == retained_id {
                            retained.clone()
                        } else {
                            TurnResult::Unavailable { turn_id }
                        }),
                        terminal_id: None,
                        interrupt_requested: false,
                        settled_at: Some(Instant::now()),
                    },
                );
            }
        }

        assert_eq!(
            runtime
                .submit_turn(&agent, retained_id, "retry", Duration::from_secs(30))
                .await
                .unwrap(),
            retained
        );
        assert_eq!(
            runtime
                .submit_turn(
                    &agent,
                    TurnId::new("new").unwrap(),
                    "new",
                    Duration::from_secs(30),
                )
                .await
                .unwrap_err(),
            RuntimeError::TurnRetentionLimit
        );
    }

    #[tokio::test]
    async fn expired_turn_retry_is_rejected_without_resubmitting_it() {
        let (runtime, agent, turn_id) = runtime_with_ready_agent();
        let terminal_id = {
            let state = runtime.lock_state().unwrap();
            state.embedded.target_terminal_for_test(agent.target())
        };
        {
            let mut state = runtime.lock_state().unwrap();
            state.turns.insert(
                (agent.clone(), turn_id.clone()),
                TurnRecord {
                    agent: agent.clone(),
                    result: Some(TurnResult::Failed {
                        turn_id: turn_id.clone(),
                        message: "failed".into(),
                    }),
                    terminal_id: Some(terminal_id),
                    interrupt_requested: false,
                    settled_at: Some(Instant::now() - TURN_RESULT_RETRY_WINDOW),
                },
            );
        }

        assert_eq!(
            runtime
                .submit_turn(&agent, turn_id, "must not be sent", Duration::from_secs(30))
                .await,
            Err(RuntimeError::TurnRetryExpired)
        );
    }

    #[tokio::test]
    async fn retained_turn_ids_are_scoped_to_the_agent_session() {
        let (runtime, agent, turn_id) = runtime_with_ready_agent();
        let other = AgentSession::new(1, 2, LogicalTarget::conversation("other").unwrap());
        let other_result = TurnResult::Failed {
            turn_id: turn_id.clone(),
            message: "other".into(),
        };
        {
            let mut state = runtime.lock_state().unwrap();
            state.agents.insert(other.clone(), other.target().clone());
            state.turns.insert(
                (other.clone(), turn_id.clone()),
                TurnRecord {
                    agent: other.clone(),
                    result: Some(other_result.clone()),
                    terminal_id: None,
                    interrupt_requested: false,
                    settled_at: Some(Instant::now()),
                },
            );
        }

        assert_eq!(
            runtime
                .submit_turn(&other, turn_id, "retry", Duration::from_secs(30))
                .await
                .unwrap(),
            other_result
        );
        assert!(runtime
            .submit_turn(
                &agent,
                TurnId::new("unique").unwrap(),
                "",
                Duration::from_secs(30),
            )
            .await
            .is_ok());
    }

    #[test]
    fn successful_interrupt_wins_over_a_late_completion() {
        let turn_id = TurnId::new("turn").unwrap();
        assert_eq!(
            resolve_turn_result(
                &turn_id,
                true,
                TurnResult::Completed {
                    turn_id: turn_id.clone(),
                    text: "done".into(),
                },
            ),
            TurnResult::Interrupted { turn_id }
        );
    }

    #[tokio::test]
    async fn shutdown_reports_a_child_termination_failure() {
        let (runtime, _, _) = runtime_with_ready_agent();
        runtime
            .lock_state()
            .unwrap()
            .embedded
            .fail_shutdown_for_test();

        assert_eq!(
            runtime.shutdown().await.unwrap_err(),
            RuntimeError::ShutdownFailed("test child remains alive".into())
        );
    }

    #[tokio::test]
    async fn interrupting_a_settled_turn_returns_its_retained_result() {
        let config = config();
        let (events, mut receiver) = broadcast::channel(config.event_buffer);
        let target = LogicalTarget::conversation("conversation").unwrap();
        let agent = AgentSession::new(1, 1, target.clone());
        let turn_id = TurnId::new("turn").unwrap();
        let result = TurnResult::Completed {
            turn_id: turn_id.clone(),
            text: "done".into(),
        };
        let embedded = crate::app::embedded::EmbeddedApp::new(&config).unwrap();
        let runtime = RuntimeHandle {
            runtime_id: 1,
            config,
            state: Arc::new(Mutex::new(RuntimeState {
                next_agent_id: 2,
                agents: HashMap::from([(agent.clone(), target)]),
                turns: HashMap::from([(
                    (agent.clone(), turn_id.clone()),
                    TurnRecord {
                        agent: agent.clone(),
                        result: Some(result.clone()),
                        terminal_id: None,
                        interrupt_requested: false,
                        settled_at: Some(Instant::now()),
                    },
                )]),
                expired_turns: HashMap::new(),
                embedded,
                shutdown: false,
            })),
            events,
            turn_changed: Arc::new(Notify::new()),
            _not_send_or_sync: PhantomData,
        };

        assert_eq!(
            runtime.interrupt_turn(&agent, &turn_id).await.unwrap(),
            result
        );
        assert!(matches!(
            receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }
}
