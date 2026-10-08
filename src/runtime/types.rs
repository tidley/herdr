use std::{
    fmt,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::broadcast;

/// Cooperative cancellation owned by the embedding host.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Minimal terminal configuration supplied by an embedding host.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TerminalConfig {
    pub command: Option<PathBuf>,
}

/// Selects whether OpenCode starts a new session or resumes an existing one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionSelection {
    New,
    Resume(String),
}

/// OpenCode execution settings for one logical target.
///
/// Each call to [`crate::RuntimeHandle::open_agent`] supplies one immutable
/// launch snapshot. It is never shared with other logical targets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TargetExecutionConfig {
    /// Directory in which the target's terminal and OpenCode process start.
    pub working_directory: PathBuf,
    /// OpenCode agent passed with `--agent`.
    pub agent: String,
    /// OpenCode model passed with `--model`.
    pub model: String,
    /// Whether to start a new OpenCode session or resume the supplied session.
    pub session: SessionSelection,
}

impl TargetExecutionConfig {
    pub(crate) fn opencode_arguments(&self) -> Vec<String> {
        let mut arguments = vec![
            "--agent".into(),
            self.agent.clone(),
            "--model".into(),
            self.model.clone(),
        ];
        if let SessionSelection::Resume(session) = &self.session {
            arguments.extend(["--session".into(), session.clone()]);
        }
        arguments
    }
}

/// Runtime-wide OpenCode lifecycle settings supplied by an embedding host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenCodeConfig {
    /// Time to wait for Herdr to observe the started OpenCode session.
    pub startup_timeout_ms: u64,
}

impl Default for OpenCodeConfig {
    fn default() -> Self {
        Self {
            startup_timeout_ms: 30_000,
        }
    }
}

/// Configuration for an embedded runtime.
///
/// The host owns all process-wide concerns. This type does not parse CLI input,
/// install signal handlers, create a Tokio runtime, or define a process exit policy.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Directory owned by the embedding worker for runtime state.
    pub state_path: PathBuf,
    /// Directory owned by the embedding worker for runtime data.
    pub data_path: PathBuf,
    pub cancellation: CancellationToken,
    pub terminal: TerminalConfig,
    pub opencode: OpenCodeConfig,
    /// Maximum pending events per subscriber. The event stream is lossy when full.
    pub event_buffer: usize,
    /// Maximum live embedded OpenCode sessions. New sessions are rejected at the limit.
    pub max_active_sessions: usize,
    /// Maximum settled turn results retained per live session during the retry window.
    /// New turn IDs are rejected at the limit while those results remain retained.
    pub max_retained_turns_per_session: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            state_path: std::env::temp_dir().join("herdr-state"),
            data_path: std::env::temp_dir().join("herdr-data"),
            cancellation: CancellationToken::new(),
            terminal: TerminalConfig::default(),
            opencode: OpenCodeConfig::default(),
            event_buffer: 64,
            max_active_sessions: 16,
            max_retained_turns_per_session: 1_024,
        }
    }
}

/// A host-visible target for an agent session.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum LogicalTarget {
    Conversation(String),
    Thread {
        conversation_id: String,
        thread_id: String,
    },
}

impl LogicalTarget {
    pub fn conversation(value: impl Into<String>) -> Result<Self, RuntimeError> {
        Self::new(value, Self::Conversation)
    }

    pub fn thread(
        conversation_id: impl Into<String>,
        thread_id: impl Into<String>,
    ) -> Result<Self, RuntimeError> {
        let conversation_id = conversation_id.into();
        let thread_id = thread_id.into();
        if conversation_id.trim().is_empty() || thread_id.trim().is_empty() {
            return Err(RuntimeError::InvalidTarget);
        }
        Ok(Self::Thread {
            conversation_id,
            thread_id,
        })
    }

    fn new(
        value: impl Into<String>,
        constructor: impl FnOnce(String) -> Self,
    ) -> Result<Self, RuntimeError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(RuntimeError::InvalidTarget);
        }
        Ok(constructor(value))
    }
}

/// Caller-assigned identity for a submitted agent turn.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TurnId(String);

impl TurnId {
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(RuntimeError::InvalidTurnId);
        }
        Ok(Self(value))
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

/// A logical agent session opened by a [`crate::RuntimeHandle`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AgentSession {
    runtime_id: u64,
    id: u64,
    target: LogicalTarget,
}

impl AgentSession {
    pub(crate) fn new(runtime_id: u64, id: u64, target: LogicalTarget) -> Self {
        Self {
            runtime_id,
            id,
            target,
        }
    }

    pub fn target(&self) -> &LogicalTarget {
        &self.target
    }

    pub(crate) fn runtime_id(&self) -> u64 {
        self.runtime_id
    }
}

/// The terminal-facing outcome of a turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnResult {
    Completed { turn_id: TurnId, text: String },
    Interrupted { turn_id: TurnId },
    Failed { turn_id: TurnId, message: String },
    Unavailable { turn_id: TurnId },
    TimedOut { turn_id: TurnId },
}

impl TurnResult {
    pub fn turn_id(&self) -> &TurnId {
        match self {
            Self::Completed { turn_id, .. }
            | Self::Interrupted { turn_id }
            | Self::Failed { turn_id, .. }
            | Self::Unavailable { turn_id }
            | Self::TimedOut { turn_id } => turn_id,
        }
    }
}

/// Events emitted by the embedded runtime contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeEvent {
    AgentOpened(AgentSession),
    TargetLost { target: LogicalTarget },
    AgentLost { agent: AgentSession },
    TurnFinished { turn_id: TurnId, result: TurnResult },
    Shutdown,
}

/// Bounded, lossy event subscription. Lagged subscribers receive `RecvError::Lagged`.
pub type RuntimeSubscription = broadcast::Receiver<RuntimeEvent>;

/// Errors returned by the embedded runtime contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuntimeError {
    Cancelled,
    Closed,
    CrossRuntimeSession,
    InvalidEventBuffer,
    InvalidRetentionLimit,
    InvalidStartupTimeout,
    InvalidTarget,
    InvalidTurnId,
    HostRuntimeRequired,
    LocalRuntimeRequired,
    StartupFailed(String),
    StatePoisoned,
    TurnOwnedByDifferentAgent,
    TurnPending,
    TurnRetryExpired,
    TurnRetentionLimit,
    Unavailable,
    UnknownAgent,
    UnknownTurn,
    ShutdownFailed(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Cancelled => "runtime cancellation requested",
            Self::Closed => "runtime is shut down",
            Self::CrossRuntimeSession => "agent session belongs to another runtime",
            Self::InvalidEventBuffer => "event buffer must be greater than zero",
            Self::InvalidRetentionLimit => "runtime retention limits must be greater than zero",
            Self::InvalidStartupTimeout => {
                "OpenCode startup timeout must be greater than 3000ms and at most 300000ms"
            }
            Self::InvalidTarget => "logical target must not be empty",
            Self::InvalidTurnId => "turn ID must not be empty",
            Self::HostRuntimeRequired => "an entered Tokio runtime is required",
            Self::LocalRuntimeRequired => {
                "the embedded runtime must run through LocalRuntime on a Tokio LocalSet"
            }
            Self::StartupFailed(message) => return formatter.write_str(message),
            Self::ShutdownFailed(message) => return formatter.write_str(message),
            Self::StatePoisoned => "runtime state is unavailable",
            Self::TurnOwnedByDifferentAgent => "turn belongs to another agent session",
            Self::TurnPending => "turn is still pending",
            Self::TurnRetryExpired => "turn result retry window has expired",
            Self::TurnRetentionLimit => "retained turn limit reached for this agent session",
            Self::Unavailable => "embedded target is unavailable",
            Self::UnknownAgent => "agent session is not open in this runtime",
            Self::UnknownTurn => "turn ID has not been submitted",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RuntimeError {}
