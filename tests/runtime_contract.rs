use herdr::{
    CancellationToken, LocalRuntime, LogicalTarget, OpenCodeConfig, RuntimeConfig, RuntimeError,
    RuntimeHandle, SessionSelection, TargetExecutionConfig, TerminalConfig, TurnId, TurnResult,
};
use std::path::PathBuf;

fn config() -> RuntimeConfig {
    RuntimeConfig {
        state_path: PathBuf::from("/tmp/herdr-runtime-contract-state"),
        data_path: PathBuf::from("/tmp/herdr-runtime-contract-data"),
        cancellation: CancellationToken::new(),
        terminal: TerminalConfig::default(),
        opencode: OpenCodeConfig {
            startup_timeout_ms: 5_000,
        },
        event_buffer: 4,
        max_active_sessions: 4,
        max_retained_turns_per_session: 8,
    }
}

fn execution() -> TargetExecutionConfig {
    TargetExecutionConfig {
        working_directory: PathBuf::from("/tmp/herdr-runtime-contract-work"),
        executable: PathBuf::from("opencode"),
        agent: "agent".into(),
        model: "provider/model".into(),
        session: SessionSelection::New,
    }
}

#[test]
fn thread_target_requires_conversation_and_thread_ids() {
    assert_eq!(
        LogicalTarget::thread("conversation", "thread").unwrap(),
        LogicalTarget::Thread {
            conversation_id: "conversation".into(),
            thread_id: "thread".into(),
        }
    );
    assert_eq!(
        LogicalTarget::thread("", "thread").unwrap_err(),
        RuntimeError::InvalidTarget
    );
    assert_eq!(
        LogicalTarget::thread("conversation", "").unwrap_err(),
        RuntimeError::InvalidTarget
    );
}

#[test]
fn completed_turn_exposes_opencode_completion_text() {
    let result = TurnResult::Completed {
        turn_id: TurnId::new("turn").unwrap(),
        text: "Finished the requested work.".into(),
    };

    assert!(matches!(
        result,
        TurnResult::Completed { text, .. } if text == "Finished the requested work."
    ));
}

#[tokio::test]
async fn cancelled_config_prevents_runtime_start() {
    let config = config();
    config.cancellation.cancel();

    assert_eq!(
        RuntimeHandle::start(config).await.unwrap_err(),
        RuntimeError::Cancelled
    );
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_requires_a_local_runtime_host() {
    assert_eq!(
        RuntimeHandle::start(config()).await.unwrap_err(),
        RuntimeError::LocalRuntimeRequired
    );

    let host = LocalRuntime::new();
    let local_set = tokio::task::LocalSet::new();
    let runtime = host
        .run_until(&local_set, RuntimeHandle::start(config()))
        .await
        .unwrap();
    assert_eq!(
        runtime
            .open_agent(
                LogicalTarget::conversation("conversation").unwrap(),
                execution()
            )
            .await
            .unwrap_err(),
        RuntimeError::LocalRuntimeRequired
    );
}

#[tokio::test]
async fn invalid_retention_and_startup_limits_are_rejected() {
    let mut invalid_retention = config();
    invalid_retention.max_retained_turns_per_session = 0;
    assert_eq!(
        RuntimeHandle::start(invalid_retention).await.unwrap_err(),
        RuntimeError::InvalidRetentionLimit
    );

    let mut invalid_startup = config();
    invalid_startup.opencode.startup_timeout_ms = 3_000;
    assert_eq!(
        RuntimeHandle::start(invalid_startup).await.unwrap_err(),
        RuntimeError::InvalidStartupTimeout
    );
}
