use std::{collections::HashMap, path::PathBuf, time::Duration};

use crate::{
    runtime::{
        AgentSession, LogicalTarget, RuntimeConfig, RuntimeEvent, TargetExecutionConfig, TurnId,
        TurnResult,
    },
    terminal::TerminalId,
};

use super::{App, AppPolicy};

const PUMP_LIMIT: usize = 64;

/// Socket-free App owner used by the public embedded runtime facade.
pub(crate) struct EmbeddedApp {
    app: App,
    targets: HashMap<LogicalTarget, EmbeddedTarget>,
    startup_timeout: Duration,
    state_path: PathBuf,
    data_path: PathBuf,
    #[cfg(test)]
    test_terminal_factory: Option<Box<dyn FnMut(&mut App, PathBuf) -> std::io::Result<TerminalId>>>,
    #[cfg(test)]
    shutdown_error: Option<std::io::Error>,
}

struct EmbeddedTarget {
    terminal_id: TerminalId,
    execution: TargetExecutionConfig,
    _reporter: Option<super::embedded_report::PaneReportListener>,
    shutdown_failure: Option<String>,
}

pub(crate) enum EmbeddedTurn {
    Unavailable,
    Pending {
        terminal_id: TerminalId,
        request_id: String,
    },
    Settled(TurnResult),
}

impl EmbeddedApp {
    pub(crate) fn new(config: &RuntimeConfig) -> std::io::Result<Self> {
        std::fs::create_dir_all(&config.state_path)?;
        std::fs::create_dir_all(&config.data_path)?;
        let mut app_config = crate::config::Config::default();
        if let Some(command) = &config.terminal.command {
            app_config.terminal.default_shell = command.to_string_lossy().into_owned();
        }
        let app = App::try_new(
            &app_config,
            AppPolicy::EMBEDDED,
            None,
            tokio::sync::mpsc::unbounded_channel().1,
            crate::api::EventHub::default(),
        )?;
        Ok(Self {
            app,
            targets: HashMap::new(),
            startup_timeout: Duration::from_millis(config.opencode.startup_timeout_ms),
            state_path: config.state_path.clone(),
            data_path: config.data_path.clone(),
            #[cfg(test)]
            test_terminal_factory: None,
            #[cfg(test)]
            shutdown_error: None,
        })
    }

    #[cfg(test)]
    fn new_for_test(
        factory: impl FnMut(&mut App, PathBuf) -> std::io::Result<TerminalId> + 'static,
    ) -> Self {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            app: App::new(
                &crate::config::Config::default(),
                AppPolicy::TEST,
                None,
                api_rx,
                crate::api::EventHub::default(),
            ),
            targets: HashMap::new(),
            startup_timeout: Duration::from_secs(5),
            state_path: std::env::temp_dir().join("herdr-embedded-test-state"),
            data_path: std::env::temp_dir().join("herdr-embedded-test-data"),
            test_terminal_factory: Some(Box::new(factory)),
            shutdown_error: None,
        }
    }

    #[cfg(test)]
    pub(crate) fn ready_for_test() -> Self {
        Self::new_for_test(|app, _| {
            let workspace = crate::workspace::Workspace::test_new("embedded");
            let pane_id = workspace.tabs[0].root_pane;
            let terminal_id = workspace.terminal_id(pane_id).unwrap().clone();
            app.state.workspaces.push(workspace);
            app.state.ensure_test_terminals();
            let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
            terminal.set_detected_state(
                Some(crate::detect::Agent::OpenCode),
                crate::detect::AgentState::Idle,
            );
            terminal.persisted_agent_session = Some(crate::agent_resume::PersistedAgentSession {
                source: "herdr:opencode".into(),
                agent: "opencode".into(),
                session_ref: crate::agent_resume::AgentSessionRef::id("session").unwrap(),
            });
            let (runtime, _input) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
            app.terminal_runtimes.insert(terminal_id.clone(), runtime);
            Ok(terminal_id)
        })
    }

    #[cfg(test)]
    pub(crate) fn pending_for_test() -> (
        Self,
        std::sync::Arc<std::sync::Mutex<Vec<tokio::sync::mpsc::Receiver<bytes::Bytes>>>>,
    ) {
        let inputs = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let factory_inputs = inputs.clone();
        let app = Self::new_for_test(move |app, _| {
            let workspace = crate::workspace::Workspace::test_new("embedded");
            let pane_id = workspace.tabs[0].root_pane;
            let terminal_id = workspace.terminal_id(pane_id).unwrap().clone();
            app.state.workspaces.push(workspace);
            app.state.ensure_test_terminals();
            let (runtime, input) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
            app.terminal_runtimes.insert(terminal_id.clone(), runtime);
            factory_inputs.lock().unwrap().push(input);
            Ok(terminal_id)
        });
        (app, inputs)
    }

    #[cfg(test)]
    pub(crate) fn add_ready_target_for_test(
        &mut self,
        target: LogicalTarget,
        execution: TargetExecutionConfig,
    ) {
        let terminal_id = self
            .create_terminal(self.data_path.clone(), Vec::new())
            .unwrap();
        self.targets.insert(
            target,
            EmbeddedTarget {
                terminal_id,
                execution,
                _reporter: None,
                shutdown_failure: None,
            },
        );
    }

    #[cfg(test)]
    pub(crate) fn lose_target_for_test(&mut self, target: &LogicalTarget) {
        let terminal_id = self.targets[target].terminal_id.clone();
        self.app
            .terminal_runtimes
            .remove(&terminal_id)
            .unwrap()
            .shutdown();
        self.app.state.terminals.remove(&terminal_id);
    }

    #[cfg(test)]
    pub(crate) fn fail_shutdown_for_test(&mut self) {
        self.shutdown_error = Some(std::io::Error::other("test child remains alive"));
    }

    #[cfg(test)]
    pub(crate) fn target_terminal_for_test(&self, target: &LogicalTarget) -> TerminalId {
        self.targets[target].terminal_id.clone()
    }

    pub(crate) fn open(
        &mut self,
        target: LogicalTarget,
        execution: TargetExecutionConfig,
    ) -> std::io::Result<()> {
        if self.targets.contains_key(&target) {
            return Ok(());
        }
        let (reporter, report_env, pane) = super::embedded_report::PaneReportListener::start(
            self.app.event_tx.clone(),
            &self.state_path,
        )?;
        let terminal_id = self.create_terminal(execution.working_directory.clone(), report_env)?;
        let pane_id = self
            .app
            .state
            .workspaces
            .last()
            .and_then(|workspace| workspace.tabs.first())
            .map(|tab| tab.root_pane);
        if let Ok(mut reported_pane) = pane.lock() {
            *reported_pane = pane_id;
        } else {
            if let Err(error) = self.remove_created_terminal(&terminal_id) {
                return Err(std::io::Error::other(format!(
                    "embedded report listener is unavailable and terminal shutdown failed: {error}"
                )));
            }
            return Err(std::io::Error::other(
                "embedded report listener is unavailable",
            ));
        }
        if let Err(error) = self.app.start_embedded_opencode(
            &terminal_id,
            embedded_agent_name(self.targets.len()),
            &execution.opencode_arguments(),
            self.startup_timeout,
        ) {
            if let Err(cleanup_error) = self.remove_created_terminal(&terminal_id) {
                return Err(std::io::Error::other(format!(
                    "{error}; terminal shutdown failed: {cleanup_error}"
                )));
            }
            return Err(error);
        }
        self.targets.insert(
            target,
            EmbeddedTarget {
                terminal_id,
                execution,
                _reporter: Some(reporter),
                shutdown_failure: None,
            },
        );
        Ok(())
    }

    pub(crate) fn contains_target(&self, target: &LogicalTarget) -> bool {
        self.targets.contains_key(target)
    }

    pub(crate) fn execution(&self, target: &LogicalTarget) -> Option<&TargetExecutionConfig> {
        self.targets.get(target).map(|target| &target.execution)
    }

    pub(crate) fn shutdown_failure(&self, target: &LogicalTarget) -> Option<&str> {
        self.targets
            .get(target)
            .and_then(|target| target.shutdown_failure.as_deref())
    }

    pub(crate) fn rollback(&mut self, target: &LogicalTarget) {
        let _ = self.close(target);
    }

    pub(crate) fn close(&mut self, target: &LogicalTarget) -> std::io::Result<bool> {
        #[cfg(test)]
        if let Some(error) = self.shutdown_error.take() {
            if let Some(target) = self.targets.get_mut(target) {
                target.shutdown_failure = Some(error.to_string());
            }
            return Err(error);
        }
        let Some(terminal_id) = self
            .targets
            .get(target)
            .map(|target| target.terminal_id.clone())
        else {
            return Ok(false);
        };
        if let Err(error) = self.remove_created_terminal(&terminal_id) {
            if let Some(target) = self.targets.get_mut(target) {
                target.shutdown_failure = Some(error.to_string());
            }
            return Err(error);
        }
        self.targets.remove(target);
        Ok(true)
    }

    pub(crate) fn target_count(&self) -> usize {
        self.targets.len()
    }

    #[cfg(test)]
    pub(crate) fn resource_counts_for_test(&self) -> (usize, usize, usize, usize) {
        (
            self.app.state.workspaces.len(),
            self.app.state.terminals.len(),
            self.app.terminal_runtimes.len(),
            self.targets.len(),
        )
    }

    #[cfg(test)]
    pub(crate) fn paths_for_test(&self) -> (&std::path::Path, &std::path::Path) {
        (&self.state_path, &self.data_path)
    }

    #[cfg(test)]
    pub(crate) fn execution_for_test(
        &self,
        target: &LogicalTarget,
    ) -> Option<&TargetExecutionConfig> {
        self.execution(target)
    }

    #[cfg(test)]
    pub(crate) fn workspace_cwd_for_test(
        &self,
        target: &LogicalTarget,
    ) -> Option<&std::path::Path> {
        let terminal_id = &self.targets.get(target)?.terminal_id;
        self.app
            .state
            .terminals
            .get(terminal_id)
            .map(|terminal| terminal.cwd.as_path())
    }

    pub(crate) fn ready(&mut self, target: &LogicalTarget) -> bool {
        let Some(target) = self.targets.get(target) else {
            return false;
        };
        self.app
            .state
            .terminals
            .get_mut(&target.terminal_id)
            .is_some_and(|terminal| {
                terminal.reconcile_managed_agent_at(std::time::Instant::now(), false);
                terminal.effective_known_agent() == Some(crate::detect::Agent::OpenCode)
                    && terminal
                        .persisted_agent_session
                        .as_ref()
                        .is_some_and(|session| {
                            session.source == "herdr:opencode" && session.agent == "opencode"
                        })
            })
            && self
                .app
                .terminal_runtimes
                .get(&target.terminal_id)
                .is_some_and(|runtime| {
                    super::agents::runtime_hosts_agent(runtime, crate::detect::Agent::OpenCode)
                })
    }

    fn create_terminal(
        &mut self,
        working_directory: PathBuf,
        extra_env: Vec<(String, String)>,
    ) -> std::io::Result<TerminalId> {
        #[cfg(test)]
        if let Some(factory) = self.test_terminal_factory.as_mut() {
            return factory(&mut self.app, working_directory);
        }

        let workspace =
            self.app
                .create_workspace_with_launch_env(working_directory, true, extra_env)?;
        let pane_id = self.app.state.workspaces[workspace].tabs[0].root_pane;
        Ok(self.app.state.workspaces[workspace]
            .terminal_id(pane_id)
            .expect("new embedded workspace has a terminal")
            .clone())
    }

    fn remove_created_terminal(&mut self, terminal_id: &TerminalId) -> std::io::Result<()> {
        if let Some(runtime) = self.app.terminal_runtimes.get_mut(terminal_id) {
            runtime.shutdown_checked()?;
        }
        self.app.terminal_runtimes.remove(terminal_id);
        self.app.state.terminals.remove(terminal_id);
        if self.app.state.workspaces.last().is_some_and(|workspace| {
            workspace.tabs.len() == 1
                && workspace.tabs[0].terminal_id(workspace.tabs[0].root_pane) == Some(terminal_id)
        }) {
            self.app.state.workspaces.pop();
        }
        Ok(())
    }

    pub(crate) fn submit(
        &mut self,
        target: &LogicalTarget,
        turn_id: &TurnId,
        text: &str,
        deadline: std::time::Instant,
    ) -> EmbeddedTurn {
        if text.is_empty() {
            return EmbeddedTurn::Settled(TurnResult::Failed {
                turn_id: turn_id.clone(),
                message: "agent turn text must not be empty".into(),
            });
        }
        let Some(target) = self.targets.get(target) else {
            return EmbeddedTurn::Unavailable;
        };
        let terminal_id = target.terminal_id.clone();
        let available = self
            .app
            .state
            .terminals
            .get(&terminal_id)
            .is_some_and(|terminal| {
                terminal.effective_known_agent() == Some(crate::detect::Agent::OpenCode)
                    && terminal
                        .persisted_agent_session
                        .as_ref()
                        .is_some_and(|session| {
                            session.source == "herdr:opencode" && session.agent == "opencode"
                        })
            })
            && self
                .app
                .terminal_runtimes
                .get(&terminal_id)
                .is_some_and(|runtime| {
                    super::agents::runtime_hosts_agent(runtime, crate::detect::Agent::OpenCode)
                });
        if !available {
            return EmbeddedTurn::Unavailable;
        }
        match self
            .app
            .submit_embedded_agent_turn(
                &terminal_id,
                turn_id.as_str().to_string(),
                text,
                deadline,
            )
        {
            Some(result) => EmbeddedTurn::Settled(turn_result(turn_id.clone(), result)),
            None => EmbeddedTurn::Pending {
                terminal_id,
                request_id: turn_id.as_str().to_string(),
            },
        }
    }

    pub(crate) fn interrupt(&mut self, target: &LogicalTarget) -> bool {
        let Some(target) = self.targets.get(target) else {
            return false;
        };
        self.app.interrupt_embedded_agent(&target.terminal_id)
    }

    pub(crate) fn turn_result(
        &mut self,
        terminal_id: &TerminalId,
        turn_id: &TurnId,
    ) -> Option<TurnResult> {
        self.app
            .embedded_agent_turn_result(terminal_id, turn_id.as_str())
            .map(|result| turn_result(turn_id.clone(), result))
    }

    pub(crate) fn pump(
        &mut self,
        agents: &HashMap<AgentSession, LogicalTarget>,
    ) -> Vec<RuntimeEvent> {
        let _ = self.app.drain_internal_events_up_to(PUMP_LIMIT);
        let mut events = Vec::new();
        self.targets.retain(|target, mapped| {
            let alive = mapped.shutdown_failure.is_some()
                || (self.app.state.terminals.contains_key(&mapped.terminal_id)
                    && self
                        .app
                        .terminal_runtimes
                        .get(&mapped.terminal_id)
                        .is_some());
            if !alive {
                events.push(RuntimeEvent::TargetLost {
                    target: target.clone(),
                });
                for agent in agents
                    .iter()
                    .filter_map(|(agent, mapped_target)| (mapped_target == target).then_some(agent))
                {
                    events.push(RuntimeEvent::AgentLost {
                        agent: agent.clone(),
                    });
                }
            }
            alive
        });
        for mapped in self.targets.values() {
            if let Some(terminal) = self.app.state.terminals.get_mut(&mapped.terminal_id) {
                terminal.reconcile_agent_turn();
            }
        }
        events
    }

    pub(crate) fn shutdown(&mut self) -> std::io::Result<usize> {
        #[cfg(test)]
        if let Some(error) = self.shutdown_error.take() {
            return Err(error);
        }
        let terminal_ids = self
            .app
            .terminal_runtimes
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for terminal_id in &terminal_ids {
            self.app
                .terminal_runtimes
                .get_mut(terminal_id)
                .expect("runtime key was collected from the registry")
                .shutdown_checked()?;
        }
        let drained = terminal_ids.len();
        for terminal_id in terminal_ids {
            self.app.terminal_runtimes.remove(&terminal_id);
        }
        self.targets.clear();
        Ok(drained)
    }
}

fn embedded_agent_name(index: usize) -> String {
    format!("embedded-{index}")
}

fn turn_result(turn_id: TurnId, result: crate::api::schema::AgentTurnResult) -> TurnResult {
    match result.status {
        crate::api::schema::AgentTurnStatus::Completed => TurnResult::Completed {
            turn_id,
            text: result.text,
        },
        crate::api::schema::AgentTurnStatus::Interrupted => TurnResult::Interrupted { turn_id },
        crate::api::schema::AgentTurnStatus::TimedOut => TurnResult::TimedOut { turn_id },
        crate::api::schema::AgentTurnStatus::Failed => TurnResult::Failed {
            turn_id,
            message: "agent turn failed".into(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::{
        io::Write,
        path::Path,
        sync::{Arc, Mutex},
        time::Duration,
    };

    fn test_embedded_app() -> (
        EmbeddedApp,
        Arc<Mutex<Vec<tokio::sync::mpsc::Receiver<Bytes>>>>,
    ) {
        let inputs = Arc::new(Mutex::new(Vec::new()));
        let factory_inputs = inputs.clone();
        let app = EmbeddedApp::new_for_test(move |app, working_directory| {
            let workspace = crate::workspace::Workspace::test_new("embedded");
            let pane_id = workspace.tabs[0].root_pane;
            let terminal_id = workspace.terminal_id(pane_id).unwrap().clone();
            app.state.workspaces.push(workspace);
            app.state.ensure_test_terminals();
            let terminal = app.state.terminals.get_mut(&terminal_id).unwrap();
            terminal.cwd = working_directory;
            terminal.set_detected_state(
                Some(crate::detect::Agent::OpenCode),
                crate::detect::AgentState::Idle,
            );
            terminal.persisted_agent_session = Some(crate::agent_resume::PersistedAgentSession {
                source: "herdr:opencode".into(),
                agent: "opencode".into(),
                session_ref: crate::agent_resume::AgentSessionRef::id("session").unwrap(),
            });
            let (runtime, input) = crate::terminal::TerminalRuntime::test_with_channel(80, 24);
            app.terminal_runtimes.insert(terminal_id.clone(), runtime);
            factory_inputs.lock().unwrap().push(input);
            Ok(terminal_id)
        });
        (app, inputs)
    }

    fn target() -> LogicalTarget {
        LogicalTarget::conversation("conversation").unwrap()
    }

    fn execution(
        working_directory: impl Into<PathBuf>,
        agent: &str,
        model: &str,
        session: crate::runtime::SessionSelection,
    ) -> crate::runtime::TargetExecutionConfig {
        crate::runtime::TargetExecutionConfig {
            working_directory: working_directory.into(),
            agent: agent.into(),
            model: model.into(),
            session,
        }
    }

    fn test_execution() -> crate::runtime::TargetExecutionConfig {
        execution(
            "/work",
            "agent",
            "model",
            crate::runtime::SessionSelection::New,
        )
    }

    #[tokio::test]
    async fn submit_queues_prompt_bytes_then_enter() {
        let (mut app, inputs) = test_embedded_app();
        let target = target();
        let turn_id = TurnId::new("turn").unwrap();
        app.add_ready_target_for_test(target.clone(), test_execution());

        assert!(matches!(
            app.submit(
                &target,
                &turn_id,
                "prompt",
                std::time::Instant::now() + Duration::from_secs(30),
            ),
            EmbeddedTurn::Pending { .. }
        ));
        let mut input = inputs.lock().unwrap().remove(0);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), input.recv())
                .await
                .unwrap(),
            Some(Bytes::from_static(b"prompt"))
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), input.recv())
                .await
                .unwrap(),
            Some(Bytes::from_static(b"\r"))
        );
    }

    #[tokio::test]
    async fn interrupted_turn_settles_once() {
        let (mut app, _) = test_embedded_app();
        let target = target();
        let turn_id = TurnId::new("turn").unwrap();
        app.add_ready_target_for_test(target.clone(), test_execution());
        let EmbeddedTurn::Pending { terminal_id, .. } = app.submit(
            &target,
            &turn_id,
            "prompt",
            std::time::Instant::now() + Duration::from_secs(30),
        )
        else {
            panic!("turn should be pending");
        };

        assert!(app.interrupt(&target));
        app.app
            .state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .set_detected_state(
                Some(crate::detect::Agent::OpenCode),
                crate::detect::AgentState::Idle,
            );
        assert!(app.pump(&HashMap::new()).is_empty());
        let result = app.turn_result(&terminal_id, &turn_id);
        assert!(matches!(result, Some(TurnResult::Interrupted { .. })));
        assert_eq!(app.turn_result(&terminal_id, &turn_id), result);
    }

    #[tokio::test]
    async fn authenticated_private_completion_settles_an_embedded_turn() {
        let mut app = EmbeddedApp::ready_for_test();
        let target = target();
        let turn_id = TurnId::new("turn").unwrap();
        app.add_ready_target_for_test(target.clone(), test_execution());
        let terminal_id = app.target_terminal_for_test(&target);
        let pane_id = app.app.state.workspaces.last().unwrap().tabs[0].root_pane;
        let (listener, environment, pane) = crate::app::embedded_report::PaneReportListener::start(
            app.app.event_tx.clone(),
            &app.state_path,
        )
        .unwrap();
        *pane.lock().unwrap() = Some(pane_id);

        assert!(matches!(
            app.submit(
                &target,
                &turn_id,
                "prompt",
                std::time::Instant::now() + Duration::from_secs(30),
            ),
            EmbeddedTurn::Pending { .. }
        ));
        let mut stream = crate::ipc::connect_local_stream(Path::new(&environment[0].1)).unwrap();
        writeln!(
            stream,
            "{}",
            serde_json::json!({
                "token": environment[1].1,
                "source": "herdr:opencode",
                "agent": "opencode",
                "state": "idle",
                "seq": 1,
                "agent_session_id": "session",
                "completion": {"id": "completion", "text": "done"}
            })
        )
        .unwrap();

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while app.turn_result(&terminal_id, &turn_id).is_none()
            && std::time::Instant::now() < deadline
        {
            app.pump(&HashMap::new());
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            app.turn_result(&terminal_id, &turn_id),
            Some(TurnResult::Completed {
                turn_id,
                text: "done".into(),
            })
        );
        drop(listener);
    }

    #[tokio::test]
    async fn target_is_not_ready_until_opencode_reports_a_session() {
        let mut app = EmbeddedApp::ready_for_test();
        let target = target();
        app.add_ready_target_for_test(target.clone(), test_execution());
        let terminal_id = app.target_terminal_for_test(&target);
        app.app
            .state
            .terminals
            .get_mut(&terminal_id)
            .unwrap()
            .persisted_agent_session = None;

        assert!(!app.ready(&target));
    }

    #[test]
    fn config_paths_are_created_and_retained_by_the_embedded_app() {
        let unique = format!("herdr-embedded-paths-{}", std::process::id());
        let state_path = std::env::temp_dir().join(&unique).join("state");
        let data_path = std::env::temp_dir().join(&unique).join("data");
        let config = RuntimeConfig {
            state_path: state_path.clone(),
            data_path: data_path.clone(),
            ..RuntimeConfig::default()
        };

        let app = EmbeddedApp::new(&config).unwrap();
        assert_eq!(
            app.paths_for_test(),
            (state_path.as_path(), data_path.as_path())
        );
        assert!(state_path.is_dir());
        assert!(data_path.is_dir());
        drop(app);
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join(unique));
    }

    #[tokio::test]
    async fn open_rolls_back_terminal_and_workspace_when_opencode_start_rejects_arguments() {
        let (mut app, _) = test_embedded_app();
        let execution = execution(
            "/work",
            "agent",
            "model",
            crate::runtime::SessionSelection::New,
        );
        let mut execution = execution;
        execution.agent = "invalid\u{0}".into();

        assert!(app.open(target(), execution).is_err());
        assert!(app.targets.is_empty());
        assert!(app.app.state.workspaces.is_empty());
        assert!(app.app.state.terminals.is_empty());
        assert_eq!(app.app.terminal_runtimes.len(), 0);
    }

    #[tokio::test]
    async fn open_keeps_each_target_execution_snapshot_and_launch_directory() {
        let (mut app, _) = test_embedded_app();
        let first = LogicalTarget::conversation("first").unwrap();
        let second = LogicalTarget::thread("second", "thread").unwrap();
        let first_execution = execution(
            "/work/first",
            "reviewer",
            "openai/gpt-5",
            crate::runtime::SessionSelection::New,
        );
        let second_execution = execution(
            "/work/second",
            "builder",
            "anthropic/claude-sonnet-4",
            crate::runtime::SessionSelection::Resume("session-42".into()),
        );

        app.open(first.clone(), first_execution.clone()).unwrap();
        app.open(second.clone(), second_execution.clone()).unwrap();

        assert_eq!(app.execution_for_test(&first), Some(&first_execution));
        assert_eq!(app.execution_for_test(&second), Some(&second_execution));
        assert_eq!(
            app.workspace_cwd_for_test(&first),
            Some(Path::new("/work/first"))
        );
        assert_eq!(
            app.workspace_cwd_for_test(&second),
            Some(Path::new("/work/second"))
        );
        assert_eq!(
            first_execution.opencode_arguments(),
            vec!["--agent", "reviewer", "--model", "openai/gpt-5"]
        );
        assert_eq!(
            second_execution.opencode_arguments(),
            vec![
                "--agent",
                "builder",
                "--model",
                "anthropic/claude-sonnet-4",
                "--session",
                "session-42"
            ]
        );
    }

    #[tokio::test]
    async fn lost_target_mapping_is_removed_and_open_recreates_it() {
        let (mut app, _) = test_embedded_app();
        let target = target();
        app.add_ready_target_for_test(target.clone(), test_execution());
        let old_terminal_id = app.targets[&target].terminal_id.clone();
        app.app
            .terminal_runtimes
            .remove(&old_terminal_id)
            .unwrap()
            .shutdown();
        app.app.state.terminals.remove(&old_terminal_id);

        assert_eq!(
            app.pump(&HashMap::new()),
            vec![RuntimeEvent::TargetLost {
                target: target.clone()
            }]
        );
        assert!(!app.targets.contains_key(&target));
        app.add_ready_target_for_test(target.clone(), test_execution());
        assert_ne!(app.targets[&target].terminal_id, old_terminal_id);
    }

    #[tokio::test]
    async fn shutdown_drains_all_owned_terminal_runtimes() {
        let (mut app, _) = test_embedded_app();
        app.add_ready_target_for_test(
            LogicalTarget::conversation("one").unwrap(),
            test_execution(),
        );
        app.add_ready_target_for_test(
            LogicalTarget::conversation("two").unwrap(),
            test_execution(),
        );

        assert_eq!(app.shutdown().unwrap(), 2);
        assert_eq!(app.app.terminal_runtimes.len(), 0);
        assert!(app.targets.is_empty());
    }

    #[tokio::test]
    async fn failed_shutdown_retains_owned_runtimes_for_a_checked_retry() {
        let (mut app, _) = test_embedded_app();
        app.add_ready_target_for_test(target(), test_execution());
        app.fail_shutdown_for_test();

        assert!(app.shutdown().is_err());
        assert_eq!(app.app.terminal_runtimes.len(), 1);
        assert_eq!(app.targets.len(), 1);
        assert_eq!(app.shutdown().unwrap(), 1);
        assert_eq!(app.app.terminal_runtimes.len(), 0);
        assert!(app.targets.is_empty());
    }
}
