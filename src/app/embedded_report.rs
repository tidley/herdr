use std::{
    io::{self, Read, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use interprocess::local_socket::{
    traits::{Listener as _, Stream as _},
    ListenerNonblockingMode,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::{agent_resume, detect::AgentState, events::AppEvent, layout::PaneId};

const MAX_REPORT_BYTES: usize = 64 * 1024;
const REPORT_DEADLINE: Duration = Duration::from_millis(500);
const SOCKET_MODE: u32 = 0o600;
static NEXT_REPORT_SOCKET: AtomicU64 = AtomicU64::new(1);

pub(crate) struct PaneReportListener {
    path: PathBuf,
    identity: crate::ipc::SocketFileIdentity,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

#[derive(Deserialize)]
struct Report {
    token: String,
    source: String,
    agent: String,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    seq: Option<u64>,
    #[serde(default)]
    agent_session_id: Option<String>,
    #[serde(default)]
    agent_session_path: Option<String>,
    #[serde(default)]
    session_start_source: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    completion: Option<crate::api::schema::AgentCompletion>,
}

impl PaneReportListener {
    pub(crate) fn start(
        event_tx: tokio::sync::mpsc::Sender<AppEvent>,
        state_path: &std::path::Path,
    ) -> io::Result<(Self, Vec<(String, String)>, Arc<Mutex<Option<PaneId>>>)> {
        std::fs::create_dir_all(state_path)?;
        let path = report_socket_path(state_path);
        crate::ipc::prepare_socket_path(&path, |_| {
            "embedded report socket is already in use".into()
        })?;
        let listener = crate::ipc::bind_private_local_listener(&path)?;
        crate::ipc::restrict_socket_permissions(&path, SOCKET_MODE)?;
        let identity = crate::ipc::socket_file_identity(&path)?;
        listener.set_nonblocking(ListenerNonblockingMode::Both)?;
        let token = report_token();
        let stop = Arc::new(AtomicBool::new(false));
        let pane = Arc::new(Mutex::new(None));
        let thread_stop = stop.clone();
        let thread_pane = pane.clone();
        let thread_token = token.clone();
        let thread = std::thread::Builder::new()
            .name("herdr-embedded-report".into())
            .spawn(move || run(listener, thread_stop, thread_pane, thread_token, event_tx))?;
        Ok((
            Self {
                path: path.clone(),
                identity,
                stop,
                thread: Some(thread),
            },
            vec![
                (
                    "HERDR_PANE_REPORT_SOCKET".into(),
                    path.to_string_lossy().into_owned(),
                ),
                ("HERDR_PANE_REPORT_TOKEN".into(), token),
            ],
            pane,
        ))
    }
}

impl Drop for PaneReportListener {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = crate::ipc::connect_local_stream(&self.path);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = crate::ipc::remove_socket_file_if_owned(&self.path, &self.identity);
    }
}

fn run(
    listener: crate::ipc::LocalListener,
    stop: Arc<AtomicBool>,
    pane: Arc<Mutex<Option<PaneId>>>,
    token: String,
    event_tx: tokio::sync::mpsc::Sender<AppEvent>,
) {
    while !stop.load(Ordering::Acquire) {
        match listener.accept() {
            Ok(mut stream) => {
                let report = read_report(&mut stream);
                let pane_id = pane.lock().ok().and_then(|pane| *pane);
                if let (Ok(Some(report)), Some(pane_id)) = (report, pane_id) {
                    if report.token == token
                        && report.source == "herdr:opencode"
                        && report.agent == "opencode"
                    {
                        if dispatch_report(event_tx.clone(), pane_id, report) {
                            let _ = stream.write_all(b"{}\n");
                        }
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(5))
            }
            Err(_) => break,
        }
    }
}

fn read_report(stream: &mut crate::ipc::LocalStream) -> io::Result<Option<Report>> {
    stream.set_nonblocking(true)?;
    let deadline = Instant::now() + REPORT_DEADLINE;
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                bytes.extend_from_slice(&buffer[..read]);
                if bytes.len() > MAX_REPORT_BYTES
                    || bytes.iter().filter(|&&byte| byte == b'\n').count() != 1
                {
                    return Ok(None);
                }
                if bytes.ends_with(b"\n") {
                    break;
                }
            }
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error),
        }
    }
    if bytes.len() > MAX_REPORT_BYTES
        || !bytes.ends_with(b"\n")
        || bytes[..bytes.len() - 1].contains(&b'\n')
    {
        return Ok(None);
    }
    Ok(serde_json::from_slice(&bytes[..bytes.len() - 1]).ok())
}

fn dispatch_report(
    event_tx: tokio::sync::mpsc::Sender<AppEvent>,
    pane_id: PaneId,
    report: Report,
) -> bool {
    if report
        .state
        .as_deref()
        .is_some_and(|state| parse_state(state).is_none())
    {
        return false;
    }
    let session_ref = agent_resume::session_ref_from_report(
        &report.source,
        &report.agent,
        report.agent_session_id,
        report.agent_session_path,
    );
    let event = match report.state.as_deref().and_then(parse_state) {
        Some(state) => AppEvent::HookStateReported {
            pane_id,
            source: report.source,
            agent_label: report.agent,
            state,
            message: report.message,
            completion: report.completion,
            seq: report.seq,
            session_ref,
        },
        None => AppEvent::AgentSessionReported {
            pane_id,
            source: report.source,
            agent_label: report.agent,
            seq: report.seq,
            session_ref,
            session_start_source: agent_resume::normalize_session_start_source(
                report.session_start_source,
            ),
        },
    };
    event_tx.try_send(event).is_ok()
}

fn parse_state(state: &str) -> Option<AgentState> {
    match state {
        "idle" => Some(AgentState::Idle),
        "working" => Some(AgentState::Working),
        "blocked" => Some(AgentState::Blocked),
        _ => None,
    }
}

fn report_socket_path(state_path: &std::path::Path) -> PathBuf {
    state_path.join(format!(
        "herdr-embedded-report-{}-{}.sock",
        std::process::id(),
        NEXT_REPORT_SOCKET.fetch_add(1, Ordering::Relaxed),
    ))
}

fn report_token() -> String {
    #[cfg(unix)]
    {
        let mut bytes = [0_u8; 32];
        if std::fs::File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut bytes))
            .is_ok()
        {
            return bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        }
    }
    let nonce = NEXT_REPORT_SOCKET.fetch_add(1, Ordering::Relaxed);
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut hash = Sha256::new();
    hash.update(std::process::id().to_le_bytes());
    hash.update(nonce.to_le_bytes());
    hash.update(time.to_le_bytes());
    format!("{:x}", hash.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn accepts_only_one_bounded_json_line() {
        assert_eq!(parse_state("working"), Some(AgentState::Working));
        assert_eq!(parse_state("unknown"), None);
        assert_ne!(report_token(), report_token());
    }

    #[test]
    fn authenticated_socket_report_forwards_completion() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
        let state_path =
            std::env::temp_dir().join(format!("herdr-embedded-report-test-{}", std::process::id()));
        let (listener, environment, pane) =
            PaneReportListener::start(event_tx, &state_path).unwrap();
        assert!(listener.path.starts_with(&state_path));
        let token = environment[1].1.clone();
        let pane_id = crate::layout::PaneId::alloc();
        *pane.lock().unwrap() = Some(pane_id);

        let mut stream = crate::ipc::connect_local_stream(&listener.path).unwrap();
        writeln!(
            stream,
            "{}",
            serde_json::json!({
                "token": token,
                "source": "herdr:opencode",
                "agent": "opencode",
                "state": "idle",
                "seq": 1,
                "agent_session_id": "session",
                "completion": {"id": "completion", "text": "done"}
            })
        )
        .unwrap();

        let event = event_rx
            .blocking_recv()
            .expect("authenticated report event");
        assert!(matches!(
            event,
            AppEvent::HookStateReported {
                pane_id: reported_pane,
                completion: Some(crate::api::schema::AgentCompletion { id, text }),
                ..
            } if reported_pane == pane_id && id == "completion" && text == "done"
        ));
        drop(listener);
        let _ = std::fs::remove_dir(&state_path);
    }

    #[test]
    fn authenticated_socket_report_forwards_startup_session_source() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
        let state_path = std::env::temp_dir().join(format!(
            "herdr-embedded-report-session-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (listener, environment, pane) =
            PaneReportListener::start(event_tx, &state_path).unwrap();
        let token = environment[1].1.clone();
        let pane_id = crate::layout::PaneId::alloc();
        *pane.lock().unwrap() = Some(pane_id);

        let mut stream = crate::ipc::connect_local_stream(&listener.path).unwrap();
        writeln!(
            stream,
            "{}",
            serde_json::json!({
                "token": token,
                "source": "herdr:opencode",
                "agent": "opencode",
                "agent_session_id": "session",
                "session_start_source": "startup"
            })
        )
        .unwrap();

        assert!(matches!(
            event_rx.blocking_recv(),
            Some(AppEvent::AgentSessionReported {
                pane_id: reported_pane,
                session_start_source: Some(source),
                ..
            }) if reported_pane == pane_id && source == "startup"
        ));
        let mut acknowledgement = Vec::new();
        stream.read_to_end(&mut acknowledgement).unwrap();
        assert_eq!(acknowledgement, b"{}\n");
        drop(listener);
        let _ = std::fs::remove_dir(&state_path);
    }

    #[test]
    fn socket_report_rejects_wrong_token() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
        let state_path = std::env::temp_dir().join(format!(
            "herdr-embedded-report-token-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (listener, _, pane) = PaneReportListener::start(event_tx, &state_path).unwrap();
        *pane.lock().unwrap() = Some(crate::layout::PaneId::alloc());

        let mut stream = crate::ipc::connect_local_stream(&listener.path).unwrap();
        writeln!(
            stream,
            "{}",
            serde_json::json!({
                "token": "wrong",
                "source": "herdr:opencode",
                "agent": "opencode",
                "agent_session_id": "session"
            })
        )
        .unwrap();
        drop(stream);
        std::thread::sleep(Duration::from_millis(25));
        assert!(event_rx.try_recv().is_err());
        drop(listener);
        let _ = std::fs::remove_dir(&state_path);
    }

    #[test]
    fn socket_report_rejects_malformed_payload() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
        let state_path = std::env::temp_dir().join(format!(
            "herdr-embedded-report-malformed-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (listener, _, pane) = PaneReportListener::start(event_tx, &state_path).unwrap();
        *pane.lock().unwrap() = Some(crate::layout::PaneId::alloc());

        let mut stream = crate::ipc::connect_local_stream(&listener.path).unwrap();
        writeln!(stream, "not json").unwrap();
        drop(stream);
        std::thread::sleep(Duration::from_millis(25));
        assert!(event_rx.try_recv().is_err());
        drop(listener);
        let _ = std::fs::remove_dir(&state_path);
    }

    #[test]
    fn timed_out_socket_report_does_not_block_a_later_report() {
        let (event_tx, mut event_rx) = tokio::sync::mpsc::channel(1);
        let state_path = std::env::temp_dir().join(format!(
            "herdr-embedded-report-timeout-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (listener, environment, pane) =
            PaneReportListener::start(event_tx, &state_path).unwrap();
        let path = listener.path.clone();
        let token = environment[1].1.clone();
        let pane_id = crate::layout::PaneId::alloc();
        *pane.lock().unwrap() = Some(pane_id);
        let stalled = crate::ipc::connect_local_stream(&path).unwrap();

        let sender = std::thread::spawn(move || {
            std::thread::sleep(REPORT_DEADLINE + Duration::from_millis(50));
            let mut stream = crate::ipc::connect_local_stream(&path).unwrap();
            writeln!(
                stream,
                "{}",
                serde_json::json!({
                    "token": token,
                    "source": "herdr:opencode",
                    "agent": "opencode",
                    "state": "idle"
                })
            )
            .unwrap();
        });

        assert!(matches!(
            event_rx.blocking_recv(),
            Some(AppEvent::HookStateReported { pane_id: reported_pane, .. }) if reported_pane == pane_id
        ));
        drop(stalled);
        sender.join().unwrap();
        drop(listener);
        let _ = std::fs::remove_dir(&state_path);
    }

    #[test]
    fn full_event_channel_does_not_acknowledge_a_report() {
        let (event_tx, _event_rx) = tokio::sync::mpsc::channel(1);
        let state_path =
            std::env::temp_dir().join(format!("herdr-embedded-report-test-{}", std::process::id()));
        let (listener, environment, pane) =
            PaneReportListener::start(event_tx.clone(), &state_path).unwrap();
        let token = environment[1].1.clone();
        let pane_id = crate::layout::PaneId::alloc();
        *pane.lock().unwrap() = Some(pane_id);

        assert!(dispatch_report(
            event_tx,
            pane_id,
            Report {
                token: token.clone(),
                source: "herdr:opencode".into(),
                agent: "opencode".into(),
                state: Some("idle".into()),
                seq: None,
                agent_session_id: None,
                agent_session_path: None,
                session_start_source: None,
                message: None,
                completion: None,
            },
        ));

        let mut stream = crate::ipc::connect_local_stream(&listener.path).unwrap();
        writeln!(
            stream,
            "{}",
            serde_json::json!({
                "token": token,
                "source": "herdr:opencode",
                "agent": "opencode",
                "state": "idle"
            })
        )
        .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).unwrap();

        assert!(response.is_empty());
        drop(listener);
        let _ = std::fs::remove_dir(&state_path);
    }
}
