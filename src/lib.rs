//! Embedded-safe runtime contract for hosts that integrate Herdr.
#![allow(dead_code)] // The App graph is shared with the binary; its CLI-only paths stay private here.

pub(crate) const HERDR_ENV_VAR: &str = "HERDR_ENV";
pub(crate) const HERDR_ENV_VALUE: &str = "1";

mod agent_resume;
mod agent_view_eval;
mod api;
mod app;
mod build_info;
mod checksum;
mod config;
mod copy_mode;
mod detect;
mod events;
use ghostty_vt as ghostty;
mod handoff_runtime;
mod input;
mod integration;
mod ipc;
mod kitty_graphics;
mod layout;
mod logging;
mod metadata_tokens;
mod noninteractive_process;
mod pane;
use ghostty_vt::pane_graphics_files;
mod client;
mod persist;
mod platform;
mod plugin_command;
mod plugin_installations;
mod plugin_paths;
mod popup_size;
mod product_announcements;
mod protocol;
mod pty;
mod raw_input;
mod release_notes;
mod remote;
mod render_prof;
mod render_signal;
mod runtime;
mod selection;
mod server;
mod session;
mod sound;
mod terminal;
mod terminal_effects;
mod terminal_modes;
mod terminal_notify;
mod terminal_theme;
mod thread_spawn;
mod ui;
mod update;
mod workspace;
mod worktree;

pub use runtime::{
    AgentSession, CancellationToken, LocalRuntime, LogicalTarget, OpenCodeConfig, RuntimeConfig,
    RuntimeError, RuntimeEvent, RuntimeHandle, RuntimeSubscription, SessionSelection,
    TargetExecutionConfig, TerminalConfig, TurnId, TurnResult, TURN_RESULT_RETRY_WINDOW,
};
