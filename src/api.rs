use std::fmt;
use std::future::Future;
use std::pin::Pin;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone)]
pub struct GatewayConfig {
    pub codex_bin: String,
    pub default_model: String,
    pub default_cwd: Option<String>,
    pub inherit_stderr: bool,
    pub client_name: String,
    pub client_title: Option<String>,
    pub client_version: String,
    pub service_name: Option<String>,
}

impl GatewayConfig {
    pub fn new(default_model: impl Into<String>) -> Self {
        Self {
            codex_bin: "codex".into(),
            default_model: default_model.into(),
            default_cwd: None,
            inherit_stderr: false,
            client_name: "codex_gateway".into(),
            client_title: Some("Codex Gateway".into()),
            client_version: "0.1.0".into(),
            service_name: None,
        }
    }
}

pub trait TurnOutput<K: Sync>: Send + Sync + 'static {
    fn turn_started<'a>(&'a self, _key: &'a K) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn assistant_message_started<'a>(&'a self, _key: &'a K) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn send<'a>(&'a self, key: &'a K, text: &'a str) -> BoxFuture<'a, anyhow::Result<()>>;

    fn assistant_message_finished<'a>(&'a self, _key: &'a K) -> BoxFuture<'a, anyhow::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn send_error<'a>(&'a self, key: &'a K, error: &'a anyhow::Error) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let _ = self
                .send(key, &format!("Codex turn failed: {error:#}"))
                .await;
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReasoningEffort {
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
}

impl fmt::Display for ReasoningEffort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Minimal => f.write_str("minimal"),
            Self::Low => f.write_str("low"),
            Self::Medium => f.write_str("medium"),
            Self::High => f.write_str("high"),
            Self::XHigh => f.write_str("xhigh"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoalStatus {
    Active,
    Paused,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAction {
    Show,
    New,
    List,
    Switch,
}

impl From<ReasoningEffort> for codex_app_server_protocol::ReasoningEffort {
    fn from(value: ReasoningEffort) -> Self {
        match value {
            ReasoningEffort::Minimal => Self::Minimal,
            ReasoningEffort::Low => Self::Low,
            ReasoningEffort::Medium => Self::Medium,
            ReasoningEffort::High => Self::High,
            ReasoningEffort::XHigh => Self::Xhigh,
        }
    }
}
