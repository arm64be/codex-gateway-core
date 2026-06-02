use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context as _, anyhow};
use chrono::{DateTime, Utc};
use codex_app_server::{AppClientInfo, AppServer, AppServerConfig, ThreadHandle};
use codex_app_server_protocol::{
    ClientRequest, GetAccountRateLimitsResponse, ModelListParams, ModelListResponse, RequestId,
    ServerNotification, ThreadGoalClearParams, ThreadGoalClearResponse, ThreadGoalGetParams,
    ThreadGoalGetResponse, ThreadGoalSetParams, ThreadGoalSetResponse, ThreadGoalStatus,
    ThreadListParams, ThreadListResponse, ThreadResumeParams, ThreadResumeResponse,
    ThreadStartParams, TurnInterruptParams, TurnInterruptResponse, TurnStartParams,
    TurnStartResponse, TurnSteerParams, TurnSteerResponse, UserInput,
};
use tokio::sync::{Mutex, mpsc};
use tracing::error;

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
    fn send<'a>(&'a self, key: &'a K, text: &'a str) -> BoxFuture<'a, anyhow::Result<()>>;

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
}

impl fmt::Display for ReasoningEffort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Minimal => f.write_str("minimal"),
            Self::Low => f.write_str("low"),
            Self::Medium => f.write_str("medium"),
            Self::High => f.write_str("high"),
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

pub struct CodexGateway<K> {
    server: Arc<AppServer>,
    sessions: Arc<Mutex<HashMap<K, SessionState>>>,
    default_model: String,
    default_cwd: Option<String>,
    service_name: Option<String>,
    next_request_id: AtomicI64,
    tx: mpsc::Sender<WorkItem<K>>,
}

#[derive(Clone)]
struct SessionState {
    thread_id: Option<String>,
    model: String,
    effort: Option<ReasoningEffort>,
    active_turn_id: Option<String>,
    queued: VecDeque<QueuedTurn>,
}

#[derive(Clone)]
struct QueuedTurn {
    prompt: String,
}

struct WorkItem<K> {
    key: K,
    output: Arc<dyn TurnOutput<K>>,
}

impl<K> CodexGateway<K>
where
    K: Clone + Eq + Hash + Send + Sync + fmt::Display + 'static,
{
    pub async fn spawn(config: GatewayConfig) -> anyhow::Result<Self> {
        let mut server = AppServer::spawn(AppServerConfig {
            codex_bin: config.codex_bin,
            server_args: vec!["app-server".into()],
            inherit_stderr: config.inherit_stderr,
        })
        .await?;
        server
            .initialize(AppClientInfo {
                name: config.client_name,
                title: config.client_title,
                version: config.client_version,
            })
            .await?;

        let (tx, rx) = mpsc::channel(128);
        let gateway = Self {
            server: Arc::new(server),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            default_model: config.default_model,
            default_cwd: config.default_cwd,
            service_name: config.service_name,
            next_request_id: AtomicI64::new(10_000),
            tx,
        };
        gateway.start_worker(rx);
        Ok(gateway)
    }

    fn start_worker(&self, mut rx: mpsc::Receiver<WorkItem<K>>) {
        let server = Arc::clone(&self.server);
        let sessions = Arc::clone(&self.sessions);
        let default_model = self.default_model.clone();
        let default_cwd = self.default_cwd.clone();
        let service_name = self.service_name.clone();

        tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                loop {
                    let next = {
                        let mut guard = sessions.lock().await;
                        let session = guard
                            .entry(item.key.clone())
                            .or_insert_with(|| SessionState::new(default_model.clone()));
                        if session.active_turn_id.is_some() {
                            None
                        } else {
                            session.queued.pop_front()
                        }
                    };

                    let Some(turn) = next else {
                        break;
                    };

                    if let Err(err) = run_turn(
                        &server,
                        &sessions,
                        &default_model,
                        default_cwd.as_deref(),
                        service_name.as_deref(),
                        item.key.clone(),
                        turn,
                        Arc::clone(&item.output),
                    )
                    .await
                    {
                        error!(?err, "turn failed");
                        item.output.send_error(&item.key, &err).await;
                    }
                }
            }
        });
    }

    pub async fn enqueue_turn(
        &self,
        key: K,
        prompt: impl Into<String>,
        steer: bool,
        output: Arc<dyn TurnOutput<K>>,
    ) -> anyhow::Result<String> {
        let prompt = prompt.into();
        if steer {
            let snapshot = self.session_snapshot(&key).await;
            if let (Some(thread_id), Some(turn_id)) =
                (snapshot.thread_id.clone(), snapshot.active_turn_id.clone())
            {
                let _: TurnSteerResponse = self
                    .call(|id| ClientRequest::TurnSteer {
                        id,
                        params: TurnSteerParams {
                            client_user_message_id: None,
                            expected_turn_id: turn_id,
                            input: text_input(prompt),
                            thread_id,
                        },
                    })
                    .await?;
                return Ok("Done.".into());
            }
        }

        {
            let mut guard = self.sessions.lock().await;
            let session = guard
                .entry(key.clone())
                .or_insert_with(|| SessionState::new(self.default_model.clone()));
            session.queued.push_back(QueuedTurn { prompt });
        }

        self.tx.send(WorkItem { key, output }).await?;
        Ok("Done.".into())
    }

    pub async fn queue_status(&self, key: &K) -> String {
        let guard = self.sessions.lock().await;
        let Some(session) = guard.get(key) else {
            return "No session for this channel yet.".into();
        };
        let active = session
            .active_turn_id
            .as_deref()
            .unwrap_or("no active turn");
        format!("Active: {active}\nQueued turns: {}", session.queued.len())
    }

    pub async fn model_status(&self, key: &K) -> String {
        let session = self.session_snapshot(key).await;
        format!("Current model: `{}`", session.model)
    }

    pub async fn set_model(&self, key: K, model: impl Into<String>) -> String {
        let model = model.into();
        let mut guard = self.sessions.lock().await;
        let session = guard
            .entry(key)
            .or_insert_with(|| SessionState::new(self.default_model.clone()));
        session.model = model.clone();
        format!("Model set to `{model}`. It will apply to the next turn.")
    }

    pub async fn effort_status(&self, key: &K) -> String {
        let session = self.session_snapshot(key).await;
        match session.effort {
            Some(effort) => format!("Current reasoning effort: `{effort}`"),
            None => "Current reasoning effort: Codex default".into(),
        }
    }

    pub async fn set_effort(&self, key: K, effort: Option<ReasoningEffort>) -> String {
        let mut guard = self.sessions.lock().await;
        let session = guard
            .entry(key)
            .or_insert_with(|| SessionState::new(self.default_model.clone()));
        session.effort = effort;
        match effort {
            Some(value) => format!("Reasoning effort set to `{value}`."),
            None => "Reasoning effort cleared; Codex default will be used.".into(),
        }
    }

    pub async fn list_models(&self) -> anyhow::Result<String> {
        let response: ModelListResponse = self
            .call(|id| ClientRequest::ModelList {
                id,
                params: ModelListParams {
                    include_hidden: Some(false),
                    limit: Some(25),
                    ..Default::default()
                },
            })
            .await?;
        let mut out = String::from("Available models:\n");
        for model in response.data.iter().take(25) {
            let default = if model.is_default { " default" } else { "" };
            out.push_str(&format!(
                "- `{}` ({}){}\n",
                model.model, model.display_name, default
            ));
        }
        if response.next_cursor.is_some() {
            out.push_str("\nMore models exist; increase this command if needed.");
        }
        Ok(out)
    }

    pub async fn status(&self, key: &K) -> anyhow::Result<String> {
        let session = self.session_snapshot(key).await;
        let limits: GetAccountRateLimitsResponse = self
            .call(|id| ClientRequest::AccountRateLimitsRead { id, params: () })
            .await?;

        let mut out = String::new();
        out.push_str(&format!(
            "Thread: `{}`\nModel: `{}`\nEffort: `{}`\n",
            session.thread_id.as_deref().unwrap_or("none"),
            session.model,
            session
                .effort
                .map(|e| e.to_string())
                .unwrap_or_else(|| "Codex default".into())
        ));
        out.push_str(&format_rate_limits(&limits));
        Ok(out)
    }

    pub async fn goal(
        &self,
        key: K,
        objective: Option<String>,
        budget: Option<i64>,
    ) -> anyhow::Result<String> {
        let thread_id = self.ensure_thread(key).await?;
        if let Some(objective) = objective {
            let response: ThreadGoalSetResponse = self
                .call(|id| ClientRequest::ThreadGoalSet {
                    id,
                    params: ThreadGoalSetParams {
                        thread_id: thread_id.clone(),
                        objective: Some(objective),
                        token_budget: budget,
                        status: Some(ThreadGoalStatus::Active),
                    },
                })
                .await?;
            return Ok(format_goal(Some(&response.goal)));
        }

        let response: ThreadGoalGetResponse = self
            .call(|id| ClientRequest::ThreadGoalGet {
                id,
                params: ThreadGoalGetParams {
                    thread_id: thread_id.clone(),
                },
            })
            .await?;
        Ok(format_goal(response.goal.as_ref()))
    }

    pub async fn set_goal_status(&self, key: K, status: GoalStatus) -> anyhow::Result<String> {
        let thread_id = self.ensure_thread(key).await?;
        let response: ThreadGoalSetResponse = self
            .call(|id| ClientRequest::ThreadGoalSet {
                id,
                params: ThreadGoalSetParams {
                    thread_id: thread_id.clone(),
                    objective: None,
                    token_budget: None,
                    status: Some(match status {
                        GoalStatus::Active => ThreadGoalStatus::Active,
                        GoalStatus::Paused => ThreadGoalStatus::Paused,
                    }),
                },
            })
            .await?;
        Ok(format_goal(Some(&response.goal)))
    }

    pub async fn clear_goal(&self, key: K) -> anyhow::Result<String> {
        let thread_id = self.ensure_thread(key).await?;
        let response: ThreadGoalClearResponse = self
            .call(|id| ClientRequest::ThreadGoalClear {
                id,
                params: ThreadGoalClearParams {
                    thread_id: thread_id.clone(),
                },
            })
            .await?;
        Ok(if response.cleared {
            "Goal cleared.".into()
        } else {
            "No goal was set.".into()
        })
    }

    pub async fn session(
        &self,
        key: K,
        action: SessionAction,
        thread_id: Option<String>,
    ) -> anyhow::Result<String> {
        match action {
            SessionAction::Show => {
                let session = self.session_snapshot(&key).await;
                Ok(format!(
                    "Thread: `{}`\nModel: `{}`\nEffort: `{}`",
                    session.thread_id.as_deref().unwrap_or("none"),
                    session.model,
                    session
                        .effort
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| "Codex default".into())
                ))
            }
            SessionAction::New => {
                let thread = self.start_thread(key).await?;
                Ok(format!("Started new thread `{}`.", thread.thread_id))
            }
            SessionAction::List => {
                let response: ThreadListResponse = self
                    .call(|id| ClientRequest::ThreadList {
                        id,
                        params: ThreadListParams {
                            limit: Some(10),
                            ..Default::default()
                        },
                    })
                    .await?;
                let mut out = String::from("Recent threads:\n");
                for thread in response.data {
                    out.push_str(&format!(
                        "- `{}` {} ({})\n",
                        thread.id,
                        thread.name.unwrap_or(thread.preview),
                        fmt_ts(thread.updated_at)
                    ));
                }
                Ok(out)
            }
            SessionAction::Switch => {
                let thread_id = thread_id.context("thread_id is required")?;
                let snapshot = self.session_snapshot(&key).await;
                let response: ThreadResumeResponse = self
                    .call(|id| ClientRequest::ThreadResume {
                        id,
                        params: ThreadResumeParams {
                            thread_id: thread_id.clone(),
                            model: Some(snapshot.model.clone()),
                            cwd: self.default_cwd.clone(),
                            ..empty_thread_resume_params()
                        },
                    })
                    .await?;
                let mut guard = self.sessions.lock().await;
                let session = guard
                    .entry(key)
                    .or_insert_with(|| SessionState::new(self.default_model.clone()));
                session.thread_id = Some(response.thread.id.clone());
                Ok(format!("Switched to thread `{}`.", response.thread.id))
            }
        }
    }

    pub async fn interrupt(&self, key: &K) -> anyhow::Result<String> {
        let session = self.session_snapshot(key).await;
        let thread_id = session
            .thread_id
            .ok_or_else(|| anyhow!("no active thread for this channel"))?;
        let turn_id = session
            .active_turn_id
            .ok_or_else(|| anyhow!("no active turn for this channel"))?;
        let _: TurnInterruptResponse = self
            .call(|id| ClientRequest::TurnInterrupt {
                id,
                params: TurnInterruptParams { thread_id, turn_id },
            })
            .await?;
        Ok("Interrupt requested.".into())
    }

    async fn ensure_thread(&self, key: K) -> anyhow::Result<String> {
        if let Some(thread_id) = self.session_snapshot(&key).await.thread_id {
            return Ok(thread_id);
        }
        Ok(self.start_thread(key).await?.thread_id)
    }

    async fn start_thread(&self, key: K) -> anyhow::Result<ThreadHandle> {
        let snapshot = self.session_snapshot(&key).await;
        let thread = {
            self.server
                .thread_start(ThreadStartParams {
                    model: Some(snapshot.model),
                    cwd: self.default_cwd.clone(),
                    service_name: self.service_name.clone(),
                    ..Default::default()
                })
                .await?
        };
        let mut guard = self.sessions.lock().await;
        let session = guard
            .entry(key)
            .or_insert_with(|| SessionState::new(self.default_model.clone()));
        session.thread_id = Some(thread.thread_id.clone());
        Ok(thread)
    }

    async fn call<R, F>(&self, build: F) -> anyhow::Result<R>
    where
        R: for<'de> serde::Deserialize<'de>,
        F: FnOnce(RequestId) -> ClientRequest,
    {
        let id = RequestId::Int64(self.next_request_id.fetch_add(1, Ordering::Relaxed));
        self.server.call(build(id)).await.map_err(Into::into)
    }

    async fn session_snapshot(&self, key: &K) -> SessionState {
        let mut guard = self.sessions.lock().await;
        guard
            .entry(key.clone())
            .or_insert_with(|| SessionState::new(self.default_model.clone()))
            .clone()
    }
}

impl SessionState {
    fn new(model: String) -> Self {
        Self {
            thread_id: None,
            model,
            effort: None,
            active_turn_id: None,
            queued: VecDeque::new(),
        }
    }
}

async fn run_turn<K>(
    server: &Arc<AppServer>,
    sessions: &Mutex<HashMap<K, SessionState>>,
    default_model: &str,
    default_cwd: Option<&str>,
    service_name: Option<&str>,
    key: K,
    queued: QueuedTurn,
    output: Arc<dyn TurnOutput<K>>,
) -> anyhow::Result<()>
where
    K: Clone + Eq + Hash + Send + Sync + fmt::Display + 'static,
{
    let snapshot = {
        let mut guard = sessions.lock().await;
        guard
            .entry(key.clone())
            .or_insert_with(|| SessionState::new(default_model.to_string()))
            .clone()
    };

    let thread_id = match snapshot.thread_id {
        Some(thread_id) => thread_id,
        None => {
            let thread = {
                server
                    .thread_start(ThreadStartParams {
                        model: Some(snapshot.model.clone()),
                        cwd: default_cwd.map(str::to_string),
                        service_name: service_name.map(str::to_string),
                        ..Default::default()
                    })
                    .await?
            };
            let mut guard = sessions.lock().await;
            let session = guard
                .entry(key.clone())
                .or_insert_with(|| SessionState::new(default_model.to_string()));
            session.thread_id = Some(thread.thread_id.clone());
            thread.thread_id
        }
    };

    let start: TurnStartResponse = {
        server
            .call(ClientRequest::TurnStart {
                id: RequestId::String(format!("turn-start-{key}-{thread_id}")),
                params: TurnStartParams {
                    thread_id: thread_id.clone(),
                    input: text_input(queued.prompt),
                    model: Some(snapshot.model),
                    effort: snapshot.effort.map(Into::into),
                    cwd: default_cwd.map(str::to_string),
                    ..empty_turn_start_params()
                },
            })
            .await?
    };
    let turn_id = start.turn.id.clone();

    {
        let mut guard = sessions.lock().await;
        if let Some(session) = guard.get_mut(&key) {
            session.active_turn_id = Some(turn_id.clone());
        }
    }

    let mut sent_agent_items = HashSet::new();

    loop {
        let incoming = { server.recv().await? };

        let codex_app_server::IncomingMessage::Notification(notification) = incoming else {
            continue;
        };

        match *notification {
            ServerNotification::ItemCompleted(item)
                if item.thread_id == thread_id && item.turn_id == turn_id =>
            {
                if let Some((id, text)) = agent_item_text(&item.item)
                    && !text.trim().is_empty()
                    && sent_agent_items.insert(id.to_string())
                {
                    output.send(&key, text).await?;
                }
            }
            ServerNotification::TurnCompleted(note)
                if note.thread_id == thread_id && note.turn.id == turn_id =>
            {
                for item in &note.turn.items {
                    if let Some((id, text)) = agent_item_text(item)
                        && !text.trim().is_empty()
                        && sent_agent_items.insert(id.to_string())
                    {
                        output.send(&key, text).await?;
                    }
                }
                clear_active_turn(sessions, &key).await;
                break;
            }
            ServerNotification::Error(err)
                if err.thread_id == thread_id && err.turn_id == turn_id =>
            {
                clear_active_turn(sessions, &key).await;
                return Err(anyhow!("{:?}", err.error));
            }
            _ => {}
        }
    }

    Ok(())
}

async fn clear_active_turn<K>(sessions: &Mutex<HashMap<K, SessionState>>, key: &K)
where
    K: Eq + Hash,
{
    let mut guard = sessions.lock().await;
    if let Some(session) = guard.get_mut(key) {
        session.active_turn_id = None;
    }
}

impl From<ReasoningEffort> for codex_app_server_protocol::ReasoningEffort {
    fn from(value: ReasoningEffort) -> Self {
        match value {
            ReasoningEffort::Minimal => Self::Minimal,
            ReasoningEffort::Low => Self::Low,
            ReasoningEffort::Medium => Self::Medium,
            ReasoningEffort::High => Self::High,
        }
    }
}

fn text_input(text: String) -> Vec<UserInput> {
    vec![UserInput::Text {
        text,
        text_elements: vec![],
    }]
}

fn empty_turn_start_params() -> TurnStartParams {
    TurnStartParams {
        approval_policy: None,
        approvals_reviewer: None,
        client_user_message_id: None,
        cwd: None,
        effort: None,
        input: vec![],
        model: None,
        output_schema: None,
        personality: None,
        sandbox_policy: None,
        service_tier: None,
        summary: None,
        thread_id: String::new(),
    }
}

fn empty_thread_resume_params() -> ThreadResumeParams {
    ThreadResumeParams {
        approval_policy: None,
        approvals_reviewer: None,
        base_instructions: None,
        config: None,
        cwd: None,
        developer_instructions: None,
        model: None,
        model_provider: None,
        personality: None,
        sandbox: None,
        service_tier: None,
        thread_id: String::new(),
    }
}

fn format_goal(goal: Option<&codex_app_server_protocol::ThreadGoal>) -> String {
    let Some(goal) = goal else {
        return "No goal set.".into();
    };
    format!(
        "Goal: {}\nStatus: `{}`\nTokens: {}{}\nTime used: {}s",
        goal.objective,
        goal.status,
        goal.tokens_used,
        goal.token_budget
            .map(|budget| format!(" / {budget}"))
            .unwrap_or_default(),
        goal.time_used_seconds
    )
}

fn format_rate_limits(response: &GetAccountRateLimitsResponse) -> String {
    let mut out = String::from("Rate limits\n");
    let snapshots: Vec<_> = response
        .rate_limits_by_limit_id
        .as_ref()
        .map(|map| map.values().collect())
        .unwrap_or_else(|| vec![&response.rate_limits]);

    for snapshot in snapshots {
        let name = snapshot
            .limit_name
            .as_deref()
            .or(snapshot.limit_id.as_deref())
            .unwrap_or("default");
        out.push_str(&format!("- **{name}**"));
        if let Some(primary) = &snapshot.primary {
            out.push_str(&format!(
                ": primary {} remaining, resets {}",
                remaining_percent(primary.used_percent),
                discord_relative_ts(primary.resets_at)
            ));
        }
        if let Some(secondary) = &snapshot.secondary {
            out.push_str(&format!(
                "; secondary {} remaining, resets {}",
                remaining_percent(secondary.used_percent),
                discord_relative_ts(secondary.resets_at)
            ));
        }
        if snapshot.primary.is_none() && snapshot.secondary.is_none() {
            out.push_str(" no window data");
        }
        out.push('\n');
    }
    out
}

fn remaining_percent(used_percent: i32) -> String {
    format!("{}%", (100 - used_percent).clamp(0, 100))
}

fn discord_relative_ts(ts: Option<i64>) -> String {
    ts.map(|ts| format!("<t:{ts}:R>"))
        .unwrap_or_else(|| "unknown".into())
}

fn agent_item_text(item: &codex_app_server_protocol::ThreadItem) -> Option<(&str, &str)> {
    match item {
        codex_app_server_protocol::ThreadItem::AgentMessage { id, text, .. } => {
            Some((id.as_str(), text.as_str()))
        }
        _ => None,
    }
}

fn fmt_ts(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| "unknown".into())
}
