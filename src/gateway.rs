use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{Context as _, anyhow};
use codex_app_server::{AppClientInfo, AppServer, AppServerConfig, ThreadHandle};
use codex_app_server_protocol::{
    ClientRequest, GetAccountRateLimitsResponse, ModelListParams, ModelListResponse, RequestId,
    ThreadGoalClearParams, ThreadGoalClearResponse, ThreadGoalGetParams, ThreadGoalGetResponse,
    ThreadGoalSetParams, ThreadGoalSetResponse, ThreadGoalStatus, ThreadListParams,
    ThreadListResponse, ThreadResumeParams, ThreadResumeResponse, ThreadStartParams,
    TurnInterruptParams, TurnInterruptResponse, TurnSteerParams, TurnSteerResponse,
};
use tokio::sync::{Mutex, mpsc};
use tracing::error;

use crate::api::{GatewayConfig, GoalStatus, SessionAction, TurnOutput};
use crate::format::{fmt_ts, format_goal, format_rate_limits};
use crate::protocol::{empty_thread_resume_params, text_input};
use crate::state::{QueuedTurn, SessionState, WorkItem};
use crate::turn::run_turn;

pub struct CodexGateway<K> {
    server: Arc<AppServer>,
    sessions: Arc<Mutex<HashMap<K, SessionState>>>,
    default_model: String,
    default_cwd: Option<String>,
    service_name: Option<String>,
    next_request_id: AtomicI64,
    tx: mpsc::Sender<WorkItem<K>>,
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

    pub async fn set_effort(&self, key: K, effort: Option<crate::api::ReasoningEffort>) -> String {
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
            SessionAction::Show => Ok(self.format_session(&key).await),
            SessionAction::New => {
                let thread = self.start_thread(key).await?;
                Ok(format!("Started new thread `{}`.", thread.thread_id))
            }
            SessionAction::List => self.list_sessions().await,
            SessionAction::Switch => self.switch_session(key, thread_id).await,
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

    async fn ensure_thread(&self, key: K) -> anyhow::Result<String> {
        if let Some(thread_id) = self.session_snapshot(&key).await.thread_id {
            return Ok(thread_id);
        }
        Ok(self.start_thread(key).await?.thread_id)
    }

    async fn start_thread(&self, key: K) -> anyhow::Result<ThreadHandle> {
        let snapshot = self.session_snapshot(&key).await;
        let thread = self
            .server
            .thread_start(ThreadStartParams {
                model: Some(snapshot.model),
                cwd: self.default_cwd.clone(),
                service_name: self.service_name.clone(),
                ..Default::default()
            })
            .await?;
        let mut guard = self.sessions.lock().await;
        let session = guard
            .entry(key)
            .or_insert_with(|| SessionState::new(self.default_model.clone()));
        session.thread_id = Some(thread.thread_id.clone());
        Ok(thread)
    }

    async fn list_sessions(&self) -> anyhow::Result<String> {
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

    async fn switch_session(&self, key: K, thread_id: Option<String>) -> anyhow::Result<String> {
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

    async fn format_session(&self, key: &K) -> String {
        let session = self.session_snapshot(key).await;
        format!(
            "Thread: `{}`\nModel: `{}`\nEffort: `{}`",
            session.thread_id.as_deref().unwrap_or("none"),
            session.model,
            session
                .effort
                .map(|e| e.to_string())
                .unwrap_or_else(|| "Codex default".into())
        )
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
