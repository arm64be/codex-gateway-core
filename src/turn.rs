use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::anyhow;
use codex_app_server::AppServer;
use codex_app_server_protocol::{
    ClientRequest, RequestId, ServerNotification, ThreadStartParams, TurnStartParams,
    TurnStartResponse,
};
use tokio::sync::Mutex;

use crate::api::TurnOutput;
use crate::protocol::{agent_item_text, empty_turn_start_params, text_input};
use crate::state::{QueuedTurn, SessionState};

pub(crate) async fn run_turn<K>(
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
            let thread = server
                .thread_start(ThreadStartParams {
                    model: Some(snapshot.model.clone()),
                    cwd: default_cwd.map(str::to_string),
                    service_name: service_name.map(str::to_string),
                    ..Default::default()
                })
                .await?;
            let mut guard = sessions.lock().await;
            let session = guard
                .entry(key.clone())
                .or_insert_with(|| SessionState::new(default_model.to_string()));
            session.thread_id = Some(thread.thread_id.clone());
            thread.thread_id
        }
    };

    let start: TurnStartResponse = server
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
        .await?;
    let turn_id = start.turn.id.clone();

    {
        let mut guard = sessions.lock().await;
        if let Some(session) = guard.get_mut(&key) {
            session.active_turn_id = Some(turn_id.clone());
        }
    }

    let mut sent_agent_items = HashSet::new();
    loop {
        let incoming = server.recv().await?;
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
