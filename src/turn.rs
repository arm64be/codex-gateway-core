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

    if let Err(err) = output.turn_started(&key).await {
        clear_active_turn(sessions, &key).await;
        return Err(err);
    }

    let mut started_agent_items = HashSet::new();
    let mut finished_agent_items = HashSet::new();
    let mut sent_agent_items = HashSet::new();
    loop {
        let incoming = server.recv().await?;
        let codex_app_server::IncomingMessage::Notification(notification) = incoming else {
            continue;
        };

        match *notification {
            ServerNotification::ItemStarted(item)
                if item.thread_id == thread_id && item.turn_id == turn_id =>
            {
                if let Some((id, _)) = agent_item_text(&item.item)
                    && started_agent_items.insert(id.to_string())
                    && let Err(err) = output.assistant_message_started(&key).await
                {
                    clear_active_turn(sessions, &key).await;
                    return Err(err);
                }
            }
            ServerNotification::ItemCompleted(item)
                if item.thread_id == thread_id && item.turn_id == turn_id =>
            {
                if let Some((id, text)) = agent_item_text(&item.item)
                    && !text.trim().is_empty()
                    && sent_agent_items.insert(id.to_string())
                {
                    if let Err(err) = send_agent_message(
                        output.as_ref(),
                        &key,
                        id,
                        text,
                        &mut started_agent_items,
                        &mut finished_agent_items,
                    )
                    .await
                    {
                        clear_active_turn(sessions, &key).await;
                        return Err(err);
                    }
                } else if let Some((id, _)) = agent_item_text(&item.item)
                    && started_agent_items.contains(id)
                    && finished_agent_items.insert(id.to_string())
                    && let Err(err) = output.assistant_message_finished(&key).await
                {
                    clear_active_turn(sessions, &key).await;
                    return Err(err);
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
                        if let Err(err) = send_agent_message(
                            output.as_ref(),
                            &key,
                            id,
                            text,
                            &mut started_agent_items,
                            &mut finished_agent_items,
                        )
                        .await
                        {
                            clear_active_turn(sessions, &key).await;
                            return Err(err);
                        }
                    } else if let Some((id, _)) = agent_item_text(item)
                        && started_agent_items.contains(id)
                        && finished_agent_items.insert(id.to_string())
                        && let Err(err) = output.assistant_message_finished(&key).await
                    {
                        clear_active_turn(sessions, &key).await;
                        return Err(err);
                    }
                }
                clear_active_turn(sessions, &key).await;
                break;
            }
            ServerNotification::Error(err)
                if err.thread_id == thread_id && err.turn_id == turn_id =>
            {
                let finish_result = finish_started_agent_messages(
                    output.as_ref(),
                    &key,
                    &started_agent_items,
                    &mut finished_agent_items,
                )
                .await;
                clear_active_turn(sessions, &key).await;
                finish_result?;
                return Err(anyhow!("{:?}", err.error));
            }
            _ => {}
        }
    }

    Ok(())
}

async fn send_agent_message<K>(
    output: &dyn TurnOutput<K>,
    key: &K,
    id: &str,
    text: &str,
    started_agent_items: &mut HashSet<String>,
    finished_agent_items: &mut HashSet<String>,
) -> anyhow::Result<()>
where
    K: Sync + 'static,
{
    if started_agent_items.insert(id.to_string()) {
        output.assistant_message_started(key).await?;
    }

    let send_result = output.send(key, text).await;

    if finished_agent_items.insert(id.to_string()) {
        output.assistant_message_finished(key).await?;
    }

    send_result
}

async fn finish_started_agent_messages<K>(
    output: &dyn TurnOutput<K>,
    key: &K,
    started_agent_items: &HashSet<String>,
    finished_agent_items: &mut HashSet<String>,
) -> anyhow::Result<()>
where
    K: Sync + 'static,
{
    for id in started_agent_items {
        if finished_agent_items.insert(id.clone()) {
            output.assistant_message_finished(key).await?;
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
