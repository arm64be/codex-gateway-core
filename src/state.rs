use std::collections::VecDeque;
use std::sync::Arc;

use crate::api::{ReasoningEffort, TurnOutput};

#[derive(Clone)]
pub(crate) struct SessionState {
    pub(crate) thread_id: Option<String>,
    pub(crate) model: String,
    pub(crate) effort: Option<ReasoningEffort>,
    pub(crate) active_turn_id: Option<String>,
    pub(crate) queued: VecDeque<QueuedTurn>,
}

#[derive(Clone)]
pub(crate) struct QueuedTurn {
    pub(crate) prompt: String,
}

pub(crate) struct WorkItem<K> {
    pub(crate) key: K,
    pub(crate) output: Arc<dyn TurnOutput<K>>,
}

impl SessionState {
    pub(crate) fn new(model: String) -> Self {
        Self {
            thread_id: None,
            model,
            effort: None,
            active_turn_id: None,
            queued: VecDeque::new(),
        }
    }
}
