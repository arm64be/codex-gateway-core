use codex_app_server_protocol::{ThreadResumeParams, TurnStartParams, UserInput};

pub(crate) fn text_input(text: String) -> Vec<UserInput> {
    vec![UserInput::Text {
        text,
        text_elements: vec![],
    }]
}

pub(crate) fn empty_turn_start_params() -> TurnStartParams {
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

pub(crate) fn empty_thread_resume_params() -> ThreadResumeParams {
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

pub(crate) fn agent_item_text(
    item: &codex_app_server_protocol::ThreadItem,
) -> Option<(&str, &str)> {
    match item {
        codex_app_server_protocol::ThreadItem::AgentMessage { id, text, .. } => {
            Some((id.as_str(), text.as_str()))
        }
        _ => None,
    }
}
