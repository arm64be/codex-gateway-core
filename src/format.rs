use chrono::{DateTime, Utc};
use codex_app_server_protocol::GetAccountRateLimitsResponse;

pub(crate) fn format_goal(goal: Option<&codex_app_server_protocol::ThreadGoal>) -> String {
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

pub(crate) fn format_rate_limits(response: &GetAccountRateLimitsResponse) -> String {
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
                relative_ts(primary.resets_at)
            ));
        }
        if let Some(secondary) = &snapshot.secondary {
            out.push_str(&format!(
                "; secondary {} remaining, resets {}",
                remaining_percent(secondary.used_percent),
                relative_ts(secondary.resets_at)
            ));
        }
        if snapshot.primary.is_none() && snapshot.secondary.is_none() {
            out.push_str(" no window data");
        }
        out.push('\n');
    }
    out
}

pub(crate) fn fmt_ts(ts: i64) -> String {
    DateTime::<Utc>::from_timestamp(ts, 0)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| "unknown".into())
}

fn remaining_percent(used_percent: i32) -> String {
    format!("{}%", (100 - used_percent).clamp(0, 100))
}

fn relative_ts(ts: Option<i64>) -> String {
    ts.map(|ts| format!("<t:{ts}:R>"))
        .unwrap_or_else(|| "unknown".into())
}
