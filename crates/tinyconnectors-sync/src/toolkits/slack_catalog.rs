//! The curated action catalog for the `slack` toolkit.
//!
//! Ported action-for-action from the list Slack was already curated against
//! before the connector pipelines moved into this crate, so an agent that had
//! Slack tools keeps exactly the ones it had.
//!
//! # Why the reads are the long half
//!
//! Composio publishes far more Slack actions than this. The ones kept are the
//! ones an agent has a reason to reach for: find a channel or a person, read a
//! conversation, say something, react. The administrative tail — channel
//! management, workspace membership, reminders — is present only where a user
//! might plausibly ask for it out loud, and every entry that changes who can
//! see what is scoped [`ToolScope::Admin`] so a "read only" preference refuses
//! it.

use crate::scope::{CuratedTool, ToolScope};

/// Slack actions worth offering an agent, with how invasive each one is.
pub(super) const CURATED: &[CuratedTool] = &[
    // ── reads ───────────────────────────────────────────────────────
    CuratedTool {
        slug: "SLACK_FIND_CHANNELS",
        scope: ToolScope::Read,
    },
    CuratedTool {
        slug: "SLACK_FIND_USERS",
        scope: ToolScope::Read,
    },
    CuratedTool {
        slug: "SLACK_FETCH_CONVERSATION_HISTORY",
        scope: ToolScope::Read,
    },
    CuratedTool {
        slug: "SLACK_FETCH_MESSAGE_THREAD_FROM_A_CONVERSATION",
        scope: ToolScope::Read,
    },
    CuratedTool {
        slug: "SLACK_LIST_ALL_CHANNELS",
        scope: ToolScope::Read,
    },
    CuratedTool {
        slug: "SLACK_LIST_ALL_USERS",
        scope: ToolScope::Read,
    },
    CuratedTool {
        slug: "SLACK_LIST_CONVERSATIONS",
        scope: ToolScope::Read,
    },
    CuratedTool {
        slug: "SLACK_FETCH_TEAM_INFO",
        scope: ToolScope::Read,
    },
    CuratedTool {
        slug: "SLACK_GET_USER_PRESENCE",
        scope: ToolScope::Read,
    },
    CuratedTool {
        slug: "SLACK_ASSISTANT_SEARCH_CONTEXT",
        scope: ToolScope::Read,
    },
    // ── writes ──────────────────────────────────────────────────────
    CuratedTool {
        slug: "SLACK_SEND_MESSAGE",
        scope: ToolScope::Write,
    },
    CuratedTool {
        slug: "SLACK_POST_MESSAGE_TO_CHANNEL",
        scope: ToolScope::Write,
    },
    CuratedTool {
        slug: "SLACK_SEND_MESSAGE_TO_CHANNEL",
        scope: ToolScope::Write,
    },
    CuratedTool {
        slug: "SLACK_CREATE_CHANNEL",
        scope: ToolScope::Write,
    },
    CuratedTool {
        slug: "SLACK_ADD_REACTION_TO_AN_ITEM",
        scope: ToolScope::Write,
    },
    CuratedTool {
        slug: "SLACK_UPLOAD_FILE",
        scope: ToolScope::Write,
    },
    CuratedTool {
        slug: "SLACK_CREATE_A_REMINDER",
        scope: ToolScope::Write,
    },
    CuratedTool {
        slug: "SLACK_CREATE_USER_GROUP",
        scope: ToolScope::Write,
    },
    // ── admin ───────────────────────────────────────────────────────
    // Everything that removes something, or changes who can reach it.
    // Several of these read as `Read` to the verb heuristic — `ARCHIVE`,
    // `LEAVE` and `CONVERT` are in none of its word lists — so the curation is
    // doing real work here rather than restating what the verb already says.
    CuratedTool {
        slug: "SLACK_DELETE_CHANNEL",
        scope: ToolScope::Admin,
    },
    CuratedTool {
        slug: "SLACK_ARCHIVE_CONVERSATION",
        scope: ToolScope::Admin,
    },
    CuratedTool {
        slug: "SLACK_DELETE_FILE",
        scope: ToolScope::Admin,
    },
    CuratedTool {
        slug: "SLACK_DELETES_A_MESSAGE_FROM_A_CHAT",
        scope: ToolScope::Admin,
    },
    CuratedTool {
        slug: "SLACK_DELETE_REMINDER",
        scope: ToolScope::Admin,
    },
    CuratedTool {
        slug: "SLACK_LEAVE_CONVERSATION",
        scope: ToolScope::Admin,
    },
    CuratedTool {
        slug: "SLACK_INVITE_USER_TO_WORKSPACE",
        scope: ToolScope::Admin,
    },
    CuratedTool {
        slug: "SLACK_CONVERT_CHANNEL_TO_PRIVATE",
        scope: ToolScope::Admin,
    },
    // Adding someone to a channel hands them its whole history, which is a
    // change to who can see what however friendly the verb sounds. The verb
    // heuristic reads `INVITE` as a plain read, and
    // `SLACK_INVITE_USER_TO_WORKSPACE` above is already `Admin`; leaving this
    // one at `Write` let a "changes, but nothing destructive" preference grant
    // channel access.
    CuratedTool {
        slug: "SLACK_INVITE_USERS_TO_A_SLACK_CHANNEL",
        scope: ToolScope::Admin,
    },
];
