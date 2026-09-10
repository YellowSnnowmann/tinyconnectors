//! Tests for the Slack provider's channel walk.
//!
//! The shared toolkit tests cannot drive this one: their action double answers
//! every action with the same canned payload, and a Slack page read needs the
//! conversation list, the user directory and the history to answer differently.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};

use super::{Cursor, SlackProvider, render, to_millis};
use crate::pipeline::run_sync;
use crate::provider::{ActionRunner, ConnectorProvider, ProviderContext, SyncLimits, SyncReason};
use crate::state::SyncStateStore;
use crate::{Error, Result};

// ── doubles ─────────────────────────────────────────────────────────

/// An action runner scripted per action, recording what it was asked.
#[derive(Debug, Default)]
struct ScriptedActions {
    replies: Mutex<HashMap<String, VecDeque<Result<Value>>>>,
    calls: Mutex<Vec<(String, Value)>>,
}

impl ScriptedActions {
    fn queue(&self, action: &str, reply: Result<Value>) {
        self.replies
            .lock()
            .unwrap()
            .entry(action.to_string())
            .or_default()
            .push_back(reply);
    }

    /// Every call made against `action`, in order, with its arguments.
    fn calls_to(&self, action: &str) -> Vec<Value> {
        self.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(name, _)| name == action)
            .map(|(_, arguments)| arguments.clone())
            .collect()
    }
}

#[async_trait]
impl ActionRunner for ScriptedActions {
    async fn run(&self, action: &str, arguments: Value, _: &str) -> Result<Value> {
        self.calls
            .lock()
            .unwrap()
            .push((action.to_string(), arguments));
        let queued = self
            .replies
            .lock()
            .unwrap()
            .get_mut(action)
            .and_then(VecDeque::pop_front);
        queued.unwrap_or_else(|| Ok(json!({})))
    }
}

/// A state store that actually remembers, so a walk can resume.
#[derive(Debug, Default)]
struct MemoryStore {
    rows: Mutex<HashMap<(String, String), Value>>,
}

#[async_trait]
impl SyncStateStore for MemoryStore {
    async fn get(&self, namespace: &str, key: &str) -> Result<Option<Value>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .get(&(namespace.to_string(), key.to_string()))
            .cloned())
    }

    async fn set(&self, namespace: &str, key: &str, value: &Value) -> Result<()> {
        self.rows
            .lock()
            .unwrap()
            .insert((namespace.to_string(), key.to_string()), value.clone());
        Ok(())
    }
}

fn context(actions: Arc<ScriptedActions>, store: Arc<MemoryStore>) -> ProviderContext {
    ProviderContext {
        toolkit: "slack".into(),
        connection_id: "conn_1".into(),
        source_id: "slack:conn_1".into(),
        limits: SyncLimits::default(),
        actions,
        state: store,
    }
}

fn action_error(message: &str) -> Error {
    Error::Action {
        action: "SLACK_FETCH_CONVERSATION_HISTORY".into(),
        message: message.into(),
    }
}

fn channels(entries: &[(&str, &str)], next: &str) -> Value {
    let list: Vec<Value> = entries
        .iter()
        .map(|(id, name)| json!({ "id": id, "name": name }))
        .collect();
    json!({ "data": { "channels": list, "response_metadata": { "next_cursor": next } } })
}

fn history(messages: &[Value], next: &str) -> Value {
    json!({ "data": { "messages": messages, "response_metadata": { "next_cursor": next } } })
}

fn message(ts: &str, text: &str) -> Value {
    json!({ "ts": ts, "user": "U1", "text": text })
}

// ── the cursor ──────────────────────────────────────────────────────

#[test]
fn a_cursor_survives_a_round_trip() {
    let cursor = Cursor::new(3, Some("dXNlcjpVMDYx".into()));
    assert_eq!(Cursor::decode(Some(&cursor.encode())), cursor);

    let start = Cursor::new(0, None);
    assert_eq!(Cursor::decode(Some(&start.encode())), start);
}

#[test]
fn an_unreadable_cursor_restarts_the_walk_rather_than_failing_it() {
    // The seen-set turns a re-read into skips, so restarting costs requests.
    // Erroring would cost the connection every future sync.
    for raw in ["", "not-a-number|abc", "|", "garbage"] {
        assert_eq!(Cursor::decode(Some(raw)).index, 0, "{raw}");
    }
    assert_eq!(Cursor::decode(None), Cursor::new(0, None));
}

// ── the walk ────────────────────────────────────────────────────────

#[tokio::test]
async fn the_walk_pages_each_channel_then_moves_to_the_next_and_ends_once() {
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng"), ("C2", "ops")], "")),
    );
    // C1 has two pages, C2 has one.
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(&[message("1700000002.000100", "newest")], "page2")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(&[message("1700000001.000100", "older")], "")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(&[message("1700000003.000100", "ops news")], "")),
    );

    let store = Arc::new(MemoryStore::default());
    let context = context(actions.clone(), store);
    let provider = SlackProvider;

    let mut cursor = None;
    let mut seen = Vec::new();
    let mut pages = 0;
    loop {
        let page = provider
            .fetch_page(&context, cursor.as_deref())
            .await
            .unwrap();
        seen.extend(page.records.iter().map(|record| record.item_id.clone()));
        pages += 1;
        assert!(pages < 10, "the walk did not terminate");
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    assert_eq!(
        seen,
        vec![
            "C1:1700000002.000100",
            "C1:1700000001.000100",
            "C2:1700000003.000100"
        ]
    );
    assert_eq!(
        actions.calls_to("SLACK_LIST_CONVERSATIONS").len(),
        1,
        "the roster is read once per walk, not once per channel"
    );
}

#[tokio::test]
async fn the_same_timestamp_in_two_channels_yields_two_records() {
    // A Slack `ts` is unique within a channel only, and the run loop dedupes
    // against one flat set for the whole connection.
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng"), ("C2", "ops")], "")),
    );
    for _ in 0..2 {
        actions.queue(
            "SLACK_FETCH_CONVERSATION_HISTORY",
            Ok(history(&[message("1700000000.000100", "same")], "")),
        );
    }

    let context = context(actions, Arc::new(MemoryStore::default()));
    let provider = SlackProvider;

    let first = provider.fetch_page(&context, None).await.unwrap();
    let second = provider
        .fetch_page(&context, first.next_cursor.as_deref())
        .await
        .unwrap();

    assert_eq!(first.records[0].item_id, "C1:1700000000.000100");
    assert_eq!(second.records[0].item_id, "C2:1700000000.000100");
}

#[tokio::test]
async fn one_unreadable_channel_is_skipped_not_fatal() {
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "private"), ("C2", "ops")], "")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Err(action_error("not_in_channel")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(&[message("1700000000.000100", "readable")], "")),
    );

    let context = context(actions, Arc::new(MemoryStore::default()));
    let provider = SlackProvider;

    let skipped = provider.fetch_page(&context, None).await.unwrap();
    assert!(skipped.records.is_empty());
    let next = skipped.next_cursor.expect("the walk must move past it");

    let page = provider.fetch_page(&context, Some(&next)).await.unwrap();
    assert_eq!(page.records.len(), 1, "the other channel still syncs");
}

#[tokio::test]
async fn a_revoked_token_fails_the_run_instead_of_every_channel() {
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng")], "")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Err(action_error("token_revoked")),
    );

    let context = context(actions, Arc::new(MemoryStore::default()));
    assert!(SlackProvider.fetch_page(&context, None).await.is_err());
}

#[tokio::test]
async fn a_finished_channel_is_read_from_its_newest_message_next_time() {
    let actions = Arc::new(ScriptedActions::default());
    for _ in 0..2 {
        actions.queue(
            "SLACK_LIST_CONVERSATIONS",
            Ok(channels(&[("C1", "eng")], "")),
        );
    }
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(&[message("1700000009.000100", "newest")], "")),
    );
    actions.queue("SLACK_FETCH_CONVERSATION_HISTORY", Ok(history(&[], "")));

    let store = Arc::new(MemoryStore::default());
    let context = context(actions.clone(), store);
    let provider = SlackProvider;

    // First walk: no mark yet, so the depth bound is the floor.
    provider.fetch_page(&context, None).await.unwrap();
    let first = &actions.calls_to("SLACK_FETCH_CONVERSATION_HISTORY")[0];
    assert_ne!(first["oldest"], json!("1700000009.000100"));

    // Second walk, after the channel was exhausted: start at the mark.
    provider.fetch_page(&context, None).await.unwrap();
    let second = &actions.calls_to("SLACK_FETCH_CONVERSATION_HISTORY")[1];
    assert_eq!(second["oldest"], json!("1700000009.000100"));
}

#[tokio::test]
async fn a_run_without_a_depth_bound_asks_for_everything() {
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng")], "")),
    );
    actions.queue("SLACK_FETCH_CONVERSATION_HISTORY", Ok(history(&[], "")));

    let mut context = context(actions.clone(), Arc::new(MemoryStore::default()));
    context.limits = SyncLimits {
        max_items: 50,
        depth_days: None,
    };

    SlackProvider.fetch_page(&context, None).await.unwrap();
    let arguments = &actions.calls_to("SLACK_FETCH_CONVERSATION_HISTORY")[0];
    assert!(
        arguments.get("oldest").is_none(),
        "an unbounded run must not invent a floor: {arguments}"
    );
}

#[tokio::test]
async fn a_join_notice_with_no_text_is_not_a_memory() {
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng")], "")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(
            &[
                json!({ "ts": "1700000000.000100", "subtype": "channel_join" }),
                message("1700000001.000100", "real content"),
            ],
            "",
        )),
    );

    let context = context(actions, Arc::new(MemoryStore::default()));
    let page = SlackProvider.fetch_page(&context, None).await.unwrap();
    assert_eq!(page.records.len(), 1);
    assert_eq!(page.records[0].content, "real content");
}

#[tokio::test]
async fn an_edited_message_reports_a_version_so_it_re_ingests() {
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng")], "")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(
            &[json!({
                "ts": "1700000000.000100",
                "user": "U1",
                "text": "fixed a typo",
                "edited": { "ts": "1700000050.000200" }
            })],
            "",
        )),
    );

    let context = context(actions, Arc::new(MemoryStore::default()));
    let page = SlackProvider.fetch_page(&context, None).await.unwrap();
    assert_eq!(
        page.versions,
        vec![(
            "C1:1700000000.000100".to_string(),
            "1700000050.000200".to_string()
        )]
    );
}

// ── rendering ───────────────────────────────────────────────────────

#[test]
fn references_resolve_to_something_a_person_can_read() {
    let mut users = std::collections::BTreeMap::new();
    users.insert("U1".to_string(), "Ada".to_string());

    assert_eq!(render("hi <@U1>", &users), "hi @Ada");
    assert_eq!(render("see <#C7|deploys>", &users), "see #deploys");
    assert_eq!(
        render("read <https://example.com|the docs>", &users),
        "read the docs"
    );
    assert_eq!(
        render("bare <https://example.com>", &users),
        "bare https://example.com"
    );
}

#[test]
fn an_unknown_user_degrades_rather_than_dropping_the_message() {
    let users = std::collections::BTreeMap::new();
    assert_eq!(render("ping <@U404>", &users), "ping @U404");
    assert_eq!(render("ping <@U404|ada>", &users), "ping @ada");
    // An unmatched bracket is text, not a broken reference.
    assert_eq!(render("2 < 3 and more", &users), "2 < 3 and more");
}

#[test]
fn a_slack_timestamp_becomes_milliseconds() {
    assert_eq!(to_millis("1700000000.000100"), Some(1_700_000_000_000));
    assert_eq!(to_millis("1700000000.123456"), Some(1_700_000_000_123));
    // A short fraction is padded, not read as a smaller number.
    assert_eq!(to_millis("1700000000.5"), Some(1_700_000_000_500));
    assert_eq!(to_millis("nonsense"), None);
}

#[tokio::test]
async fn the_profile_names_the_workspace() {
    // A Slack connection is *of* a workspace; a UI labelling the account has
    // nothing better to show than its name.
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_FETCH_TEAM_INFO",
        Ok(json!({
            "team": {
                "name": "Tiny Humans",
                "domain": "tinyhumans",
                "icon": { "image_132": "https://example.com/team.png" }
            }
        })),
    );

    let context = context(actions, Arc::new(MemoryStore::default()));
    let profile = SlackProvider.fetch_user_profile(&context).await.unwrap();

    assert_eq!(profile.toolkit, "slack");
    assert_eq!(profile.connection_id.as_deref(), Some("conn_1"));
    assert_eq!(profile.display_name.as_deref(), Some("Tiny Humans"));
    assert_eq!(profile.username.as_deref(), Some("tinyhumans"));
    assert_eq!(
        profile.avatar_url.as_deref(),
        Some("https://example.com/team.png")
    );
}

#[tokio::test]
async fn a_workspace_reporting_nothing_yields_an_empty_profile_not_an_error() {
    let context = context(
        Arc::new(ScriptedActions::default()),
        Arc::new(MemoryStore::default()),
    );
    let profile = SlackProvider.fetch_user_profile(&context).await.unwrap();
    assert_eq!(profile.toolkit, "slack");
    assert!(profile.display_name.is_none());
}

#[tokio::test]
async fn the_roster_is_walked_one_page_at_a_time() {
    // A workspace larger than one roster page costs one extra request per page,
    // not a loop inside a single page read.
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng")], "roster2")),
    );
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C2", "ops")], "")),
    );
    for ts in ["1700000001.000100", "1700000002.000100"] {
        actions.queue(
            "SLACK_FETCH_CONVERSATION_HISTORY",
            Ok(history(&[message(ts, "hello")], "")),
        );
    }

    let context = context(actions.clone(), Arc::new(MemoryStore::default()));
    let provider = SlackProvider;

    let mut cursor = None;
    let mut seen = Vec::new();
    for _ in 0..6 {
        let page = provider
            .fetch_page(&context, cursor.as_deref())
            .await
            .unwrap();
        seen.extend(page.records.iter().map(|record| record.item_id.clone()));
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    assert_eq!(seen, vec!["C1:1700000001.000100", "C2:1700000002.000100"]);
    assert_eq!(actions.calls_to("SLACK_LIST_CONVERSATIONS").len(), 2);
}

#[tokio::test]
async fn a_roster_cursor_that_does_not_advance_ends_the_walk() {
    // Slack repeating the cursor it was handed would otherwise walk the same
    // page until the daily budget ran out, ingesting nothing on every pass.
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng")], "stuck")),
    );
    actions.queue("SLACK_LIST_CONVERSATIONS", Ok(channels(&[], "stuck")));
    actions.queue("SLACK_FETCH_CONVERSATION_HISTORY", Ok(history(&[], "")));

    let context = context(actions.clone(), Arc::new(MemoryStore::default()));
    let provider = SlackProvider;

    let mut cursor = None;
    let mut pages = 0;
    loop {
        let page = provider
            .fetch_page(&context, cursor.as_deref())
            .await
            .unwrap();
        pages += 1;
        assert!(pages < 8, "the walk did not terminate on a stuck cursor");
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    assert_eq!(actions.calls_to("SLACK_LIST_CONVERSATIONS").len(), 2);
}

#[tokio::test]
async fn authors_and_mentions_read_as_names_and_the_directory_is_cached() {
    let actions = Arc::new(ScriptedActions::default());
    for _ in 0..2 {
        actions.queue(
            "SLACK_LIST_CONVERSATIONS",
            Ok(channels(&[("C1", "eng")], "")),
        );
    }
    actions.queue(
        "SLACK_LIST_ALL_USERS",
        Ok(json!({ "data": { "members": [
            { "id": "U1", "profile": { "display_name": "Ada" } },
            { "id": "U2", "real_name": "Grace" }
        ] } })),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(
            &[json!({ "ts": "1700000000.000100", "user": "U1", "text": "ping <@U2>" })],
            "",
        )),
    );
    actions.queue("SLACK_FETCH_CONVERSATION_HISTORY", Ok(history(&[], "")));

    let context = context(actions.clone(), Arc::new(MemoryStore::default()));
    let provider = SlackProvider;

    let page = provider.fetch_page(&context, None).await.unwrap();
    assert_eq!(page.records[0].title, "#eng — Ada");
    assert_eq!(page.records[0].content, "ping @Grace");
    assert_eq!(
        page.records[0].url.as_deref(),
        Some("https://slack.com/archives/C1/p1700000000000100")
    );
    assert_eq!(page.records[0].updated_at_ms, Some(1_700_000_000_000));

    // A second walk reuses the directory rather than paying for it again.
    provider.fetch_page(&context, None).await.unwrap();
    assert_eq!(actions.calls_to("SLACK_LIST_ALL_USERS").len(), 1);
}

#[tokio::test]
async fn a_directory_that_will_not_load_leaves_ids_raw_rather_than_failing() {
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng")], "")),
    );
    actions.queue(
        "SLACK_LIST_ALL_USERS",
        Err(Error::Action {
            action: "SLACK_LIST_ALL_USERS".into(),
            message: "ratelimited".into(),
        }),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(
            &[json!({ "ts": "1700000000.000100", "user": "U9", "text": "hi <@U9>" })],
            "",
        )),
    );

    let context = context(actions, Arc::new(MemoryStore::default()));
    let page = SlackProvider.fetch_page(&context, None).await.unwrap();

    assert_eq!(page.records.len(), 1, "the walk still ingests");
    assert_eq!(page.records[0].content, "hi @U9");
    assert_eq!(page.records[0].title, "#eng — U9");
}

#[tokio::test]
async fn a_message_that_is_only_an_empty_reference_is_not_a_memory() {
    // Slack reports the odd message whose whole body resolves to nothing.
    // Ingesting it would fill a user's memory with blank rows.
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng")], "")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(&[message("1700000000.000100", "<>")], "")),
    );

    let context = context(actions, Arc::new(MemoryStore::default()));
    let page = SlackProvider.fetch_page(&context, None).await.unwrap();
    assert!(page.records.is_empty());
}

#[test]
fn a_channel_reference_without_a_label_keeps_its_id() {
    let users = std::collections::BTreeMap::new();
    assert_eq!(render("see <#C7>", &users), "see #C7");
}

// ── through the real pipeline ───────────────────────────────────────

#[tokio::test]
async fn a_run_stops_at_the_item_limit_and_the_next_one_carries_on() {
    // The limit belongs to the run loop, not to this provider. Driving the
    // real `run_sync` is what proves the composite cursor is a cursor as far
    // as the pipeline is concerned.
    let actions = Arc::new(ScriptedActions::default());
    actions.queue(
        "SLACK_LIST_CONVERSATIONS",
        Ok(channels(&[("C1", "eng"), ("C2", "ops")], "")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(&[message("1700000001.000100", "first")], "")),
    );
    actions.queue(
        "SLACK_FETCH_CONVERSATION_HISTORY",
        Ok(history(&[message("1700000002.000100", "second")], "")),
    );

    let store = Arc::new(MemoryStore::default());
    let mut context = context(actions, store);
    context.limits = SyncLimits {
        max_items: 1,
        depth_days: Some(30),
    };

    let first = run_sync(&SlackProvider, &context, SyncReason::Manual)
        .await
        .unwrap();
    assert_eq!(first.batch.records.len(), 1);
    assert!(
        !first.batch.complete,
        "a run stopped by the limit has more to give"
    );

    let second = run_sync(&SlackProvider, &context, SyncReason::Scheduled)
        .await
        .unwrap();
    assert_eq!(second.batch.records.len(), 1);
    assert_ne!(
        second.batch.records[0].item_id, first.batch.records[0].item_id,
        "the resumed run must not re-read the message the first one took"
    );
}

#[tokio::test]
async fn a_second_full_walk_re_reads_as_skips_rather_than_duplicates() {
    // A completed walk clears the cursor, so the next one starts at the top.
    // The seen-set is what stops that from filling memory with copies.
    let actions = Arc::new(ScriptedActions::default());
    for _ in 0..2 {
        actions.queue(
            "SLACK_LIST_CONVERSATIONS",
            Ok(channels(&[("C1", "eng"), ("C2", "ops")], "")),
        );
    }
    for _ in 0..2 {
        actions.queue(
            "SLACK_FETCH_CONVERSATION_HISTORY",
            Ok(history(&[message("1700000001.000100", "one")], "")),
        );
        actions.queue(
            "SLACK_FETCH_CONVERSATION_HISTORY",
            Ok(history(&[message("1700000002.000100", "two")], "")),
        );
    }

    let store = Arc::new(MemoryStore::default());
    let context = context(actions, store);

    let first = run_sync(&SlackProvider, &context, SyncReason::InitialConnect)
        .await
        .unwrap();
    assert_eq!(first.batch.records.len(), 2);
    assert!(first.batch.complete, "the whole workspace was walked");

    let second = run_sync(&SlackProvider, &context, SyncReason::Scheduled)
        .await
        .unwrap();
    assert!(second.batch.records.is_empty(), "nothing new to ingest");
    assert_eq!(
        second.records_skipped, 2,
        "both were recognised, not re-read"
    );
}

#[test]
fn slack_re_syncs_on_the_interval_its_host_has_always_advertised() {
    // Fifteen minutes. The host's own table has named this for `slack` since
    // long before there was a provider to honour it.
    assert_eq!(SlackProvider.sync_interval_secs(), Some(900));
}
