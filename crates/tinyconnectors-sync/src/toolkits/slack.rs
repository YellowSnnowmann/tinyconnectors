//! The Slack provider.
//!
//! # Why this one is not a [`PageSpec`](crate::pipeline::PageSpec)
//!
//! Every other toolkit here is *flat*: one action reads the whole account, one
//! cursor says where it got to, and the provider is a declaration. Slack is
//! *scoped*. `SLACK_FETCH_CONVERSATION_HISTORY` reads one channel and requires
//! its id, so a sync has to walk the conversation list and page each channel in
//! turn, remembering a position per channel. No single action reads a
//! workspace.
//!
//! It is scoped twice over. A channel's history carries only the top of each
//! conversation; the replies under a message are a second read, against a
//! second action, with a cursor of their own. Ingesting the question and not
//! the answer is the failure mode a Slack memory has to avoid, so a walk
//! detours into a message's thread before it reads the next page of history.
//!
//! # How that fits a pipeline built for one cursor
//!
//! [`run_sync`](crate::pipeline::run_sync) never looks inside a cursor — it
//! stores the string a page reports and hands it back on the next call. So the
//! three-level walk fits without changing the pipeline at all: the cursor
//! carries `"<index>|<history>|<thread>|<thread cursor>"`, and each call
//! returns exactly one page of one channel or one thread.
//!
//! That is what keeps the rule in [`ConnectorProvider::fetch_page`] intact. The
//! item limit, the daily request budget and the already-ingested set stay where
//! they are, in the run loop, instead of being re-implemented here so that one
//! toolkit could loop internally.
//!
//! # What lives beside the cursor, and why it is not in it
//!
//! The channel roster, the queue of threads owed by the channel being read, the
//! per-channel high-water marks and the user directory are kept in this
//! provider's own key in the host's state store, not encoded in the cursor — a
//! cursor is a position, and a roster of two hundred channels is not one.
//!
//! It has to be a **different key** from the one
//! [`SyncState`](crate::state::SyncState) uses. The run loop loads that state
//! when a run starts and writes it back when the run ends, so anything this
//! provider wrote to the same key mid-run would be overwritten by a copy that
//! predates it.
//!
//! # Reading a channel twice is cheaper than reading it from the start
//!
//! A completed walk clears the cursor, so the next run begins again at the
//! first channel. Without a lower bound that would re-read every channel back
//! to the depth boundary on every scheduled run and discard all of it as
//! already seen. Each channel therefore records the newest message the walk
//! has finished reading, and later runs ask Slack only for what is newer.
//!
//! The mark advances when a channel is **exhausted**, never when a page of it
//! is read: a walk stopped half way through a channel by the item limit must
//! resume where it stopped, and a mark moved early would step over everything
//! below it. A channel is not exhausted until the threads it owes are drained,
//! so a reply is never stranded above the mark.
//!
//! # What this deliberately does not read
//!
//! Direct messages and group DMs. The conversation list asks for
//! `public_channel,private_channel` only. A Slack connection is granted for a
//! workspace, and quietly ingesting someone's private correspondence into a
//! memory the agent quotes from is not what connecting a workspace asks for.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::{TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tinyconnectors_bus::records::ConnectorRecord;

use super::identity::pick;
use super::slack_catalog::CURATED;
use crate::pipeline::{ProviderPage, first_array, pick_str};
use crate::provider::{ConnectorProvider, ProviderContext, ProviderUserProfile};
use crate::scope::CuratedTool;
use crate::state::STATE_NAMESPACE;
use crate::{Error, Result};

/// Lists the conversations the connected account can read.
const ACTION_CONVERSATIONS: &str = "SLACK_LIST_CONVERSATIONS";
/// Reads one page of one conversation.
const ACTION_HISTORY: &str = "SLACK_FETCH_CONVERSATION_HISTORY";
/// Reads one page of the replies under one message.
const ACTION_THREAD: &str = "SLACK_FETCH_MESSAGE_THREAD_FROM_A_CONVERSATION";
/// Reads the workspace the connection belongs to.
const PROFILE_ACTION: &str = "SLACK_FETCH_TEAM_INFO";
/// Reads the directory used to turn `<@U…>` into a name.
const ACTION_USERS: &str = "SLACK_LIST_ALL_USERS";

/// Conversations asked for per roster page.
///
/// Slack's own maximum for this call. The roster is walked one page at a time
/// like everything else, so a large workspace costs one extra request per two
/// hundred channels rather than a loop inside a single page read.
const CHANNELS_PER_PAGE: usize = 200;

/// Ceiling on messages asked for in one history or thread page.
///
/// The run's own item limit is the real bound and is almost always smaller;
/// this only stops an unbounded limit from asking Slack for more than it will
/// return anyway.
const MESSAGES_PER_PAGE: usize = 200;

/// Directory entries asked for per page.
const USERS_PER_PAGE: usize = 200;

/// Most directory pages read in one walk.
///
/// The directory is a nicety — it turns `<@U04AB>` into a name — so it is
/// bounded rather than walked to the end. A workspace larger than
/// `USERS_PER_PAGE * USER_PAGES_MAX` people resolves the first thousand and
/// leaves the rest as ids, which reads worse but costs nothing else. Spending
/// an unbounded number of requests on it before a single message is read would
/// be the wrong trade.
const USER_PAGES_MAX: usize = 5;

/// Longest message body kept, in characters.
///
/// Matches the cap the shared page reader applies. A Slack message is rarely
/// anywhere near it; a pasted stack trace is, and one such record can outweigh
/// a hundred useful ones in both storage and the attention of anything reading
/// them back.
const MAX_BODY_CHARS: usize = 20_000;

/// How long a cached user directory is trusted, in milliseconds.
///
/// A day. Display names change rarely, and the cost of a stale one is a name
/// that reads slightly wrong in a message body — far below the cost of another
/// directory fetch at the head of every walk.
const USERS_TTL_MS: i64 = 24 * 60 * 60 * 1000;

/// Slack error codes that mean no later channel will succeed either.
///
/// Everything else — a channel the account is not in, an archived conversation,
/// a rate limit on one busy channel — is per-channel and must not fail the
/// workspace. These are the connection-level ones, where continuing would spend
/// a request per channel to collect the same failure.
const FATAL_AUTH_ERRORS: &[&str] = &[
    "invalid_auth",
    "not_authed",
    "token_revoked",
    "token_expired",
    "account_inactive",
    "missing_scope",
];

/// Where the walk is: which channel, where in its history, and which of its
/// threads is being drained.
///
/// Encoded as `"<index>|<history>|<thread>|<thread cursor>"`. No field can
/// contain a `|`: the index is a number, a Slack `ts` is digits and a dot, and
/// Slack's cursors are base64, whose alphabet does not include one.
///
/// `history` keeps its meaning while a thread is being read — it is where the
/// channel resumes once the detour ends.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Cursor {
    /// Position in the roster page held in [`WalkState::channels`].
    index: usize,
    /// Slack's own position inside that channel's history, if part way in.
    history: Option<String>,
    /// The message whose replies are being read, if the walk is in a thread.
    thread: Option<String>,
    /// Slack's position inside those replies, if part way in.
    reply_page: Option<String>,
}

impl Cursor {
    /// A cursor naming `index`, at `history` within it, reading no thread.
    fn new(index: usize, history: Option<String>) -> Self {
        Self {
            index,
            history,
            thread: None,
            reply_page: None,
        }
    }

    /// A cursor part way through the replies under `thread`.
    fn in_thread(
        index: usize,
        history: Option<String>,
        thread: String,
        reply_page: Option<String>,
    ) -> Self {
        Self {
            index,
            history,
            thread: Some(thread),
            reply_page,
        }
    }

    /// Read a cursor the pipeline handed back.
    ///
    /// A cursor that does not parse restarts the walk rather than failing it:
    /// the seen-set turns the re-read into skips, so the cost of being wrong
    /// here is requests, and the cost of erroring would be a connection that
    /// never syncs again. A cursor written by an older release has fewer
    /// fields and reads as "not in a thread", which is exactly right.
    fn decode(raw: Option<&str>) -> Self {
        let Some(raw) = raw else {
            return Self::default();
        };
        let mut fields = raw.splitn(4, '|');
        let index = fields.next().unwrap_or_default().parse().unwrap_or(0);
        let owned = |field: Option<&str>| {
            field
                .filter(|value| !value.is_empty())
                .map(std::string::ToString::to_string)
        };
        Self {
            index,
            history: owned(fields.next()),
            thread: owned(fields.next()),
            reply_page: owned(fields.next()),
        }
    }

    /// Render this position for the pipeline to store.
    fn encode(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.index,
            self.history.as_deref().unwrap_or(""),
            self.thread.as_deref().unwrap_or(""),
            self.reply_page.as_deref().unwrap_or("")
        )
    }
}

/// One conversation the walk knows about.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Channel {
    /// Slack channel id, e.g. `C0123ABCD`.
    id: String,
    /// Channel name without the leading `#`.
    #[serde(default)]
    name: String,
}

/// What the walk remembers between pages and between runs.
///
/// Persisted under this provider's own key — see the module docs for why it
/// cannot share the run loop's.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct WalkState {
    /// The roster page being walked.
    #[serde(default)]
    channels: Vec<Channel>,
    /// Where the next roster page starts, when there is one.
    #[serde(default)]
    channels_cursor: Option<String>,
    /// Messages in the channel being read whose replies are still owed.
    #[serde(default)]
    threads: Vec<String>,
    /// Newest message id fully read per channel, `channel id → Slack ts`.
    #[serde(default)]
    high_water: BTreeMap<String, String>,
    /// Newest message id *seen* per channel, promoted once the channel ends.
    #[serde(default)]
    pending: BTreeMap<String, String>,
    /// `user id → display name`, for resolving mentions and authors.
    #[serde(default)]
    users: BTreeMap<String, String>,
    /// When the directory was read, in milliseconds since the Unix epoch.
    #[serde(default)]
    users_fetched_ms: i64,
}

impl WalkState {
    /// The store key holding one connection's walk.
    ///
    /// Deliberately unlike [`crate::state::SyncState::key`], which is
    /// `"<toolkit>:<connection>"`.
    fn key(connection_id: &str) -> String {
        format!("slack-walk:{connection_id}")
    }

    /// Load this connection's walk, or an empty one.
    async fn load(context: &ProviderContext) -> Result<Self> {
        let key = Self::key(&context.connection_id);
        let Some(value) = context.state.get(STATE_NAMESPACE, &key).await? else {
            return Ok(Self::default());
        };
        // A shape that no longer decodes restarts the walk instead of failing
        // it, for the same reason a bad cursor does.
        Ok(serde_json::from_value(value).unwrap_or_default())
    }

    /// Persist this walk.
    async fn save(&self, context: &ProviderContext) -> Result<()> {
        let key = Self::key(&context.connection_id);
        let value = serde_json::to_value(self).map_err(|error| Error::Decode {
            key: key.clone(),
            message: error.to_string(),
        })?;
        context.state.set(STATE_NAMESPACE, &key, &value).await
    }

    /// The channel at `index` of the roster page in hand.
    fn channel_at(&self, index: usize) -> Option<&Channel> {
        self.channels.get(index)
    }

    /// Where to start reading `channel`, honouring the run's depth bound.
    ///
    /// The high-water mark wins when there is one: it is never older than the
    /// bound, having been set by a run that already respected it.
    fn oldest_for(&self, channel: &str, depth_days: Option<u32>) -> Option<String> {
        if let Some(mark) = self.high_water.get(channel) {
            return Some(mark.clone());
        }
        depth_days.map(|days| {
            let since = Utc::now() - TimeDelta::days(i64::from(days));
            format!("{}.000000", since.timestamp())
        })
    }

    /// Promote the pending mark for a channel that has just been exhausted.
    fn commit_high_water(&mut self, channel: &str) {
        if let Some(newest) = self.pending.remove(channel) {
            self.high_water.insert(channel.to_string(), newest);
        }
    }

    /// Whether the user directory needs re-reading.
    fn users_are_stale(&self, now_ms: i64) -> bool {
        self.users.is_empty() || now_ms - self.users_fetched_ms > USERS_TTL_MS
    }
}

/// One page read, and whether it changed anything worth persisting.
struct Read {
    /// What to hand back to the run loop.
    page: ProviderPage,
    /// Whether [`WalkState`] moved and needs saving.
    changed: bool,
}

/// Slack as a connector toolkit.
#[derive(Debug, Default, Clone, Copy)]
pub struct SlackProvider;

impl SlackProvider {
    /// Read one roster page, replacing the one in hand.
    async fn load_channel_page(
        &self,
        context: &ProviderContext,
        walk: &mut WalkState,
        cursor: Option<&str>,
    ) -> Result<()> {
        let mut arguments = json!({
            "limit": CHANNELS_PER_PAGE,
            // Channels only. See the module docs: DMs are deliberately not read.
            "types": "public_channel,private_channel",
            "exclude_archived": true,
        });
        if let Some(cursor) = cursor {
            arguments["cursor"] = Value::String(cursor.to_string());
        }

        let payload = context.run(ACTION_CONVERSATIONS, arguments).await?;
        walk.channels = first_array(
            &payload,
            &["/data/channels", "/channels", "/data/data/channels"],
        )
        .iter()
        .filter_map(|channel| {
            let id = pick_str(channel, &["id", "data.id"])?;
            let name = pick_str(channel, &["name", "data.name"]).unwrap_or_else(|| id.clone());
            Some(Channel { id, name })
        })
        .collect();
        // Slack handing back the cursor it was just given would walk the same
        // page for ever: the run loop stops on records or on the daily budget,
        // and a roster page that yields no channels produces neither. Refusing
        // a cursor that has not moved ends the walk instead of spending the
        // budget discovering the same nothing.
        walk.channels_cursor = next_cursor(&payload).filter(|next| Some(next.as_str()) != cursor);
        Ok(())
    }

    /// Refresh the user directory, if it is missing or old.
    ///
    /// A failure here is not a failure of the walk: mentions render as raw ids,
    /// which is worse to read and no reason to ingest nothing. Bounded by
    /// [`USER_PAGES_MAX`] — see there for why it does not read to the end.
    async fn refresh_users(&self, context: &ProviderContext, walk: &mut WalkState) -> bool {
        let now_ms = Utc::now().timestamp_millis();
        if !walk.users_are_stale(now_ms) {
            return false;
        }

        let mut directory = BTreeMap::new();
        let mut cursor: Option<String> = None;
        for _ in 0..USER_PAGES_MAX {
            let mut arguments = json!({ "limit": USERS_PER_PAGE });
            if let Some(cursor) = cursor.as_deref() {
                arguments["cursor"] = Value::String(cursor.to_string());
            }
            let Ok(payload) = context.run(ACTION_USERS, arguments).await else {
                tracing::debug!("[connectors][slack] user directory unavailable; ids stay raw");
                break;
            };
            for user in &first_array(
                &payload,
                &["/data/members", "/members", "/data/data/members"],
            ) {
                if let Some(id) = pick_str(user, &["id"])
                    && let Some(name) =
                        pick_str(user, &["profile.display_name", "real_name", "name"])
                {
                    directory.insert(id, name);
                }
            }
            cursor = next_cursor(&payload).filter(|next| Some(next.as_str()) != cursor.as_deref());
            if cursor.is_none() {
                break;
            }
        }

        if directory.is_empty() {
            return false;
        }
        walk.users = directory;
        walk.users_fetched_ms = now_ms;
        true
    }

    /// Move to the next roster page, or report the workspace walked.
    async fn advance_roster(
        &self,
        context: &ProviderContext,
        walk: &mut WalkState,
    ) -> Result<Read> {
        let Some(next) = walk.channels_cursor.clone() else {
            return Ok(Read {
                page: empty_page(None),
                changed: false,
            });
        };
        self.load_channel_page(context, walk, Some(&next)).await?;
        Ok(Read {
            page: empty_page(Some(Cursor::new(0, None).encode())),
            changed: true,
        })
    }

    /// Read one page of the replies under the message the cursor names.
    async fn read_thread(
        &self,
        context: &ProviderContext,
        walk: &mut WalkState,
        position: &Cursor,
        channel: &Channel,
        parent: &str,
    ) -> Result<Read> {
        let mut arguments = json!({
            "channel": channel.id,
            "ts": parent,
            "limit": MESSAGES_PER_PAGE,
        });
        if let Some(cursor) = position.reply_page.as_deref() {
            arguments["cursor"] = Value::String(cursor.to_string());
        }

        let payload = match context.run(ACTION_THREAD, arguments).await {
            Ok(payload) => payload,
            Err(error) if is_fatal(&error) => return Err(error),
            Err(error) => {
                // A thread that cannot be read costs its replies, not the run.
                tracing::debug!(
                    channel = %channel.id,
                    thread = %parent,
                    error = %error,
                    "[connectors][slack] thread skipped"
                );
                return Ok(after_thread(walk, position, channel, Vec::new(), None));
            }
        };

        let replies = messages_in(&payload);
        // The parent is repeated as the first reply. It was ingested with the
        // history page that found it, and the run loop would skip it anyway;
        // dropping it here saves the round trip through the seen-set.
        let (records, versions) = records_from(&replies, channel, &walk.users, Some(parent));
        let more = next_cursor(&payload);
        let mut read = after_thread(walk, position, channel, records, more);
        read.page.versions = versions;
        Ok(read)
    }

    /// Read one page of the channel the cursor names.
    async fn read_history(
        &self,
        context: &ProviderContext,
        walk: &mut WalkState,
        position: &Cursor,
        channel: &Channel,
    ) -> Result<Read> {
        let mut arguments = json!({
            "channel": channel.id,
            "inclusive": false,
            "limit": context.limits.max_items.clamp(1, MESSAGES_PER_PAGE),
        });
        if let Some(oldest) = walk.oldest_for(&channel.id, context.limits.depth_days) {
            arguments["oldest"] = Value::String(oldest);
        }
        if let Some(history) = position.history.as_deref() {
            arguments["cursor"] = Value::String(history.to_string());
        }

        let payload = match context.run(ACTION_HISTORY, arguments).await {
            Ok(payload) => payload,
            Err(error) if is_fatal(&error) => return Err(error),
            Err(error) => {
                // One channel the account cannot read is ordinary — archived,
                // not a member, rate-limited. Skipping it costs that channel;
                // failing here would cost the workspace.
                tracing::debug!(
                    channel = %channel.id,
                    error = %error,
                    "[connectors][slack] channel skipped"
                );
                walk.threads.clear();
                return Ok(Read {
                    page: empty_page(Some(Cursor::new(position.index + 1, None).encode())),
                    changed: true,
                });
            }
        };

        let messages = messages_in(&payload);

        // Entering a channel: nothing earlier owes it replies, and a queue left
        // behind by an interrupted walk of another channel must not be drained
        // against this one.
        if position.history.is_none() {
            walk.threads.clear();
            // Slack returns a channel newest-first, so the newest message of
            // the whole channel is on its first page. Remember it now and
            // promote it only when the channel ends — see the module docs.
            if let Some(newest) = messages.iter().find_map(|m| pick_str(m, &["ts"])) {
                walk.pending.insert(channel.id.clone(), newest);
            }
        }

        // Queue the conversations under this page before reading further: they
        // belong to messages this page just ingested. Pushed back-to-front
        // because the queue is drained from its end, which makes the threads
        // come back in the order Slack listed their parents.
        walk.threads
            .extend(messages.iter().rev().filter_map(has_replies));

        let (records, versions) = records_from(&messages, channel, &walk.users, None);
        let history = next_cursor(&payload);
        let next = if let Some(parent) = walk.threads.pop() {
            Cursor::in_thread(position.index, history, parent, None)
        } else if history.is_some() {
            Cursor::new(position.index, history)
        } else {
            // Nothing more in this channel and nothing owed: promote the mark
            // it has been collecting, and move on.
            walk.commit_high_water(&channel.id);
            Cursor::new(position.index + 1, None)
        };

        Ok(Read {
            page: ProviderPage {
                records,
                versions,
                next_cursor: Some(next.encode()),
            },
            changed: true,
        })
    }
}

#[async_trait]
impl ConnectorProvider for SlackProvider {
    fn toolkit_slug(&self) -> &'static str {
        "slack"
    }

    fn description(&self) -> &'static str {
        "Read and send Slack messages, and ingest channel history as memory."
    }

    fn curated_tools(&self) -> Option<&'static [CuratedTool]> {
        Some(CURATED)
    }

    fn sync_interval_secs(&self) -> Option<u64> {
        Some(900)
    }

    async fn fetch_user_profile(&self, context: &ProviderContext) -> Result<ProviderUserProfile> {
        let payload = context.run(PROFILE_ACTION, json!({})).await?;
        Ok(ProviderUserProfile {
            toolkit: self.toolkit_slug().to_string(),
            connection_id: Some(context.connection_id.clone()),
            // The workspace is what a Slack connection is *of*; a UI labelling
            // an account has nothing better to show.
            display_name: pick(&payload, &["team.name", "name", "data.team.name"]),
            username: pick(&payload, &["team.domain", "domain", "data.team.domain"]),
            avatar_url: pick(&payload, &["team.icon.image_132", "icon.image_132"]),
            extras: payload,
            ..ProviderUserProfile::default()
        })
    }

    /// Read one page of one channel, or of one thread, advancing the walk.
    ///
    /// Every return either moves the cursor forward or reports the walk is
    /// over. That is what keeps the run loop from spinning: it stops only on a
    /// limit or on `None`, so a page that reported the position it was given
    /// would be asked for again forever.
    async fn fetch_page(
        &self,
        context: &ProviderContext,
        cursor: Option<&str>,
    ) -> Result<ProviderPage> {
        let position = Cursor::decode(cursor);
        let mut walk = WalkState::load(context).await?;
        let mut changed = false;

        // A run with no cursor is a walk starting: re-read the roster from the
        // top, and refresh the directory it renders names with.
        if cursor.is_none() {
            self.load_channel_page(context, &mut walk, None).await?;
            self.refresh_users(context, &mut walk).await;
            changed = true;
        }

        let read = match walk.channel_at(position.index).cloned() {
            None => self.advance_roster(context, &mut walk).await?,
            Some(channel) => match position.thread.clone() {
                Some(parent) => {
                    self.read_thread(context, &mut walk, &position, &channel, &parent)
                        .await?
                }
                None => {
                    self.read_history(context, &mut walk, &position, &channel)
                        .await?
                }
            },
        };

        if changed || read.changed {
            walk.save(context).await?;
        }
        Ok(read.page)
    }
}

/// Where the walk goes once a thread page has been read.
fn after_thread(
    walk: &mut WalkState,
    position: &Cursor,
    channel: &Channel,
    records: Vec<ConnectorRecord>,
    more: Option<String>,
) -> Read {
    let history = position.history.clone();
    let next = if let Some(more) = more {
        // Same thread, further in.
        position
            .thread
            .clone()
            .map(|parent| Cursor::in_thread(position.index, history, parent, Some(more)))
    } else if let Some(parent) = walk.threads.pop() {
        // This channel still owes replies elsewhere.
        Some(Cursor::in_thread(position.index, history, parent, None))
    } else if history.is_some() {
        // Threads drained; carry on where the history left off.
        Some(Cursor::new(position.index, history))
    } else {
        // Nothing left in this channel, replies included: the mark is safe.
        walk.commit_high_water(&channel.id);
        Some(Cursor::new(position.index + 1, None))
    };

    Read {
        page: ProviderPage {
            records,
            versions: Vec::new(),
            next_cursor: next.map(|cursor| cursor.encode()),
        },
        changed: true,
    }
}

/// A page carrying nothing, resuming at `next`.
fn empty_page(next: Option<String>) -> ProviderPage {
    ProviderPage {
        records: Vec::new(),
        versions: Vec::new(),
        next_cursor: next,
    }
}

/// The messages in a history or thread payload, across Composio's envelopes.
fn messages_in(payload: &Value) -> Vec<Value> {
    first_array(
        payload,
        &["/data/messages", "/messages", "/data/data/messages"],
    )
}

/// The timestamp of a message that has replies, if it has any.
fn has_replies(message: &Value) -> Option<String> {
    let replies = message
        .get("reply_count")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    (replies > 0).then(|| pick_str(message, &["ts"]))?
}

/// Whether a failure means the whole connection is unusable.
fn is_fatal(error: &Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    FATAL_AUTH_ERRORS.iter().any(|code| message.contains(code))
}

/// Slack's next page marker, across the envelopes Composio wraps it in.
///
/// Not [`crate::pipeline::next_page_token`]: that looks for `nextPageToken`,
/// which is Google's spelling. Slack reports `response_metadata.next_cursor`.
fn next_cursor(payload: &Value) -> Option<String> {
    [
        "/data/response_metadata/next_cursor",
        "/response_metadata/next_cursor",
        "/data/data/response_metadata/next_cursor",
        "/data/next_cursor",
        "/next_cursor",
    ]
    .iter()
    .find_map(|pointer| payload.pointer(pointer).and_then(Value::as_str))
    .map(str::trim)
    .filter(|cursor| !cursor.is_empty())
    .map(str::to_owned)
}

/// Turn a page of messages into records and the versions they report.
///
/// `skip` names a timestamp to leave out — the parent Slack repeats at the head
/// of its own thread.
fn records_from(
    messages: &[Value],
    channel: &Channel,
    users: &BTreeMap<String, String>,
    skip: Option<&str>,
) -> (Vec<ConnectorRecord>, Vec<(String, String)>) {
    let mut records = Vec::with_capacity(messages.len());
    let mut versions = Vec::new();
    for message in messages {
        if let Some(skip) = skip
            && pick_str(message, &["ts"]).as_deref() == Some(skip)
        {
            continue;
        }
        let Some(record) = record_from(message, channel, users, skip.is_some()) else {
            continue;
        };
        // An edited message re-ingests; an untouched one does not. Slack
        // reports the edit as its own timestamp, which is exactly a version.
        if let Some(edited) = pick_str(message, &["edited.ts"]) {
            versions.push((record.item_id.clone(), edited));
        }
        records.push(record);
    }
    (records, versions)
}

/// One message as a record, or `None` when there is nothing to ingest.
///
/// A message with no timestamp has no stable id, and one with no text is a
/// join notice or a file share whose body is elsewhere. Both would fill a
/// user's memory with rows that say nothing.
fn record_from(
    message: &Value,
    channel: &Channel,
    users: &BTreeMap<String, String>,
    reply: bool,
) -> Option<ConnectorRecord> {
    let ts = pick_str(message, &["ts"])?;
    let text = pick_str(message, &["text"])?;
    let rendered = render(&text, users);
    if rendered.trim().is_empty() {
        return None;
    }

    // An id the directory cannot resolve is still worth keeping: a reader can
    // look `U04AB` up, and collapsing every unresolved author into one word
    // would make messages from different people indistinguishable. This is the
    // same fallback `render` applies to a mention.
    let author = pick_str(message, &["user"])
        .map(|id| users.get(&id).cloned().unwrap_or(id))
        .or_else(|| pick_str(message, &["username", "bot_id"]))
        .unwrap_or_else(|| "unknown".to_string());
    let kind = if reply { " (reply)" } else { "" };

    Some(ConnectorRecord {
        // Channel-qualified: a Slack `ts` is unique within a channel, and the
        // run loop dedupes against one flat set for the whole connection.
        item_id: format!("{}:{ts}", channel.id),
        title: format!("#{} — {author}{kind}", channel.name),
        content: truncate(&rendered),
        mime: Some("text/plain".to_string()),
        url: pick_str(message, &["permalink"]).or_else(|| Some(permalink(&channel.id, &ts))),
        updated_at_ms: to_millis(&ts),
        tags: Vec::new(),
    })
}

/// A link back to one message.
///
/// `slack.com` rather than the workspace's own domain: the workspace is not
/// known here, and Slack redirects this form to the right one for whoever
/// opens it.
fn permalink(channel: &str, ts: &str) -> String {
    format!(
        "https://slack.com/archives/{channel}/p{}",
        ts.replace('.', "")
    )
}

/// Slack's `seconds.microseconds` stamp in milliseconds.
fn to_millis(ts: &str) -> Option<i64> {
    let (seconds, fraction) = ts.split_once('.').unwrap_or((ts, "0"));
    let seconds: i64 = seconds.parse().ok()?;
    // Three digits of the fraction are milliseconds; a shorter one is padded
    // rather than misread as a smaller number.
    let millis: i64 = format!("{fraction:0<3}")
        .chars()
        .take(3)
        .collect::<String>()
        .parse()
        .unwrap_or(0);
    Some(seconds * 1000 + millis)
}

/// Message text with Slack's reference syntax resolved.
///
/// `<@U04AB>` is a user, `<#C07XY|deploys>` a channel, `<https://…|label>` a
/// link. Left raw they are the bulk of what makes ingested Slack unreadable —
/// and an unresolved id is not something a reader can look up later.
fn render(raw: &str, users: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut rest = raw;

    while let Some(start) = rest.find('<') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let Some(end) = after.find('>') else {
            // An unmatched `<` is literal text, not a broken reference.
            out.push_str(&rest[start..]);
            return out;
        };
        let (token, remainder) = after.split_at(end);
        out.push_str(&resolve(token, users));
        rest = &remainder[1..];
    }
    out.push_str(rest);
    out
}

/// One `<…>` reference, without its angle brackets.
fn resolve(token: &str, users: &BTreeMap<String, String>) -> String {
    let (target, label) = token.split_once('|').unwrap_or((token, ""));
    match target.as_bytes().first() {
        // A user: the directory's name, the inline label, or the bare id.
        Some(b'@') => {
            let id = &target[1..];
            let name = users
                .get(id)
                .map(String::as_str)
                .or(Some(label).filter(|label| !label.is_empty()))
                .unwrap_or(id);
            format!("@{name}")
        }
        // A channel: Slack usually inlines the name, so prefer it.
        Some(b'#') => {
            let id = &target[1..];
            let name = if label.is_empty() { id } else { label };
            format!("#{name}")
        }
        // A link: the label if it has one, else the target itself.
        _ if label.is_empty() => target.to_string(),
        _ => label.to_string(),
    }
}

/// Cap a body at [`MAX_BODY_CHARS`], on a character boundary.
fn truncate(text: &str) -> String {
    let text = text.trim();
    if text.chars().count() <= MAX_BODY_CHARS {
        return text.to_string();
    }
    text.chars().take(MAX_BODY_CHARS).collect::<String>()
}

#[cfg(test)]
#[path = "slack_test.rs"]
mod test;
