//! Slack file shares as records.
//!
//! Split from `slack_parse.rs` because a file is not a message. The message
//! half asks what someone *said*; this asks what they *attached*, and the two
//! have different ids, different bodies and different reasons to exist.
//!
//! Metadata only. The record names the file, links to it, and carries whatever
//! text Slack already handed over in `preview` — it does not fetch the bytes.
//! Reading those would need a curated file-read action, which the catalog does
//! not have, plus the `files:read` scope, which every existing connection would
//! have to be re-authorised to gain. The name and the link are what turn "my
//! screenshot vanished" into something a reader can follow.

use std::collections::BTreeMap;

use serde_json::Value;
use tinyconnectors_bus::records::ConnectorRecord;

use super::slack::Channel;
use super::slack_parse::{author_of, to_millis, truncate};
use crate::pipeline::pick_str;

/// One record per file attached to `message`.
///
/// Empty for the overwhelming majority of messages, which carry no files at
/// all, and for a message with no timestamp — without one there is no stable id
/// to hang a file off.
pub(super) fn file_records_from(
    message: &Value,
    channel: &Channel,
    users: &BTreeMap<String, String>,
    reply: bool,
) -> Vec<ConnectorRecord> {
    let Some(ts) = pick_str(message, &["ts"]) else {
        return Vec::new();
    };
    let Some(files) = message.get("files").and_then(Value::as_array) else {
        return Vec::new();
    };

    let author = author_of(message, users);
    let kind = if reply { " (reply)" } else { "" };
    files
        .iter()
        .filter_map(|file| record_for(file, channel, &ts, &author, kind))
        .collect()
}

/// One file as a record, or `None` when it cannot be identified.
///
/// Both the id and the name are required, and for the same reason: the id is
/// the dedupe key, so a file without one would re-ingest as something new on
/// every single run, and a file without a name gives a reader nothing to
/// recognise it by. Slack sends both for real uploads; what arrives without
/// them is a tombstone for a file that has since been deleted.
fn record_for(
    file: &Value,
    channel: &Channel,
    ts: &str,
    author: &str,
    kind: &str,
) -> Option<ConnectorRecord> {
    let id = pick_str(file, &["id"])?;
    let name = pick_str(file, &["name", "title"])?;

    Some(ConnectorRecord {
        // The message timestamp alone is not unique across channels, and one
        // message can carry several files, so the id needs all three parts.
        item_id: format!("{}:{ts}:{id}", channel.id),
        title: format!("#{} — {author} shared {name}{kind}", channel.name),
        content: truncate(&describe(file, &name)),
        // The mime of `content`, which is the text below — not the mime of the
        // file, whose bytes this record does not carry. The file's own type is
        // part of the body instead.
        mime: Some("text/plain".to_string()),
        url: pick_str(file, &["permalink"]),
        // A file uploaded long after the message it hangs off — an edit, a
        // later addition to a thread — is better placed by its own clock. The
        // message timestamp is the fallback, not the first choice.
        updated_at_ms: created_millis(file).or_else(|| to_millis(ts)),
        tags: Vec::new(),
    })
}

/// What the record says about a file.
///
/// The name always, the human title when it differs from the filename, the
/// type, and Slack's own `preview` when there is one — snippets and posts carry
/// their opening lines there, which is real content for free.
fn describe(file: &Value, name: &str) -> String {
    let mut parts = vec![name.to_string()];
    if let Some(title) = pick_str(file, &["title"]).filter(|title| title != name) {
        parts.push(title);
    }
    if let Some(mimetype) = pick_str(file, &["mimetype"]) {
        parts.push(mimetype);
    }
    if let Some(preview) = pick_str(file, &["preview"]) {
        parts.push(preview);
    }
    parts.join("\n")
}

/// Slack's `created`, which is whole seconds, in milliseconds.
fn created_millis(file: &Value) -> Option<i64> {
    to_millis(&pick_str(file, &["created"])?)
}

#[cfg(test)]
#[path = "slack_files_test.rs"]
mod test;
