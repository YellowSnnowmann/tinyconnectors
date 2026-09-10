//! Tests for Slack payload parsing and rendering.
//!
//! Payloads rather than call sequences: what a page *said* is a different
//! question from where the walk goes next, and it is answerable without a walk
//! at all.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;

use super::{render, to_millis};

#[test]
fn references_resolve_to_something_a_person_can_read() {
    let mut users = BTreeMap::new();
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
    let users = BTreeMap::new();
    assert_eq!(render("ping <@U404>", &users), "ping @U404");
    assert_eq!(render("ping <@U404|ada>", &users), "ping @ada");
    // An unmatched bracket is text, not a broken reference.
    assert_eq!(render("2 < 3 and more", &users), "2 < 3 and more");
}

#[test]
fn a_channel_reference_without_a_label_keeps_its_id() {
    let users = BTreeMap::new();
    assert_eq!(render("see <#C7>", &users), "see #C7");
}

#[test]
fn a_slack_timestamp_becomes_milliseconds() {
    assert_eq!(to_millis("1700000000.000100"), Some(1_700_000_000_000));
    assert_eq!(to_millis("1700000000.123456"), Some(1_700_000_000_123));
    // A short fraction is padded, not read as a smaller number.
    assert_eq!(to_millis("1700000000.5"), Some(1_700_000_000_500));
    assert_eq!(to_millis("nonsense"), None);
}
