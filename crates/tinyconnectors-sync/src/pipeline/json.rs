//! Reading values out of a provider's JSON.
//!
//! Composio wraps provider payloads inconsistently — sometimes `data`,
//! sometimes `data.data`, sometimes neither — so every pipeline needs to try
//! several shapes for one field. These do that once.

use serde_json::Value;

/// The first non-empty scalar at any of `paths`.
///
/// Paths are dotted and resolve through JSON Pointer, so a numeric segment
/// indexes an array: `messages.0.id` is the first message's id.
///
/// Numbers are coerced to their string form, because provider ids are
/// inconsistently typed — the same field arrives as `"123"` from one endpoint
/// and `123` from another, and a caller building a record id cannot care which.
#[must_use]
pub fn pick_str(value: &Value, paths: &[&str]) -> Option<String> {
    paths.iter().find_map(|path| {
        let pointer = format!("/{}", path.replace('.', "/"));
        value
            .pointer(&pointer)
            .and_then(|found| match found {
                Value::String(text) => Some(text.clone()),
                Value::Number(number) => Some(number.to_string()),
                _ => None,
            })
            .map(|text| text.trim().to_owned())
            .filter(|text| !text.is_empty())
    })
}

/// The first array at any of `pointers`, or empty.
///
/// Pointers are JSON Pointer syntax (`/data/messages`), not dotted paths — the
/// callers of this are matching envelope shapes, where the leading slash is
/// what makes the nesting readable.
#[must_use]
pub fn first_array(value: &Value, pointers: &[&str]) -> Vec<Value> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_array))
        .cloned()
        .unwrap_or_default()
}

/// A Google-style `nextPageToken` from any of the envelopes Composio uses.
///
/// Empty tokens are dropped rather than returned: an empty string is how
/// several providers say "no more pages", and treating it as a cursor makes the
/// next request ask for a page that does not exist — forever.
#[must_use]
pub fn next_page_token(value: &Value) -> Option<String> {
    token_at(value, PAGE_TOKEN_POINTERS)
}

/// Where a Google-style `nextPageToken` sits, in every envelope Composio uses.
pub(crate) const PAGE_TOKEN_POINTERS: &[&str] = &[
    "/data/nextPageToken",
    "/nextPageToken",
    "/data/data/nextPageToken",
    "/data/next_page_token",
    "/next_page_token",
];

/// The first string at any of `pointers`, as a page token or cursor.
///
/// An empty or blank string is no token, for the reason [`next_page_token`]
/// gives.
#[must_use]
pub(crate) fn token_at(value: &Value, pointers: &[&str]) -> Option<String> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_str))
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .map(str::to_owned)
}

/// A GraphQL connection's next cursor: `endCursor` from the first `pageInfo`
/// at any of `pointers`, and only while its `hasNextPage` is true.
///
/// A connection reports `endCursor` on its last page too. Following it asks
/// for a page after the end.
#[must_use]
pub(crate) fn end_cursor(value: &Value, pointers: &[&str]) -> Option<String> {
    let page_info = pointers.iter().find_map(|pointer| value.pointer(pointer))?;
    if page_info.get("hasNextPage").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    page_info
        .get("endCursor")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|cursor| !cursor.is_empty())
        .map(str::to_owned)
}

/// The first boolean at any of `pointers`, such as a payload's `last_page`.
#[must_use]
pub(crate) fn flag_at(value: &Value, pointers: &[&str]) -> Option<bool> {
    pointers
        .iter()
        .find_map(|pointer| value.pointer(pointer).and_then(Value::as_bool))
}
