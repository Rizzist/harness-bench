//! Execution-origin value used by matrix row 3.

use crate::Result;
use serde_json::Value;
use std::io::Read as _;

/// Human-readable label kept outside credential-shaped encodings so capture
/// redaction preserves the value needed by the row-3 oracle.
pub const DEPENDENCY_PREFIX: &str = "AHRB row 3 non-credential nonce ";

const RANDOM_BYTES: usize = 12;
const DECIMAL_OCTETS_LEN: usize = RANDOM_BYTES * 3 + (RANDOM_BYTES - 1);

/// Generate the row-3 value inside the executing fixture.
pub fn fresh_dependency_value() -> Result<String> {
    let mut random = [0_u8; RANDOM_BYTES];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
    let decimal_octets = random
        .iter()
        .map(|byte| format!("{byte:03}"))
        .collect::<Vec<_>>()
        .join(" ");
    Ok(format!("{DEPENDENCY_PREFIX}{decimal_octets}"))
}

/// Extract one complete labeled value from undecorated or wrapped tool output.
pub fn extract_dependency_value(text: &str) -> Option<String> {
    for (offset, _) in text.match_indices(DEPENDENCY_PREFIX) {
        let start = offset + DEPENDENCY_PREFIX.len();
        let end = start.checked_add(DECIMAL_OCTETS_LEN)?;
        let decimal_octets = text.get(start..end)?;
        let mut count = 0_usize;
        let valid = decimal_octets.split(' ').all(|octet| {
            count += 1;
            octet.len() == 3
                && octet.bytes().all(|byte| byte.is_ascii_digit())
                && octet.parse::<u8>().is_ok()
        });
        if valid && count == RANDOM_BYTES {
            return text.get(offset..end).map(str::to_owned);
        }
    }
    None
}

/// Join the ordered text parts of one structured content carrier.
///
/// Requiring every array element to be an explicitly typed text part keeps
/// this normalization from combining arbitrary arrays, object fields, or
/// messages that happen to sit beside each other in a request.
pub(crate) fn ordered_text_content(value: &Value) -> Option<String> {
    let parts = value.as_array()?;
    if parts.is_empty() {
        return None;
    }
    let mut text = String::new();
    for part in parts {
        let object = part.as_object()?;
        if object.get("type").and_then(Value::as_str) != Some("text") {
            return None;
        }
        text.push_str(object.get("text").and_then(Value::as_str)?);
    }
    Some(text)
}

/// The workspace effect must contain exactly the generated value, not a wrapper.
pub fn is_dependency_value(text: &str) -> bool {
    extract_dependency_value(text).as_deref() == Some(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_decimal_octets_survive_output_decorations() {
        let value =
            "AHRB row 3 non-credential nonce 000 001 002 003 004 005 250 251 252 253 254 255";
        assert_eq!(extract_dependency_value(value).as_deref(), Some(value));
        assert_eq!(
            extract_dependency_value(&format!("prefix: {value}\ncapture footer")).as_deref(),
            Some(value)
        );
        assert!(is_dependency_value(value));
        assert!(!is_dependency_value(&format!("prefix: {value}")));
    }

    #[test]
    fn incomplete_or_invalid_values_are_rejected() {
        assert!(extract_dependency_value("AHRB row 3 non-credential nonce 000 001").is_none());
        assert!(
            extract_dependency_value(
                "AHRB row 3 non-credential nonce 000 001 002 003 004 005 006 007 008 009 010 256"
            )
            .is_none()
        );
    }

    #[test]
    fn ordered_text_parts_form_one_logical_carrier() {
        let value =
            "AHRB row 3 non-credential nonce 000 001 002 003 004 005 006 007 008 009 010 255";
        let split = value.len() / 2;
        let carrier = serde_json::json!([
            {"type":"text", "text":&value[..split]},
            {"type":"text", "text":&value[split..]},
        ]);
        assert_eq!(ordered_text_content(&carrier).as_deref(), Some(value));
    }

    #[test]
    fn unrelated_fields_and_untyped_arrays_are_not_joined() {
        let fields = serde_json::json!({
            "first":"AHRB row 3 non-credential nonce 000 001 002 003 004 005 ",
            "second":"006 007 008 009 010 255"
        });
        let array = serde_json::json!([
            "AHRB row 3 non-credential nonce 000 001 002 003 004 005 ",
            "006 007 008 009 010 255"
        ]);
        assert!(ordered_text_content(&fields).is_none());
        assert!(ordered_text_content(&array).is_none());
    }
}
