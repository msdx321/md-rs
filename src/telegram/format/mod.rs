use std::path::{Path, PathBuf};

const BYTE_UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB", "PB"];

pub fn validate_title(title: &str) -> String {
    title
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\n' => '_',
            c => c,
        })
        .collect()
}

pub fn parse_byte_str(s: &str) -> Option<u64> {
    let compact: String = s.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    for (power, unit) in BYTE_UNITS.iter().enumerate().rev() {
        if let Some(num) = compact.strip_suffix(unit) {
            return num
                .parse::<u64>()
                .ok()?
                .checked_mul(1024u64.pow(power as u32));
        }
    }
    None
}

pub fn format_byte(size: f64) -> String {
    if size == 0.0 {
        return "0".to_string();
    }
    if (0.0..1.0).contains(&size) {
        return format!("{:.0}b", size / 0.125);
    }
    let mut value = size;
    for unit in BYTE_UNITS {
        if value < 1024.0 {
            return format!("{:.1}{unit}", value);
        }
        value /= 1024.0;
    }
    format!("{:.1}PB", value)
}

/// Truncate the leaf filename so its UTF-8 byte length ≤ `limit`.
pub fn truncate_filename(path: &Path, limit: usize) -> PathBuf {
    let parent = path.parent().unwrap_or(Path::new(""));
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");

    let ext_bytes = ext.len();
    let dot = if ext.is_empty() { 0 } else { 1 };
    let max_stem = limit.saturating_sub(ext_bytes + dot);

    // Build UTF-8 string byte-by-byte so we don't split a char.
    let mut truncated = String::with_capacity(max_stem);
    for ch in stem.chars() {
        if truncated.len() + ch.len_utf8() > max_stem {
            break;
        }
        truncated.push(ch);
    }

    if ext.is_empty() {
        parent.join(truncated)
    } else {
        parent.join(format!("{truncated}.{ext}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_byte_str_checks_overflow_for_every_unit() {
        for (power, unit) in BYTE_UNITS.iter().enumerate() {
            let scale = 1024u64.pow(power as u32);
            let largest = u64::MAX / scale;
            assert_eq!(
                parse_byte_str(&format!("{largest}{unit}")),
                Some(largest * scale)
            );
            if scale > 1 {
                assert_eq!(parse_byte_str(&format!("{}{unit}", largest + 1)), None);
                assert_eq!(parse_byte_str(&format!("{}{unit}", u64::MAX)), None);
            }
        }
        assert_eq!(parse_byte_str("18446744073709551616B"), None);
    }

    #[test]
    fn parse_byte_str_preserves_existing_input_rules() {
        assert_eq!(parse_byte_str(" 1 \t0 MB\n"), Some(10 * 1024 * 1024));
        assert_eq!(parse_byte_str("0PB"), Some(0));
        assert_eq!(parse_byte_str("+1KB"), Some(1024));
        for invalid in ["", "1", "1kb", "1.5MB", "-1KB", "MB", "1EB", "1\u{a0}KB"] {
            assert_eq!(parse_byte_str(invalid), None, "{invalid:?}");
        }
    }
}
