//! Filename policy shared by JAV and 91Porn videos. Telegram's template and
//! extension-preserving filename rules intentionally remain separate.

/// Replace reserved filename characters and limit the title to 180 UTF-8 bytes.
pub fn sanitize_filename(title: &str) -> String {
    let cleaned: String = title
        .chars()
        .map(|c| match c {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' | '\0' => '_',
            c if (c as u32) < 0x20 => '_',
            c => c,
        })
        .collect();
    let cleaned = cleaned.trim().trim_matches('.').to_string();
    // Keep well under the 255-byte limit most filesystems impose, counting
    // UTF-8 bytes rather than characters.
    truncate_bytes(&cleaned, 180)
}

fn truncate_bytes(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].trim_end().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_filename_preserves_control_and_trimming_policy() {
        assert_eq!(sanitize_filename("\0a\n\tb\u{1f}\u{7f}"), "_a__b_\u{7f}");
        assert_eq!(sanitize_filename(" .. a .. "), " a ");
        assert_eq!(sanitize_filename("..."), "");
        assert_eq!(
            sanitize_filename(&format!("{}🙂", "a".repeat(179))),
            "a".repeat(179)
        );
        assert_eq!(
            sanitize_filename(&format!("{} 🙂", "a".repeat(179))),
            "a".repeat(179)
        );
    }
}
