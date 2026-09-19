//! Common listing filter and pagination policy; provider URL construction and
//! validation remain with each configuration model.
use anyhow::Context;

pub(super) fn title_matcher(filter: &str, is_regex: bool) -> anyhow::Result<Option<regex::Regex>> {
    if filter.trim().is_empty() {
        return Ok(None);
    }
    let pattern = if is_regex {
        filter.to_string()
    } else {
        regex::escape(filter)
    };
    regex::RegexBuilder::new(&pattern)
        .case_insensitive(!is_regex)
        .build()
        .map(Some)
        .context("invalid title filter regex")
}

/// Replace pasted page parameters, removing them for page 1 and stripping
/// fragments from parsed URLs. Preserve the legacy fallback for unparsed paths.
pub(super) fn page_suffix(path: &str, page: usize) -> String {
    if let Ok(mut url) = url::Url::parse(path) {
        let pairs: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "page")
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect();
        url.set_query(None);
        if !pairs.is_empty() || page > 1 {
            let mut query = url.query_pairs_mut();
            query.extend_pairs(pairs);
            if page > 1 {
                query.append_pair("page", &page.to_string());
            }
        }
        url.set_fragment(None);
        return url.into();
    }
    if page <= 1 {
        return path.to_string();
    }
    if path.contains('?') {
        format!("{path}&page={page}")
    } else {
        format!("{path}?page={page}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn title_matcher_preserves_whitespace_literal_and_regex_policy() {
        assert!(title_matcher(" \t", true).unwrap().is_none());
        let literal = title_matcher(" A.b ", false).unwrap().unwrap();
        assert!(literal.is_match("prefix a.B suffix"));
        assert!(!literal.is_match("a.b"));
        assert!(!literal.is_match(" Axb "));
        let regex = title_matcher("^A.b$", true).unwrap().unwrap();
        assert!(regex.is_match("Axb"));
        assert!(!regex.is_match("axb"));
        assert_eq!(
            title_matcher("[", true).unwrap_err().to_string(),
            "invalid title filter regex"
        );
    }

    #[test]
    fn page_suffix_replaces_all_pages_and_strips_fragments() {
        let path = "https://example.test/list?sort=top&page=9&tag=a%20b&page=2&tag=c#part";
        assert_eq!(
            page_suffix(path, 1),
            "https://example.test/list?sort=top&tag=a+b&tag=c"
        );
        assert_eq!(
            page_suffix(path, 3),
            "https://example.test/list?sort=top&tag=a+b&tag=c&page=3"
        );
        assert_eq!(
            page_suffix("https://example.test/list?page=8#part", 0),
            "https://example.test/list"
        );
    }

    #[test]
    fn page_suffix_keeps_unparsed_path_fallback() {
        assert_eq!(page_suffix("/list?sort=top#part", 1), "/list?sort=top#part");
        assert_eq!(page_suffix("/list?sort=top", 2), "/list?sort=top&page=2");
        assert_eq!(page_suffix("/list", 2), "/list?page=2");
    }
}
