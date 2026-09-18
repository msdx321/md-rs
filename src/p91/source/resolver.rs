//! Turning a video page into something downloadable.
//!
//! The standard page always yields a signed progressive MP4 for guests. HD
//! lives on a separate `view_video_hd.php` page whose source the server only
//! writes for a logged-in (VIP) account, so it is tried first when a cookie is
//! configured and falls back to the standard page otherwise.
//!
//! Both pages echo the internal video id (`<div id=VID>`). That is checked
//! against the id on the listing card, because a session without the site's
//! cookie gets a random video instead of the requested one.

use crate::p91::config::Config;
use crate::p91::source::http::{Fetcher, Session, origin_of};
use crate::p91::source::scraper::{self, VideoCard, VideoPage};

/// A resolved progressive media URL plus the title shown by the page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedVideo {
    pub title: String,
    pub source_url: String,
    /// The URL came from the HD page.
    pub hd: bool,
}

/// Resolve a video card to a downloadable URL.
pub async fn resolve(
    session: &Session,
    fetch: &Fetcher,
    cfg: &Config,
    card: &VideoCard,
) -> anyhow::Result<ResolvedVideo> {
    let referer = site_referer(cfg);
    session
        .ensure(&cfg.popular_url(1), &referer, fetch)
        .await
        .map_err(|error| anyhow::anyhow!("cannot establish a 91Porn session: {error:#}"))?;

    let standard_url = if card.url.trim().is_empty() {
        cfg.video_url(&card.id)
    } else {
        card.url.clone()
    };
    let hd_url = cfg.hd_url(&card.id);
    let try_hd = cfg.prefer_hd && cfg.request_cookie().is_some();
    let mut standard_error = None;
    let mut hd_error = None;

    // Two attempts: the second runs after the session cookie is re-established.
    for attempt in 0..2 {
        if try_hd {
            match scraper::fetch_video_page(fetch, &hd_url).await {
                Ok(page) if page.source_url.is_some() && vid_matches(card, &page) => {
                    return Ok(ResolvedVideo {
                        title: pick_title(&page.title, card),
                        source_url: page.source_url.expect("checked above"),
                        hd: true,
                    });
                }
                Ok(page) => log::debug!(
                    "[{}] HD page gave vid={:?} and {} source; falling back to standard",
                    card.id,
                    page.vid,
                    if page.source_url.is_some() { "a" } else { "no" }
                ),
                Err(error) => {
                    log::debug!("[{0}] HD page unavailable: {error:#}", card.id);
                    hd_error = Some(error);
                }
            }
        }

        match scraper::fetch_video_page(fetch, &standard_url).await {
            Ok(page) if vid_matches(card, &page) => {
                let Some(source_url) = page.source_url else {
                    anyhow::bail!("no video source found on {standard_url}");
                };
                return Ok(ResolvedVideo {
                    title: pick_title(&page.title, card),
                    source_url,
                    hd: false,
                });
            }
            Ok(page) => {
                // A cold or expired session makes the site answer with an
                // unrelated video. Re-warm once rather than downloading it.
                log::warn!(
                    "[{}] video page returned vid={:?} instead of {:?}; re-establishing the session",
                    card.id,
                    page.vid,
                    card.vid
                );
                if attempt == 0 {
                    session.reset();
                    session
                        .ensure(&cfg.popular_url(1), &referer, fetch)
                        .await
                        .map_err(|error| {
                            anyhow::anyhow!("cannot re-establish a 91Porn session: {error:#}")
                        })?;
                }
            }
            Err(error) => {
                standard_error = Some(error);
                break;
            }
        }
    }

    if let Some(error) = standard_error {
        // With a cookie configured the HD failure is usually the actionable one.
        return Err(hd_error.unwrap_or(error));
    }
    anyhow::bail!(
        "the site kept returning a different video for {} — the session cookie may be rejected",
        card.id
    )
}

/// A known listing id must match the page. Manual URLs without a listing id
/// cannot be checked against listing metadata.
fn vid_matches(card: &VideoCard, page: &VideoPage) -> bool {
    match (&card.vid, &page.vid) {
        (Some(expected), Some(actual)) => expected == actual,
        (Some(_), None) => false,
        (None, _) => true,
    }
}

fn pick_title(page_title: &str, card: &VideoCard) -> String {
    if page_title.trim().is_empty() {
        card.title.clone()
    } else {
        page_title.trim().to_string()
    }
}

/// Referer used for site requests and the signed media request.
pub fn site_referer(cfg: &Config) -> String {
    format!("{}/", cfg.site_base.trim_end_matches('/'))
}

/// Referer used for the signed media request.
pub fn media_referer(cfg: &Config, card: &VideoCard) -> String {
    origin_of(&card.url)
        .or_else(|| origin_of(&cfg.video_url(&card.id)))
        .unwrap_or_else(|| site_referer(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card(vid: Option<&str>) -> VideoCard {
        VideoCard {
            id: "1950234389".into(),
            url: "https://www.91porn.com/view_video.php?viewkey=1950234389".into(),
            title: "Card title".into(),
            image_url: String::new(),
            duration_secs: None,
            rank: Some(1),
            vid: vid.map(str::to_string),
            hd: true,
            original: false,
        }
    }

    #[test]
    fn page_title_wins_over_the_listing_title() {
        assert_eq!(pick_title("Page title", &card(None)), "Page title");
        assert_eq!(pick_title("   ", &card(None)), "Card title");
    }

    #[test]
    fn media_referer_uses_the_site_origin() {
        let cfg = Config::default();
        assert_eq!(media_referer(&cfg, &card(None)), "https://www.91porn.com/");
    }

    #[test]
    fn vid_check_catches_decoy_pages() {
        let page = |vid: Option<&str>| VideoPage {
            title: String::new(),
            source_url: Some("https://cdn/x.mp4".into()),
            vid: vid.map(str::to_string),
        };
        assert!(vid_matches(&card(Some("123")), &page(Some("123"))));
        assert!(!vid_matches(&card(Some("123")), &page(Some("999"))));
        // Without an expected id the check cannot fail.
        assert!(vid_matches(&card(None), &page(Some("999"))));
        assert!(!vid_matches(&card(Some("123")), &page(None)));
    }
}
