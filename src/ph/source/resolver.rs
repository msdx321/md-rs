//! Turning a video page into a downloadable MP4.
//!
//! The player configuration names a `/video/get_media` endpoint. Requesting it
//! with the same session returns the progressive MP4 variants and their
//! dimensions; the downloader then picks the configured height cap.

use url::Url;

use crate::ph::config::Config;
use crate::ph::source::http::{Fetcher, Session, origin_of};
use crate::ph::source::scraper::{self, MediaDefinition, VideoCard, VideoPage};

/// A resolved progressive media URL plus what the page told us.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedVideo {
    pub title: String,
    pub duration_secs: Option<u64>,
    pub source_url: String,
    pub width: Option<u64>,
    pub height: Option<u64>,
}

impl ResolvedVideo {
    pub fn quality_label(&self) -> String {
        match self.height {
            Some(height) => format!("{height}p"),
            None => "MP4".into(),
        }
    }
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
        .map_err(|error| anyhow::anyhow!("cannot establish a Pornhub session: {error:#}"))?;

    let video_url = if card.url.trim().is_empty() {
        cfg.video_url(&card.id)
    } else {
        card.url.clone()
    };

    for attempt in 0..2 {
        let page = scraper::fetch_video_page(fetch, &video_url).await?;
        session.observe_login(fetch, page.logged_in)?;
        if !vid_matches(card, &page) {
            anyhow::bail!(
                "the site returned a different video for {} — the session cookie may be rejected",
                card.id
            );
        }
        // A signed-out page after a successful login means the session expired.
        if page.logged_in == Some(false)
            && cfg.account().is_some()
            && cfg.request_cookie().is_none()
            && attempt == 0
        {
            log::warn!("[{}] video page is signed out; logging in again", card.id);
            session.relogin(fetch).await?;
            continue;
        }
        anyhow::ensure!(
            page.logged_in != Some(false)
                || cfg.account().is_none()
                || cfg.request_cookie().is_some(),
            "Pornhub video page is still signed out after re-login"
        );
        let mut variants = Vec::new();
        for definition in page
            .media
            .iter()
            .filter(|definition| definition.format == "mp4")
        {
            let media_url = absolute(&video_url, &definition.url)?;
            if Url::parse(&media_url)?.path().trim_end_matches('/') == "/video/get_media" {
                let response = fetch.ajax_json(&media_url, &video_url).await?;
                for mut variant in scraper::parse_media_response(&response) {
                    variant.url = absolute(&media_url, &variant.url)?;
                    variants.push(variant);
                }
            } else {
                // Direct MP4 definitions are already variants, not JSON endpoints.
                variants.push(MediaDefinition {
                    url: media_url,
                    ..definition.clone()
                });
            }
        }
        let Some(variant) = select_variant(&variants, cfg.target_height()) else {
            if attempt == 0 {
                // The endpoint occasionally answers with an empty list when the
                // page's one-time token has just rotated; one fresh retry fixes it.
                log::debug!("[{}] get_media returned no MP4 variants; retrying", card.id);
                continue;
            }
            anyhow::bail!("no MP4 variant available for {}", card.id);
        };
        let title = if page.title.trim().is_empty() {
            card.title.clone()
        } else {
            page.title.trim().to_string()
        };
        return Ok(ResolvedVideo {
            title,
            duration_secs: page.duration_secs.or(card.duration_secs),
            source_url: variant.url.clone(),
            width: variant.width,
            height: variant.height,
        });
    }
    anyhow::bail!("could not resolve an MP4 source for {}", card.id)
}

fn absolute(page_url: &str, media_url: &str) -> anyhow::Result<String> {
    let base = Url::parse(page_url)?;
    let url = base.join(media_url)?;
    anyhow::ensure!(
        matches!(url.scheme(), "http" | "https"),
        "invalid media URL scheme"
    );
    Ok(url.into())
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

/// Pick the highest variant at or below the configured height cap.
pub fn select_variant(media: &[MediaDefinition], target: Option<u64>) -> Option<MediaDefinition> {
    let mut candidates: Vec<&MediaDefinition> = media
        .iter()
        .filter(|definition| definition.format == "mp4")
        .filter(|definition| {
            definition.url.starts_with("http://") || definition.url.starts_with("https://")
        })
        .collect();
    if candidates.is_empty() {
        return None;
    }
    candidates.sort_by_key(|left| std::cmp::Reverse(left.height.unwrap_or(0)));
    candidates.dedup_by(|left, right| left.url == right.url);
    let Some(target) = target else {
        return candidates.first().map(|definition| (*definition).clone());
    };
    candidates
        .iter()
        .find(|definition| definition.height.is_some_and(|height| height <= target))
        .or_else(|| candidates.last())
        .map(|definition| (*definition).clone())
}

/// Referer used for site requests.
pub fn site_referer(cfg: &Config) -> String {
    format!("{}/", cfg.site_base.trim_end_matches('/'))
}

/// Referer used for the signed media request.
pub fn media_referer(cfg: &Config, card: &VideoCard) -> String {
    origin_of(&card.url)
        .or_else(|| origin_of(&cfg.video_url(&card.id)))
        .unwrap_or_else(|| site_referer(cfg))
}
