//! The application UI: one shell, shared assets, and source-specific pages.
use axum::{Router, response::Html, routing::get};

pub(crate) fn router(engines: &[crate::runtime::RunningEngine]) -> Router {
    let app = Router::new()
        .route("/settings/", get(|| async { page("settings") }))
        .route("/", get(|| async { page("home") }))
        .route("/telegram/", get(|| async { page("telegram") }))
        .route("/jav/", get(|| async { page("jav") }));
    let app = [
        (
            "settings.js",
            "text/javascript",
            include_str!("../web/scripts/settings.js"),
        ),
        (
            "settings.css",
            "text/css",
            include_str!("../web/styles/settings.css"),
        ),
        (
            "home.css",
            "text/css",
            include_str!("../web/styles/home.css"),
        ),
        (
            "home.js",
            "text/javascript",
            include_str!("../web/scripts/home.js"),
        ),
        (
            "components.css",
            "text/css",
            include_str!("../web/styles/components.css"),
        ),
        (
            "telegram.css",
            "text/css",
            include_str!("../web/styles/telegram.css"),
        ),
        ("jav.css", "text/css", include_str!("../web/styles/jav.css")),
        (
            "theme.js",
            "text/javascript",
            include_str!("../web/scripts/theme.js"),
        ),
        (
            "shared.js",
            "text/javascript",
            include_str!("../web/scripts/shared.js"),
        ),
        (
            "telegram-settings.js",
            "text/javascript",
            include_str!("../web/scripts/telegram-settings.js"),
        ),
        (
            "telegram.js",
            "text/javascript",
            include_str!("../web/scripts/telegram.js"),
        ),
        (
            "jav.js",
            "text/javascript",
            include_str!("../web/scripts/jav.js"),
        ),
    ]
    .into_iter()
    .fold(app, |app, (name, content_type, content)| {
        app.route(
            &format!("/assets/{name}"),
            get(move || async move { ([("content-type", content_type)], content) }),
        )
    });
    engines.iter().fold(app, |app, engine| engine.mount(app))
}

fn page(section: &str) -> Html<String> {
    let (name, description, mark, content, actions) = match section {
        "settings" => (
            "Settings",
            "Storage, server and schedules",
            "S",
            include_str!("../web/pages/settings.html"),
            "",
        ),
        "telegram" => (
            "Telegram",
            "Messages and chat subscriptions",
            "T",
            include_str!("../web/pages/telegram.html"),
            "",
        ),
        "jav" => (
            "JAV",
            "Videos and scheduled downloads",
            "J",
            include_str!("../web/pages/jav.html"),
            "",
        ),
        _ => (
            "Media Downloader",
            "",
            "",
            include_str!("../web/pages/home.html"),
            "",
        ),
    };
    let home = section == "home";
    let title = if home {
        name.to_owned()
    } else {
        format!("{name} · Media Downloader")
    };
    let header = if home {
        String::new()
    } else {
        include_str!("../web/header.html")
            .replace("{{name}}", name)
            .replace("{{description}}", description)
            .replace("{{mark}}", mark)
            .replace("{{actions}}", actions)
            .replace(
                "{{connection}}",
                if section == "settings" {
                    ""
                } else {
                    r#"<span id="connection" class="pill" role="status">Connecting</span>"#
                },
            )
    };
    let assets = format!(
        r#"<link rel="stylesheet" href="/assets/{section}.css"><script type="module" src="/assets/{section}.js"></script>"#
    );
    // Every substitution is application-owned static content, never user input.
    let mut html = include_str!("../web/shell.html")
        .replace("{{title}}", &title)
        .replace("{{section}}", section)
        .replace("{{page_assets}}", &assets)
        .replace("{{header}}", &header);
    for name in ["home", "telegram", "jav", "settings"] {
        html = html.replace(
            &format!("{{{{{name}_current}}}}"),
            if name == section {
                r#"aria-current="page""#
            } else {
                ""
            },
        );
    }
    Html(
        html.replace("{{content}}", content)
            .replace("{{task_table}}", include_str!("../web/task-table.html")),
    )
}
