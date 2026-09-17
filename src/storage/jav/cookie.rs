//! Browser clearance credentials, separate from user configuration.
use crate::storage::Database;

pub struct Cookie {
    pub value: String,
    pub user_agent: String,
}

pub async fn load(
    database: &Database,
    site: &str,
    configured_cookie: &str,
    configured_user_agent: &str,
) -> anyhow::Result<Option<Cookie>> {
    let conn = database.connection().await;
    let mut rows = conn.query("SELECT value,user_agent FROM jav_cookie WHERE id=1 AND site=? AND configured_cookie=? AND configured_user_agent=?", (site, configured_cookie, configured_user_agent)).await?;
    rows.next()
        .await?
        .map(|row| {
            Ok(Cookie {
                value: row.get(0)?,
                user_agent: row.get(1)?,
            })
        })
        .transpose()
}

pub async fn save(
    database: &Database,
    site: &str,
    configured_cookie: &str,
    configured_user_agent: &str,
    cookie: &Cookie,
) -> anyhow::Result<()> {
    database.connection().await.execute("INSERT INTO jav_cookie(id,site,configured_cookie,configured_user_agent,value,user_agent) VALUES (1,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET site=excluded.site,configured_cookie=excluded.configured_cookie,configured_user_agent=excluded.configured_user_agent,value=excluded.value,user_agent=excluded.user_agent", (site, configured_cookie, configured_user_agent, cookie.value.as_str(), cookie.user_agent.as_str())).await?;
    Ok(())
}
