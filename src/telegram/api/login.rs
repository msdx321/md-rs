use axum::{Json, extract::State, http::StatusCode};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};

use super::ApiState;

#[derive(Clone, Serialize)]
pub(crate) struct LoginSnapshot {
    pub step: String,
    pub message: String,
    pub id: u64,
}

pub(crate) struct Login {
    pub snapshot: LoginSnapshot,
    reply: Option<oneshot::Sender<LoginInput>>,
}

impl Default for Login {
    fn default() -> Self {
        Self {
            snapshot: LoginSnapshot {
                step: "working".into(),
                message: "Connecting to Telegram…".into(),
                id: 0,
            },
            reply: None,
        }
    }
}

#[derive(Deserialize)]
pub(crate) struct LoginInput {
    id: u64,
    pub value: String,
    #[serde(default)]
    pub api_hash: String,
}

impl ApiState {
    pub(crate) async fn login_prompt(
        &self,
        step: &str,
        message: &str,
    ) -> anyhow::Result<LoginInput> {
        let (tx, rx) = oneshot::channel();
        {
            let mut login = self.login.lock().await;
            login.snapshot.id += 1;
            login.snapshot.step = step.into();
            login.snapshot.message = message.into();
            login.reply = Some(tx);
        }
        self.publish().await;
        Ok(rx.await?)
    }

    pub(crate) async fn login_status(&self, step: &str, message: &str) {
        {
            let mut login = self.login.lock().await;
            login.snapshot.step = step.into();
            login.snapshot.message = message.into();
            login.reply = None;
        }
        self.publish().await;
    }
}

pub(super) async fn submit(
    State(state): State<Arc<ApiState>>,
    Json(input): Json<LoginInput>,
) -> Result<StatusCode, (StatusCode, &'static str)> {
    if input.value.is_empty() || input.value.len() > 1024 || input.api_hash.len() > 128 {
        return Err((StatusCode::BAD_REQUEST, "Enter a valid value"));
    }
    {
        let mut login = state.login.lock().await;
        if login.snapshot.id != input.id {
            return Err((StatusCode::CONFLICT, "Login step changed; try again"));
        }
        let reply = login
            .reply
            .take()
            .ok_or((StatusCode::CONFLICT, "Login is already processing"))?;
        reply
            .send(input)
            .map_err(|_| (StatusCode::CONFLICT, "Login is no longer waiting"))?;
        login.snapshot.step = "working".into();
        login.snapshot.message = "Connecting to Telegram…".into();
    }
    state.publish().await;
    Ok(StatusCode::ACCEPTED)
}

pub(crate) fn new() -> Mutex<Login> {
    Mutex::new(Login::default())
}
