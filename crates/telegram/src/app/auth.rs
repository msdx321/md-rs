use std::time::Duration;

use grammers_client::{Client, SignInError};

use super::shutdown::flood_wait_secs;
use crate::api::ApiState;

pub(super) async fn authorize_web(
    client: &Client,
    api_hash: &str,
    state: &ApiState,
) -> anyhow::Result<()> {
    let mut phone_message =
        "Enter your phone number in international format, including + and country code."
            .to_string();
    loop {
        let phone = state.login_prompt("phone", &phone_message).await?.value;
        let token = match client.request_login_code(phone.trim(), api_hash).await {
            Ok(token) => token,
            Err(error) => {
                let message = error.to_string();
                if let Some(seconds) = flood_wait_secs(&message) {
                    state
                        .login_status(
                            "working",
                            &format!(
                                "Telegram asks you to wait {seconds} seconds before trying again."
                            ),
                        )
                        .await;
                    tokio::time::sleep(Duration::from_secs(seconds)).await;
                    phone_message = "You can now request a new login code.".into();
                } else {
                    // Return to the retry/credentials screen for invalid API credentials,
                    // network failures, or rejected phone numbers.
                    return Err(error.into());
                }
                continue;
            }
        };
        let mut code_message = "Enter the verification code sent by Telegram.";
        loop {
            let code = state.login_prompt("code", code_message).await?.value;
            match client.sign_in(&token, code.trim()).await {
                Ok(_) => return Ok(()),
                Err(SignInError::InvalidCode) => {
                    code_message = "That code was not accepted. Check the code and try again.";
                }
                Err(SignInError::PasswordRequired(mut token)) => {
                    let mut message = "Enter your Telegram two-factor authentication password.";
                    loop {
                        let password = state.login_prompt("password", message).await?.value;
                        match client.check_password(token, password).await {
                            Ok(_) => return Ok(()),
                            Err(SignInError::InvalidPassword(next_token)) => {
                                token = next_token;
                                message = "Incorrect password. Please try again.";
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}
