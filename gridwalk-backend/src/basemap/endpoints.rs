use crate::config::AppState;
use crate::error::internal_error;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use base64::{Engine as _, engine::general_purpose};
use reqwest;
use serde_json::json;
use std::env;
use std::sync::Arc;

#[derive(serde::Deserialize)]
pub struct OSCredentials {
    pub api_key: String,
    pub api_secret: String,
}

pub async fn set_os_credentials(
    State(state): State<Arc<AppState>>,
    Json(payload): axum::Json<OSCredentials>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    // Get encryption key from environment variable
    let encryption_key = env::var("ENCRYPTION_KEY").map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": "ENCRYPTION_KEY environment variable not set"})),
        )
    })?;

    super::store::save_os_credentials(
        &*state.app_db,
        &encryption_key,
        &payload.api_key,
        &payload.api_secret,
    )
    .await
    .map_err(|e| internal_error("Failed to save OS credentials", e))?;

    Ok(axum::Json(
        json!({"message": "OS credentials set successfully"}),
    ))
}

pub async fn get_os_token(
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    let encryption_key = env::var("ENCRYPTION_KEY").map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": "ENCRYPTION_KEY environment variable not set"})),
        )
    })?;

    let (os_api_key, os_api_secret) = super::store::get_os_credentials(
        &*state.app_db,
        &encryption_key,
    )
    .await
    .map_err(|e| internal_error("Failed to get OS credentials", e))?;

    // Create Basic Auth header
    let credentials = format!("{}:{}", os_api_key, os_api_secret);
    let encoded_credentials = general_purpose::STANDARD.encode(credentials.as_bytes());
    let auth_header = format!("Basic {}", encoded_credentials);

    // Make the token request
    let client = reqwest::Client::new();
    let response = client
        .post("https://api.os.uk/oauth2/token/v1")
        .header("Authorization", auth_header)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("grant_type=client_credentials")
        .send()
        .await
        .map_err(|e| internal_error("Failed to request token", e))?;

    if !response.status().is_success() {
        return Err(internal_error(
            "Token request failed",
            response.status(),
        ));
    }

    let token_data: serde_json::Value = response
        .json()
        .await
        .map_err(|e| internal_error("Failed to parse token response", e))?;

    Ok(axum::Json(token_data))
}
