use axum::http::StatusCode;
use serde_json::json;

pub fn internal_error(
    context: &str,
    err: impl std::fmt::Display,
) -> (StatusCode, axum::Json<serde_json::Value>) {
    tracing::error!("{}: {}", context, err);
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        axum::Json(json!({"error": "Internal server error"})),
    )
}

pub fn not_found_error(
    context: &str,
    err: impl std::fmt::Display,
) -> (StatusCode, axum::Json<serde_json::Value>) {
    tracing::warn!("{}: {}", context, err);
    (
        StatusCode::NOT_FOUND,
        axum::Json(json!({"error": "Not found"})),
    )
}
