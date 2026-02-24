use axum::http::StatusCode;
use serde_json::json;

/// Validates that a string is safe to use as a SQL identifier.
/// Returns a 400 error tuple with the given field label on failure.
pub fn validate_identifier(
    value: &str,
    label: &str,
) -> Result<(), (StatusCode, axum::Json<serde_json::Value>)> {
    if value.is_empty()
        || value.contains(&[';', '\'', '"', '\\', '\0', '\n', '\r'][..])
        || value.to_uppercase().contains("DROP")
        || value.to_uppercase().contains("DELETE")
        || value.to_uppercase().contains("INSERT")
        || value.to_uppercase().contains("UPDATE")
    {
        return Err((
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": format!("Invalid {}", label)})),
        ));
    }
    Ok(())
}
