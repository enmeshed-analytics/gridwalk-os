use crate::config::AppState;
use crate::layer::{Layer, LayerUpload, LayerUploadStatus};
use axum::{
    extract::State,
    http::{
        HeaderMap, StatusCode,
        header::{HeaderValue, LOCATION},
    },
    response::IntoResponse,
};
use base64::prelude::*;
use gridwalk_core::LayerCore;
use serde_json::json;
use std::sync::Arc;
use tokio::fs;
use uuid::Uuid;

// POST (using TUS protocol) function to create a new layer
#[axum::debug_handler]
pub async fn post_tus(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    // Validate TUS-Resumable header
    let _tus_version = headers
        .get("tus-resumable")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": "Missing Tus-Resumable header"})),
            )
        })?;

    // Extract Upload-Length or Upload-Defer-Length header
    let total_size: Option<i64> = if let Some(length_header) = headers.get("upload-length") {
        Some(
            length_header
                .to_str()
                .map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        axum::Json(json!({"error": "Invalid Upload-Length header"})),
                    )
                })?
                .parse()
                .map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        axum::Json(json!({"error": "Upload-Length must be a valid integer"})),
                    )
                })?,
        )
    } else if let Some(defer_header) = headers.get("upload-defer-length") {
        let defer_value = defer_header.to_str().map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": "Invalid Upload-Defer-Length header"})),
            )
        })?;

        if defer_value != "1" {
            return Err((
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": "Upload-Defer-Length must be '1'"})),
            ));
        }
        None
    } else {
        return Err((
            StatusCode::BAD_REQUEST,
            axum::Json(
                json!({"error": "Either Upload-Length or Upload-Defer-Length header is required"}),
            ),
        ));
    };

    // Parse Upload-Metadata header
    let metadata_header = headers.get("upload-metadata").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "Missing Upload-Metadata header"})),
        )
    })?;

    let metadata_str = metadata_header.to_str().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "Invalid Upload-Metadata header"})),
        )
    })?;

    let mut name: Option<String> = None;
    let mut upload_type: Option<String> = None;

    for pair in metadata_str.split(',') {
        let parts: Vec<&str> = pair.trim().splitn(2, ' ').collect();
        if parts.len() == 2 {
            let key = parts[0];
            if let Ok(decoded_value) = BASE64_STANDARD.decode(parts[1]) {
                if let Ok(value) = String::from_utf8(decoded_value) {
                    match key {
                        "name" => {
                            let trimmed_name = value.trim().to_string();
                            if trimmed_name
                                .chars()
                                .all(|c| c.is_alphanumeric() || c == ' ' || c == '_' || c == '-')
                            {
                                name = Some(trimmed_name);
                            } else {
                                return Err((
                                    StatusCode::BAD_REQUEST,
                                    axum::Json(
                                        json!({"error": "Name can only contain alphanumeric characters, spaces, hyphens, and underscores"}),
                                    ),
                                ));
                            }
                        }
                        "upload_type" => {
                            let trimmed_type = value.trim().to_string();
                            if trimmed_type.chars().all(|c| c.is_alphabetic()) {
                                upload_type = Some(trimmed_type);
                            } else {
                                return Err((
                                    StatusCode::BAD_REQUEST,
                                    axum::Json(
                                        json!({"error": "Upload type can only contain letters"}),
                                    ),
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    let name = name.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "Missing 'name' in Upload-Metadata"})),
        )
    })?;

    let upload_type = upload_type.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "Missing 'upload_type' in Upload-Metadata"})),
        )
    })?;

    // Generate a unique UUID that doesn't already exist as a Layer
    let mut layer_id = Uuid::new_v4();
    let mut retry_count = 0;
    const MAX_RETRIES: usize = 5;

    while retry_count < MAX_RETRIES {
        if !Layer::exists(layer_id, &*state.app_db).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": format!("Failed to check layer existence: {}", e)})),
            )
        })? {
            break;
        }

        retry_count += 1;
        if retry_count >= MAX_RETRIES {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(
                    json!({"error": "Failed to generate unique layer ID after maximum retries"}),
                ),
            ));
        }

        layer_id = Uuid::new_v4();
    }

    let layer_upload = LayerUpload {
        id: layer_id,
        status: LayerUploadStatus::Uploading,
        name,
        upload_type: Some(upload_type),
        total_size,
        current_offset: 0,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    // Create empty file for TUS upload
    let upload_file_path = state.temp_data_path.join(layer_upload.id.to_string());
    println!("Creating upload file at {:?}", upload_file_path);
    fs::File::create(&upload_file_path).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("Failed to create upload file: {}", e)})),
        )
    })?;

    println!("Saving layer upload record to database: {:?}", layer_upload);
    layer_upload.save(&*state.app_db).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("Failed to save layer: {}", e)})),
        )
    })?;

    let mut response_headers = HeaderMap::new();
    response_headers.insert("tus-resumable", HeaderValue::from_static("1.0.0"));
    response_headers.insert(
        LOCATION,
        HeaderValue::from_str(&format!("/layers/upload/{}", layer_upload.id)).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "Failed to create location header"})),
            )
        })?,
    );

    Ok((StatusCode::CREATED, response_headers))
}
