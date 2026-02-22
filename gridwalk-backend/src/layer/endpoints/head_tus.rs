use crate::config::AppState;
use crate::layer::{Layer, LayerUpload, LayerUploadStatus};
use axum::{
    extract::{Path as RequestPath, State},
    http::{HeaderMap, StatusCode, header::HeaderValue},
    response::IntoResponse,
};
use gridwalk_core::LayerCore;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

// HEAD (using TUS protocol) function to get upload status
#[axum::debug_handler]
pub async fn head_tus(
    RequestPath(layer_id): RequestPath<Uuid>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    // Get the layer upload record from database
    let layer_upload = match LayerUpload::get(&*state.app_db, layer_id).await {
        Ok(upload) => upload,
        Err(_e) => {
            // Check if the layer exists - if so, upload was completed and finalized
            if Layer::exists(layer_id, &*state.app_db)
                .await
                .unwrap_or(false)
            {
                // Get the layer to extract upload size from metadata
                let layer = Layer::get(layer_id, &*state.app_db).await.map_err(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(json!({"error": "Failed to retrieve finalized layer"})),
                    )
                })?;

                // Extract upload_size from metadata
                let upload_size = if let Some(metadata) = &layer.metadata {
                    metadata
                        .get("upload_size")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0)
                } else {
                    0
                };

                // Prepare response headers for completed upload
                let mut response_headers = HeaderMap::new();
                response_headers.insert("tus-resumable", HeaderValue::from_static("1.0.0"));
                response_headers.insert("cache-control", HeaderValue::from_static("no-store"));

                response_headers.insert(
                    "upload-offset",
                    HeaderValue::from_str(&upload_size.to_string()).map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            axum::Json(json!({"error": "Failed to create upload-offset header"})),
                        )
                    })?,
                );

                response_headers.insert(
                    "upload-length",
                    HeaderValue::from_str(&upload_size.to_string()).map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            axum::Json(json!({"error": "Failed to create upload-length header"})),
                        )
                    })?,
                );

                return Ok((StatusCode::OK, response_headers));
            } else {
                return Err((
                    StatusCode::NOT_FOUND,
                    axum::Json(json!({"error": "Upload not found"})),
                ));
            }
        }
    };

    // If upload has been completed/finalized, return 410 Gone
    if layer_upload.status == LayerUploadStatus::Uploaded {
        return Err((
            StatusCode::GONE,
            axum::Json(json!({"error": "Upload has been finalized"})),
        ));
    }

    // Prepare response headers
    let mut response_headers = HeaderMap::new();
    response_headers.insert("tus-resumable", HeaderValue::from_static("1.0.0"));
    response_headers.insert("cache-control", HeaderValue::from_static("no-store"));

    response_headers.insert(
        "upload-offset",
        HeaderValue::from_str(&layer_upload.current_offset.to_string()).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "Failed to create upload-offset header"})),
            )
        })?,
    );

    // Include upload-length header if total size is known
    if let Some(total_size) = layer_upload.total_size {
        response_headers.insert(
            "upload-length",
            HeaderValue::from_str(&total_size.to_string()).map_err(|_| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": "Failed to create upload-length header"})),
                )
            })?,
        );
    }

    Ok((StatusCode::OK, response_headers))
}
