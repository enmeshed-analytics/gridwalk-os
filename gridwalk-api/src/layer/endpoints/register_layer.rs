use crate::config::AppState;
use crate::layer::{Layer, LayerCategory, LayerStatus};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use gridwalk_core::LayerCore;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[derive(serde::Deserialize)]
pub struct RegisterLayerRequest {
    pub name: String,
    pub location_namespace: String,
    pub location_name: String,
    pub layer_category: LayerCategory,
    pub geometry_field: String,
    pub srid: gridwalk_core::Srid,
    pub metadata: Option<serde_json::Value>,
}

/// POST endpoint to register existing PostGIS data as a layer
#[axum::debug_handler]
pub async fn register_layer(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<RegisterLayerRequest>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
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

    // Create new Layer struct
    let new_layer = Layer {
        id: layer_id,
        status: LayerStatus::Available,
        name: payload.name,
        layer_category: payload.layer_category,
        location_namespace: payload.location_namespace,
        location_name: payload.location_name,
        geometry_field: Some(payload.geometry_field),
        srid: Some(payload.srid),
        metadata: payload.metadata,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    // Save new layer to database
    new_layer.save(&*state.app_db).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("Failed to register layer: {}", e)})),
        )
    })?;

    Ok(axum::Json(json!({
        "message": "Layer registered successfully",
        "layer_id": new_layer.id,
        "name": new_layer.name,
        "location": {
            "namespace": new_layer.location_namespace,
            "name": new_layer.location_name
        },
        "category": new_layer.layer_category
    })))
}
