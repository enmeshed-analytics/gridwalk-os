use crate::config::AppState;
use crate::layer::Layer;
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use gridwalk_core::LayerCore;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[derive(serde::Deserialize)]
pub struct CreateLayerRequest {
    pub name: String,
    pub description: Option<String>,
    pub geometry: gridwalk_core::GeometryType,
}

// POST (using TUS protocol) function to create a new layer
#[axum::debug_handler]
pub async fn create(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<CreateLayerRequest>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    // Create hashmap to hold description and geometry metadata
    let mut metadata = std::collections::HashMap::new();
    if let Some(desc) = &payload.description {
        metadata.insert("description".to_string(), desc.clone());
    }
    metadata.insert("geometry".to_string(), format!("{:?}", payload.geometry));

    // Create new Layer struct
    let new_layer = Layer {
        id: Uuid::new_v4(),
        status: crate::layer::LayerStatus::Available,
        name: payload.name,
        metadata: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    // Save new layer to database
    new_layer.save(&*state.app_db).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("Failed to create layer: {}", e)})),
        )
    })?;

    // Return ok response for testing
    Ok(axum::Json(
        json!({"message": "Layer created successfully", "layer_id": new_layer.id}),
    ))
}
