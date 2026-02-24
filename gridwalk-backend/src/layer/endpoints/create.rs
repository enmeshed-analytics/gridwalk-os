use crate::config::AppState;
use crate::error::internal_error;
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

    let layer_id = Uuid::new_v4();
    // Create new Layer struct
    let new_layer = Layer {
        id: layer_id,
        status: crate::layer::LayerStatus::Available,
        name: payload.name,
        layer_category: gridwalk_core::LayerCategory::Custom,
        location_namespace: "gridwalk_layer_data".to_string(),
        location_name: layer_id.to_string(),
        geometry_field: Some("geometry".to_string()),
        srid: Some(gridwalk_core::Srid::EPSG4326),
        min_zoom: None,
        max_zoom: None,
        metadata: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };

    // Save new layer to database
    new_layer
        .save(&*state.app_db)
        .await
        .map_err(|e| internal_error("Failed to create layer", e))?;

    // Return ok response for testing
    Ok(axum::Json(
        json!({"message": "Layer created successfully", "layer_id": new_layer.id}),
    ))
}
