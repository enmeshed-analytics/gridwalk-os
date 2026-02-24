use crate::config::AppState;
use crate::error::internal_error;
use crate::layer::Layer;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use gridwalk_core::LayerCore;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct LayersQuery {
    #[serde(default = "default_limit")]
    limit: u64,
    #[serde(default)]
    offset: u64,
    #[serde(default = "default_layer_category")]
    layer_category: gridwalk_core::LayerCategory,
}

fn default_limit() -> u64 {
    100
}

fn default_layer_category() -> gridwalk_core::LayerCategory {
    gridwalk_core::LayerCategory::Custom
}

#[axum::debug_handler]
pub async fn get_layers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LayersQuery>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    let layers = Layer::list(
        query.limit,
        query.offset,
        query.layer_category,
        &*state.app_db,
    )
    .await
    .map_err(|e| internal_error("Failed to fetch layers", e))?;

    Ok(axum::Json(layers))
}
