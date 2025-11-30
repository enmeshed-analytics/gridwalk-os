use crate::config::AppState;
use crate::layer::Layer;
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use gridwalk_core::LayerCore;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

#[derive(Debug, Deserialize)]
pub struct LayersQuery {
    #[serde(default = "default_limit")]
    limit: u64,
    #[serde(default)]
    offset: u64,
}

fn default_limit() -> u64 {
    50
}

#[axum::debug_handler]
pub async fn get_layers(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LayersQuery>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    let layers = Layer::list(query.limit, query.offset, &*state.app_db)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": format!("Failed to fetch layers: {}", e)})),
            )
        })?;

    Ok(axum::Json(layers))
}
