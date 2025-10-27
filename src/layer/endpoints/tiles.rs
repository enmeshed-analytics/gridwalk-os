use crate::config::AppState;
use axum::{
    extract::{Path as RequestPath, State},
    http::{HeaderMap, StatusCode, header::HeaderValue},
    response::IntoResponse,
};
use gridwalk_core::VectorConnector;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

/// GET endpoint to retrieve a map tile in MVT (Mapbox Vector Tile) format
#[axum::debug_handler]
pub async fn get_tile(
    RequestPath((layer_id, z, x, y)): RequestPath<(Uuid, u32, u32, u32)>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    // Get the vector connector from state
    let vector_connector = if let Some(vector_connector) = state.connection.as_vector() {
        vector_connector
    } else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": "Connection is not a vector connector"})),
        ));
    };

    // Get PostGIS connector reference
    let postgis_connector = vector_connector
        .as_any()
        .downcast_ref::<gridwalk_core::connector::postgis::PostgisConnector>()
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "Vector connector is not a PostGIS connector"})),
            )
        })?;

    // Get the tile data from PostGIS
    let tile_data = postgis_connector
        .get_tile(&layer_id, z, x, y)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": format!("Failed to get tile: {}", e)})),
            )
        })?;

    // Check if tile is empty
    if tile_data.is_empty() {
        return Ok((StatusCode::NO_CONTENT, HeaderMap::new(), Vec::new()));
    }

    // Prepare response headers for MVT
    let mut headers = HeaderMap::new();
    headers.insert(
        "content-type",
        HeaderValue::from_static("application/vnd.mapbox-vector-tile"),
    );
    headers.insert(
        "cache-control",
        HeaderValue::from_static("public, max-age=3600"), // Cache for 1 hour
    );
    headers.insert(
        "access-control-allow-origin",
        HeaderValue::from_static("*"), // Allow cross-origin requests for map tiles
    );

    Ok((StatusCode::OK, headers, tile_data))
}
