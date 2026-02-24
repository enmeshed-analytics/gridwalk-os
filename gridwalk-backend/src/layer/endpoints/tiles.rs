use crate::config::AppState;
use crate::error::{internal_error, not_found_error};
use axum::{
    extract::{Path as RequestPath, State},
    http::{HeaderMap, StatusCode, header::HeaderValue},
    response::IntoResponse,
};
use gridwalk_core::{LayerCore, VectorConnector};
use serde_json::json;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

/// GET endpoint to retrieve a map tile in MVT (Mapbox Vector Tile) format
#[axum::debug_handler]
pub async fn get_tile(
    RequestPath((layer_id, z, x, y)): RequestPath<(Uuid, u32, u32, u32)>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    // TODO: Cache layer in memory to avoid repeated DB lookups for each tile request
    let layer = crate::layer::Layer::get(layer_id, &*state.app_db)
        .await
        .map_err(|e| not_found_error("Layer not found", e))?;

    // Check if zoom level is within the layer's supported range
    if let Some(min_zoom) = layer.min_zoom {
        if z < min_zoom as u32 {
            println!("Zoom level {} is below min_zoom {}", z, min_zoom);
            return Ok((StatusCode::NO_CONTENT, HeaderMap::new(), Vec::new()));
        }
    }
    if let Some(max_zoom) = layer.max_zoom {
        if z > max_zoom as u32 {
            println!("Zoom level {} is above max_zoom {}", z, max_zoom);
            return Ok((StatusCode::NO_CONTENT, HeaderMap::new(), Vec::new()));
        }
    }

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

    let layer_source = gridwalk_core::LayerSource::Database {
        namespace: layer.location_namespace.clone(),
        name: layer.location_name.clone(),
        geometry_field: layer
            .geometry_field
            .clone()
            .unwrap_or("geometry".to_string()),
        srid: layer.srid.unwrap_or(gridwalk_core::Srid::EPSG4326),
    };

    let tile_layer_config = vec![gridwalk_core::TileLayerConfig {
        source: layer_source,
        layer_name: layer.name,
        attributes: None, // Fetch all attributes
    }];

    debug!("Using PostGIS connector to fetch tile data");
    // Get the tile data from PostGIS
    let tile_data = postgis_connector
        .get_tile(&tile_layer_config, z, x, y)
        .await
        .map_err(|e| internal_error("Failed to get tile", e))?;
    debug!("Tile data length: {}", tile_data.len());

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
