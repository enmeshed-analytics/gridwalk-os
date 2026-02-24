use crate::config::AppState;
use crate::error::internal_error;
use axum::{
    extract::{Path as RequestPath, State},
    http::{HeaderMap, StatusCode, header::HeaderValue},
    response::IntoResponse,
};
use gridwalk_core::{LayerCore, VectorConnector};
use serde_json::json;
use std::sync::Arc;
use tracing::debug;

/// GET endpoint to retrieve an OSM map tile in MVT (Mapbox Vector Tile) format
#[axum::debug_handler]
pub async fn get_osm_tile(
    RequestPath((z, x, y)): RequestPath<(u32, u32, u32)>,
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

    // List OSM layers from the database
    let layers = crate::layer::Layer::list(
        100, // limit
        0,   // offset
        gridwalk_core::LayerCategory::OSM,
        &*state.app_db,
    )
    .await
    .map_err(|e| internal_error("Failed to list OSM layers", e))?;

    // Build tile layer configs from OSM layers that match the zoom level
    let tile_layer_config: Vec<gridwalk_core::TileLayerConfig> = layers
        .iter()
        .filter(|layer| {
            let above_min = layer.min_zoom.map_or(true, |min| z >= min as u32);
            let below_max = layer.max_zoom.map_or(true, |max| z <= max as u32);
            above_min && below_max
        })
        .map(|layer| {
            let layer_source = gridwalk_core::LayerSource::Database {
                namespace: layer.location_namespace.clone(),
                name: layer.location_name.clone(),
                geometry_field: layer
                    .geometry_field
                    .clone()
                    .unwrap_or_else(|| "geometry".to_string()),
                srid: layer.srid.unwrap_or(gridwalk_core::Srid::EPSG4326),
            };
            gridwalk_core::TileLayerConfig {
                source: layer_source,
                layer_name: layer.name.clone(),
                attributes: None, // Fetch all attributes
            }
        })
        .collect();

    // Get the tile data from PostGIS
    let tile_data = postgis_connector
        .get_tile(&tile_layer_config, z, x, y)
        .await
        .map_err(|e| internal_error("Failed to get tile", e))?;
    let layer_names: Vec<&str> = tile_layer_config
        .iter()
        .map(|c| c.layer_name.as_str())
        .collect();
    debug!(
        "Serving tile z={} x={} y={}, layers: {:?}, size: {} bytes",
        z,
        x,
        y,
        layer_names,
        tile_data.len()
    );

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
