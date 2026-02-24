use crate::config::AppState;
use crate::error::internal_error;
use crate::layer::{Layer, LayerStatus};
use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use gridwalk_core::LayerCore;
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

#[derive(serde::Deserialize, Default, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TableType {
    #[default]
    Base,
    Materialized,
}

#[derive(serde::Deserialize)]
pub struct RegisterOsmRequest {
    pub schema_name: String,
    pub srid: Option<gridwalk_core::Srid>,
    #[serde(default)]
    pub table_type: TableType,
    #[serde(default = "default_geometry_field")]
    pub geometry_field: String,
}

fn default_geometry_field() -> String {
    "geom".to_string() // osm2pgsql default geometry field
}

/// Maps imposm3 table names to OSM Liberty style source-layer names
/// Expected source-layers: park, landuse, landcover, waterway, water, water_name,
/// aeroway, transportation, transportation_name, building, boundary, poi, place
fn map_to_liberty_style_name(table_name: &str) -> String {
    let lower = table_name.to_lowercase();

    // Order matters: more specific patterns first
    //
    // From imposm mapping.yaml tables:
    // - water_polygon -> water
    // - waterway_linestring, waterway_relation -> waterway
    // - landcover_polygon -> landcover
    // - landuse_polygon -> landuse
    // - park_polygon -> park
    // - border_linestring, boundary_polygon -> boundary
    // - aeroway_polygon, aeroway_linestring, aeroway_point, aerodrome_label_point -> aeroway
    // - highway_*, railway_*, aerialway_*, shipway_* -> transportation
    // - route_member -> transportation_name
    // - building_polygon, building_relation -> building
    // - marine_point -> water_name
    // - continent_point, country_point, island_*, state_point, city_point -> place
    // - poi_point, poi_polygon -> poi
    // - peak_point, mountain_linestring, housenumber_point -> (no direct mapping)

    if lower.contains("route_member") {
        "transportation_name".to_string()
    } else if lower.contains("marine_point") {
        "water_name".to_string()
    } else if lower.contains("waterway") {
        "waterway".to_string()
    } else if lower.contains("water_polygon") || lower.contains("ocean") {
        "water".to_string()
    } else if lower.contains("landcover") {
        "landcover".to_string()
    } else if lower.contains("landuse") {
        "landuse".to_string()
    } else if lower.contains("aerodrome") || lower.contains("aeroway") {
        "aeroway".to_string()
    } else if lower.contains("highway")
        || lower.contains("railway")
        || lower.contains("aerialway")
        || lower.contains("shipway")
    {
        "transportation".to_string()
    } else if lower.contains("building") {
        "building".to_string()
    } else if lower.contains("border") || lower.contains("boundary") {
        "boundary".to_string()
    } else if lower.contains("poi") {
        "poi".to_string()
    } else if lower.contains("continent")
        || lower.contains("country")
        || lower.contains("island")
        || lower.contains("state_point")
        || lower.contains("city")
    {
        "place".to_string()
    } else if lower.contains("park") {
        "park".to_string()
    } else {
        // Tables without direct mapping: peak_point, mountain_linestring, housenumber_point
        table_name.to_string()
    }
}

/// Extracts zoom level constraints from table names with zoom suffixes
/// Handles explicit zoom range patterns:
/// - `_z{N}_z{M}` (e.g., layer_z0_z5) - zoom N to M inclusive
/// - `_z{N}_plus` (e.g., layer_z14_plus) - zoom N and above
/// Base tables (no suffix) are used at all zoom levels
fn extract_zoom_levels(table_name: &str) -> (Option<u8>, Option<u8>) {
    let lower = table_name.to_lowercase();

    // Check for _z{N}_plus pattern (e.g., layer_z14_plus)
    if lower.ends_with("_plus") {
        let without_plus = &lower[..lower.len() - 5]; // Remove "_plus"
        if let Some(pos) = without_plus.rfind("_z") {
            let zoom_str: String = without_plus[pos + 2..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if !zoom_str.is_empty() && pos + 2 + zoom_str.len() == without_plus.len() {
                if let Ok(min_zoom) = zoom_str.parse::<u8>() {
                    return (Some(min_zoom), None);
                }
            }
        }
    }

    // Check for _z{N}_z{M} pattern (e.g., layer_z0_z5, layer_z6_z8)
    if let Some(last_z_pos) = lower.rfind("_z") {
        let max_zoom_str: String = lower[last_z_pos + 2..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .collect();
        // Ensure the max zoom string goes to the end
        if !max_zoom_str.is_empty() && last_z_pos + 2 + max_zoom_str.len() == lower.len() {
            // Look for the first _z{N} pattern before the last one
            let before_last = &lower[..last_z_pos];
            if let Some(first_z_pos) = before_last.rfind("_z") {
                let min_zoom_str: String = before_last[first_z_pos + 2..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                // Ensure min zoom string reaches the end of before_last
                if !min_zoom_str.is_empty()
                    && first_z_pos + 2 + min_zoom_str.len() == before_last.len()
                {
                    if let (Ok(min_zoom), Ok(max_zoom)) =
                        (min_zoom_str.parse::<u8>(), max_zoom_str.parse::<u8>())
                    {
                        return (Some(min_zoom), Some(max_zoom));
                    }
                }
            }
        }
    }

    // Base tables (no zoom suffix): do not constrain zoom levels.
    (None, None)
}

/// POST endpoint to register OSM layers from a PostGIS schema
#[axum::debug_handler]
pub async fn register_osm(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<RegisterOsmRequest>,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    // Query tables in the specified schema
    let tables: Vec<(String,)> = match payload.table_type {
        TableType::Base => {
            let query = r#"
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = $1
                AND table_type = 'BASE TABLE'
                ORDER BY table_name
            "#;
            sqlx::query_as(query)
                .bind(&payload.schema_name)
                .fetch_all(&*state.app_db)
                .await
        }
        TableType::Materialized => {
            let query = r#"
                SELECT matviewname AS table_name
                FROM pg_matviews
                WHERE schemaname = $1
                ORDER BY matviewname
            "#;
            sqlx::query_as(query)
                .bind(&payload.schema_name)
                .fetch_all(&*state.app_db)
                .await
        }
    }
    .map_err(|e| internal_error("Failed to query schema tables", e))?;

    if tables.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            axum::Json(
                json!({"error": format!("No tables found in schema: {}", payload.schema_name)}),
            ),
        ));
    }

    // Create Layer structs for each table
    let mut layers: Vec<Layer> = Vec::new();
    let now = chrono::Utc::now();

    // With explicit zoom range naming (e.g., layer_z0_z5, layer_z6_z8, layer_z14_plus),
    // zoom levels are directly extracted from the table name - no need to compute ranges.

    for (table_name,) in tables {
        let lower = table_name.to_lowercase();
        let (min_zoom, max_zoom) = extract_zoom_levels(&table_name);

        // Extract base name from zoom-suffixed table for the layer name
        // Handles: layer_z0_z5, layer_z6_z8, layer_z14_plus
        let base_name: Option<String> = if lower.ends_with("_plus") {
            // Pattern: layer_z14_plus
            let without_plus = &lower[..lower.len() - 5];
            if let Some(pos) = without_plus.rfind("_z") {
                let zoom_str: String = without_plus[pos + 2..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                if !zoom_str.is_empty() && pos + 2 + zoom_str.len() == without_plus.len() {
                    Some(without_plus[..pos].to_string())
                } else {
                    None
                }
            } else {
                None
            }
        } else if let Some(last_z_pos) = lower.rfind("_z") {
            // Pattern: layer_z0_z5
            let max_zoom_str: String = lower[last_z_pos + 2..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if !max_zoom_str.is_empty() && last_z_pos + 2 + max_zoom_str.len() == lower.len() {
                let before_last = &lower[..last_z_pos];
                if let Some(first_z_pos) = before_last.rfind("_z") {
                    let min_zoom_str: String = before_last[first_z_pos + 2..]
                        .chars()
                        .take_while(|c| c.is_ascii_digit())
                        .collect();
                    if !min_zoom_str.is_empty()
                        && first_z_pos + 2 + min_zoom_str.len() == before_last.len()
                    {
                        Some(before_last[..first_z_pos].to_string())
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let layer = Layer {
            id: Uuid::new_v4(),
            status: LayerStatus::Available,
            name: base_name.clone().unwrap_or_else(|| table_name.clone()),
            layer_category: gridwalk_core::LayerCategory::OSM,
            location_namespace: payload.schema_name.clone(),
            location_name: table_name,
            geometry_field: Some(payload.geometry_field.clone()),
            srid: payload.srid,
            min_zoom,
            max_zoom,
            metadata: None,
            created_at: now,
            updated_at: now,
        };
        layers.push(layer);
    }

    // Persist layers to the database
    for layer in &layers {
        layer
            .save(&*state.app_db)
            .await
            .map_err(|e| internal_error("Failed to save layer", e))?;
    }

    Ok(axum::Json(json!({
        "message": "OSM layers created successfully",
        "total_layers": layers.len()
    })))
}
