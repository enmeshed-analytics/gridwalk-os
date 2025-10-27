use crate::config::AppState;
use crate::layer::{Layer, LayerStatus};
use axum::{
    body::Bytes,
    extract::{Path as RequestPath, State},
    http::{HeaderMap, StatusCode, header::HeaderValue},
    response::IntoResponse,
};
use gdal::vector::LayerAccess;
use gridwalk_core::LayerCore;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::{fs, io::AsyncWriteExt};
use uuid::Uuid;

// TODO: Move GDAL processing and database insertion to background worker
// TODO: Implement HEAD handler to get upload status.
// PATCH (using TUS protocol) function to upload data to an existing layer
#[axum::debug_handler]
pub async fn patch_tus(
    RequestPath(layer_id): RequestPath<Uuid>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, (StatusCode, axum::Json<serde_json::Value>)> {
    // Validate TUS-Resumable header
    let _tus_version = headers
        .get("tus-resumable")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": "Missing Tus-Resumable header"})),
            )
        })?;

    // Validate Content-Type header
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": "Missing Content-Type header"})),
            )
        })?;

    if content_type != "application/offset+octet-stream" {
        return Err((
            StatusCode::BAD_REQUEST,
            axum::Json(json!({"error": "Content-Type must be 'application/offset+octet-stream'"})),
        ));
    }

    // Extract Upload-Offset header
    let upload_offset: i64 = headers
        .get("upload-offset")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": "Missing Upload-Offset header"})),
            )
        })?
        .parse()
        .map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({"error": "Upload-Offset must be a valid integer"})),
            )
        })?;

    // Get the layer from database
    let mut layer = Layer::get(layer_id, &*state.app_db).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("Database error: {}", e)})),
        )
    })?;

    // Validate that the layer is in uploading state
    if layer.status != LayerStatus::Uploading {
        return Err((
            StatusCode::CONFLICT,
            axum::Json(json!({"error": "Layer is not in uploading state"})),
        ));
    }

    // Validate upload offset matches current offset
    if upload_offset != layer.current_offset {
        return Err((
            StatusCode::CONFLICT,
            axum::Json(json!({
                "error": "Upload-Offset does not match current offset",
                "expected": layer.current_offset,
                "received": upload_offset
            })),
        ));
    }

    // Validate that we don't exceed the total size (if known)
    if let Some(total_size) = layer.total_size {
        if upload_offset + body.len() as i64 > total_size {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                axum::Json(json!({"error": "Upload would exceed declared file size"})),
            ));
        }
    }

    // Open the upload file and append data
    let upload_file_path = state.temp_data_path.join(layer.id.to_string());
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&upload_file_path)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": format!("Failed to open upload file: {}", e)})),
            )
        })?;

    // Write the data
    file.write_all(&body).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("Failed to write to upload file: {}", e)})),
        )
    })?;

    file.flush().await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("Failed to flush upload file: {}", e)})),
        )
    })?;

    // Update layer's current offset and updated_at timestamp
    layer.current_offset += body.len() as i64;
    layer.updated_at = chrono::Utc::now();

    // Check if upload is complete and process the file
    if let Some(total_size) = layer.total_size {
        if layer.current_offset >= total_size {
            layer.status = LayerStatus::Ready;

            // TODO: Move to background worker
            // GDAL processing and database insertion only for complete uploads
            let vector_connector = if let Some(vector_connector) = state.connection.as_vector() {
                vector_connector
            } else {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": "Connection is not a vector connector"})),
                ));
            };

            let dataset =
                gridwalk_core::file_utils::open_dataset(&upload_file_path).map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(
                            json!({"error": format!("Failed to open uploaded dataset: {}", e)}),
                        ),
                    )
                })?;

            let schema = gridwalk_core::file::extract_layer_schema(dataset, vector_connector)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        axum::Json(
                            json!({"error": format!("Failed to read uploaded file: {}", e)}),
                        ),
                    )
                })?;

            // Create the layer table in the connection database
            vector_connector.create_layer(&schema).await.map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": format!("Failed to create layer table: {}", e)})),
                )
            })?;

            println!("Upload complete for layer {}", layer.id);

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

            // Create a channel for streaming SQL statements from GDAL processing to database insertion
            let (sql_sender, mut sql_receiver) = mpsc::channel::<String>(100); // Buffer 100 statements

            let upload_file_path_clone = upload_file_path.clone();

            // Spawn blocking task for GDAL processing to avoid Send issues
            let gdal_handle = tokio::task::spawn_blocking(move || -> Result<(), String> {
                // Open dataset for reading layer definition and name
                let dataset_for_defn =
                    gridwalk_core::file_utils::open_dataset(&upload_file_path_clone)
                        .map_err(|e| format!("Failed to open dataset: {}", e))?;

                let layer_for_defn = dataset_for_defn
                    .into_layer(0)
                    .map_err(|e| format!("Failed to read layer: {}", e))?;

                let layer_defn = layer_for_defn.defn();
                let layer_name = layer_for_defn.name();

                // Open separate dataset for feature iteration
                let dataset = gridwalk_core::file_utils::open_dataset(&upload_file_path_clone)
                    .map_err(|e| format!("Failed to open dataset for features: {}", e))?;

                let layer = dataset
                    .into_layer(0)
                    .map_err(|e| format!("Failed to read layer for features: {}", e))?;

                let mut owned_feature_iterator = layer.owned_features();
                let mut feature_iter = owned_feature_iterator.into_iter();

                let mut feature_count = 0;
                while let Some(feature) = feature_iter.next() {
                    let insert_sql =
                        gridwalk_core::postgis::PostgisConnector::feature_to_insert_statement(
                            &feature,
                            &layer_defn,
                            "gridwalk_layer_data",
                            &layer_name,
                            None,
                        )
                        .map_err(|e| format!("SQL generation error: {}", e))?;

                    // Send the SQL statement through the channel
                    if sql_sender.blocking_send(insert_sql).is_err() {
                        return Err("Channel closed unexpectedly".to_string());
                    }

                    feature_count += 1;
                }

                if feature_count == 0 {
                    return Err("No features found in dataset".to_string());
                }

                println!(
                    "Processed {} features for layer {}",
                    feature_count, layer_name
                );
                Ok(())
            });

            // Start a transaction for database operations
            let mut tx = postgis_connector.pool.begin().await.map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": format!("Failed to start transaction: {}", e)})),
                )
            })?;

            // Process SQL statements as they arrive from the GDAL task
            let mut inserted_count = 0u64;
            let db_result = async {
                while let Some(sql) = sql_receiver.recv().await {
                    sqlx::query(&sql).execute(&mut *tx).await.map_err(|e| {
                        format!("Failed to insert feature {}: {}", inserted_count + 1, e)
                    })?;
                    inserted_count += 1;
                }
                Ok::<(), String>(())
            }
            .await;

            // Check if database operations failed
            if let Err(db_error) = db_result {
                let _ = tx.rollback().await;
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": db_error})),
                ));
            }

            // Wait for the GDAL processing to complete and handle any errors
            let gdal_result = gdal_handle.await;
            if let Err(join_error) = gdal_result {
                let _ = tx.rollback().await;
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(
                        json!({"error": format!("GDAL processing task failed: {}", join_error)}),
                    ),
                ));
            }

            if let Err(gdal_error) = gdal_result.unwrap() {
                let _ = tx.rollback().await;
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": gdal_error})),
                ));
            }

            // Commit the transaction
            tx.commit().await.map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(json!({"error": format!("Failed to commit transaction: {}", e)})),
                )
            })?;

            println!(
                "Successfully inserted {} features for layer {}",
                inserted_count, layer_id
            );
        }
    }
    // Always update layer status in the app database (to persist offset changes)
    layer.save(&*state.app_db).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(json!({"error": format!("Failed to update layer: {}", e)})),
        )
    })?;

    // Prepare response headers
    let mut response_headers = HeaderMap::new();
    response_headers.insert("tus-resumable", HeaderValue::from_static("1.0.0"));
    response_headers.insert(
        "upload-offset",
        HeaderValue::from_str(&layer.current_offset.to_string()).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(json!({"error": "Failed to create upload-offset header"})),
            )
        })?,
    );

    Ok((StatusCode::NO_CONTENT, response_headers))
}
