mod basemap;
mod config;
mod layer;

use anyhow::Result;
use axum::{
    Router,
    routing::{get, head, patch, post},
};
use tower_http::cors::CorsLayer;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let config = config::Config::from_env()?;
    let app_state = config::AppState::new(config).await?;

    sqlx::migrate!("./migrations")
        .run(&*app_state.app_db)
        .await
        .expect("Failed to run migrations");

    // Create filter to only look at public schema for layers
    let sources = app_state.connection.list_sources().await?;

    println!("Available sources in schema:");
    for source in sources {
        println!("- {}", source);
    }

    let router = Router::new()
        .route("/service/os/credentials", post(basemap::set_os_credentials))
        .route("/service/os/token", get(basemap::get_os_token))
        .route("/layers/upload", post(layer::post_tus))
        .route("/layers/upload/:layer_id", patch(layer::patch_tus))
        .route("/layers/upload/:layer_id", head(layer::head_tus))
        .route("/layers/register", post(layer::register_layer))
        .route("/layers", get(layer::get_layers))
        .route("/layers/:layer_id/tiles/:z/:x/:y", get(layer::get_tile))
        .route("/layers/register_osm", post(layer::register_osm))
        .route("/layers/osm/tiles/:z/:x/:y", get(layer::get_osm_tile))
        .with_state(std::sync::Arc::new(app_state))
        .layer(CorsLayer::permissive()); // Allow CORS for UI requests

    // Start the Axum server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
    info!("Server listening on {}", listener.local_addr()?);
    axum::serve(listener, router).await?;

    Ok(())
}
