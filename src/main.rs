mod config;
mod layer;

use anyhow::Result;
use axum::{
    Router,
    routing::{get, patch, post},
};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for logging
    tracing_subscriber::fmt().with_ansi(false).init();

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
        .route("/layers", post(layer::post_tus))
        .route("/layers/:layer_id", patch(layer::patch_tus))
        .route("/layers/:layer_id/tiles/:z/:x/:y", get(layer::get_tile))
        .with_state(std::sync::Arc::new(app_state));

    // Start the Axum server
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
    info!("Server listening on {}", listener.local_addr()?);
    axum::serve(listener, router).await?;

    Ok(())
}
