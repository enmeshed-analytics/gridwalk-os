use gridwalk_core::connector::Connector;
use gridwalk_core::connector::postgis::PostgresConfig;

use anyhow::Result;
use dotenvy::dotenv;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::env;
use std::fs;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;
use tracing::info;

#[derive(Clone)]
pub struct AppState {
    pub app_db: Arc<PgPool>,
    pub connection: Arc<Connector>,
    pub temp_data_path: Arc<PathBuf>,
}

impl AppState {
    pub async fn new(config: Config) -> Result<Self> {
        let app_db = create_app_db_pool(&config).await;
        let connector =
            gridwalk_core::connector::postgis::PostgisConnector::new(config.postgis_db_config)
                .await?;

        let mut connector = Connector::new_vector(Box::new(connector));
        // Do all validation at startup
        connector.test_connection().await?;

        Ok(Self {
            app_db,
            connection: Arc::new(connector),
            temp_data_path: config.temp_data_path,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub app_db_config: PostgresConfig,
    pub postgis_db_config: PostgresConfig,
    pub temp_data_path: Arc<PathBuf>,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Missing environment variable: {0}")]
    MissingVar(String),
    #[error("Invalid value for {0}: {1}")]
    InvalidValue(String, String),
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        // Load .env file if it exists
        dotenv().ok();

        // Construct database URL from individual components
        let user = env::var("DATABASE_USER")
            .map_err(|_| ConfigError::MissingVar("DATABASE_USER".to_string()))?;
        let password = env::var("DATABASE_PASSWORD")
            .map_err(|_| ConfigError::MissingVar("DATABASE_PASSWORD".to_string()))?;
        let host = env::var("DATABASE_HOST")
            .map_err(|_| ConfigError::MissingVar("DATABASE_HOST".to_string()))?;
        let database_name = env::var("DATABASE_NAME")
            .map_err(|_| ConfigError::MissingVar("DATABASE_NAME".to_string()))?;
        let port = env::var("DATABASE_PORT").unwrap_or_else(|_| "5432".to_string());

        let port = port.parse::<u16>().map_err(|e: ParseIntError| {
            ConfigError::InvalidValue("DATABASE_PORT".to_string(), e.to_string())
        })?;
        let disable_ssl = env::var("DATABASE_DISABLE_SSL")
            .unwrap_or_else(|_| "false".to_string())
            .to_lowercase()
            == "true";

        let max_app_db_connections = env::var("DATABASE_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "20".to_string())
            .parse::<u32>()
            .map_err(|e: ParseIntError| {
                ConfigError::InvalidValue("PG_MAX_CONNECTIONS".to_string(), e.to_string())
            })?;

        let max_postgis_connections = env::var("POSTGIS_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u32>()
            .map_err(|e: ParseIntError| {
                ConfigError::InvalidValue("POSTGIS_MAX_CONNECTIONS".to_string(), e.to_string())
            })?;

        let app_db_schema = env::var("DATABASE_SCHEMA").unwrap_or_else(|_| "public".to_string());
        let postgis_layer_schema =
            env::var("LAYER_SCHEMA").unwrap_or_else(|_| "gridwalk_layer_data".to_string());

        let app_db_config = PostgresConfig {
            user,
            password,
            host,
            port,
            database_name,
            schema: app_db_schema,
            max_connections: max_app_db_connections,
            disable_ssl,
        };

        let postgis_db_config = PostgresConfig {
            user: app_db_config.user.clone(),
            password: app_db_config.password.clone(),
            host: app_db_config.host.clone(),
            port: app_db_config.port,
            database_name: app_db_config.database_name.clone(),
            schema: postgis_layer_schema,
            max_connections: max_postgis_connections,
            disable_ssl: app_db_config.disable_ssl,
        };

        // Dir for layers before upload to DB
        let temp_data_path_str = env::var("TEMP_DATA_PATH").unwrap_or_else(|_| "/tmp".to_string());
        let temp_data_path_buf = PathBuf::from(&temp_data_path_str);

        // Ensure the directory exists
        if !temp_data_path_buf.exists() {
            info!(
                "Creating TEMP_DATA_PATH directory at {:?}",
                temp_data_path_buf
            );
            fs::create_dir_all(&temp_data_path_buf).map_err(|e| {
                ConfigError::InvalidValue(
                    "TEMP_DATA_PATH".to_string(),
                    format!("Failed to create directory: {}", e),
                )
            })?;
        }

        let temp_data_path = Arc::new(temp_data_path_buf);

        Ok(Config {
            app_db_config,
            postgis_db_config,
            temp_data_path,
        })
    }
}

pub async fn create_app_db_pool(config: &Config) -> Arc<PgPool> {
    let database_url = format!(
        "postgresql://{}:{}@{}:{}/{}",
        config.app_db_config.user,
        config.app_db_config.password,
        config.app_db_config.host,
        config.app_db_config.port,
        config.app_db_config.database_name
    );

    Arc::new(
        PgPoolOptions::new()
            .max_connections(config.app_db_config.max_connections)
            .connect(&database_url)
            .await
            .expect("Failed to create pool"),
    )
}
