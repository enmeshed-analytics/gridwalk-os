use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Row};
use strum_macros::{Display, EnumString};
use uuid::Uuid;

#[derive(Clone, Debug, Display, Serialize, Deserialize, EnumString, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LayerStatus {
    Available,
    Hidden,
    Error,
    Cancelled,
    Failed,
}

#[derive(Clone, Debug, Display, Serialize, Deserialize, EnumString, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LayerCategory {
    Custom,
    OSM,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Layer {
    pub id: Uuid,
    pub status: LayerStatus,
    pub name: String,
    pub layer_category: LayerCategory,
    pub location_namespace: String,
    pub location_name: String,
    pub geometry_field: Option<String>,
    pub srid: Option<gridwalk_core::Srid>,
    pub metadata: Option<serde_json::Value>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> FromRow<'r, PgRow> for Layer {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(Layer {
            id: row.try_get("id")?,
            status: {
                let status_str: String = row.try_get("status")?;
                status_str.parse().map_err(|e| {
                    sqlx::Error::Decode(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid status value: {} - {}", status_str, e),
                    )))
                })?
            },
            name: row.try_get("name")?,
            layer_category: {
                let category_str: String = row.try_get("layer_category")?;
                category_str.parse().map_err(|e| {
                    sqlx::Error::Decode(Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid layer_category value: {} - {}", category_str, e),
                    )))
                })?
            },
            location_namespace: row.try_get("location_namespace")?,
            location_name: row.try_get("location_name")?,
            geometry_field: row.try_get::<Option<String>, _>("geometry_field")?,
            srid: {
                let srid_opt: Option<String> = row.try_get("srid")?;
                match srid_opt {
                    Some(srid_str) => Some(srid_str.parse().map_err(|e| {
                        sqlx::Error::Decode(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Invalid srid value: {} - {}", srid_str, e),
                        )))
                    })?),
                    None => None,
                }
            },
            metadata: row.try_get::<Option<serde_json::Value>, _>("metadata")?,
            created_at: row.try_get("created_at")?,
            updated_at: row.try_get("updated_at")?,
        })
    }
}

impl gridwalk_core::LayerCore for Layer {
    fn save<'e, E>(&self, executor: E) -> impl std::future::Future<Output = Result<()>> + Send
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        async move {
            // Query to insert a new row
            let query = "INSERT INTO gridwalk.layers (id, status, name, layer_category, location_namespace, location_name, metadata, created_at, updated_at) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
                         ON CONFLICT (id) DO UPDATE SET \
                         status = EXCLUDED.status, \
                         name = EXCLUDED.name, \
                         layer_category = EXCLUDED.layer_category, \
                         location_namespace = EXCLUDED.location_namespace, \
                         location_name = EXCLUDED.location_name, \
                         metadata = EXCLUDED.metadata, \
                         updated_at = EXCLUDED.updated_at";

            sqlx::query(query)
                .bind(self.id)
                .bind(self.status.to_string())
                .bind(&self.name)
                .bind(self.layer_category.to_string())
                .bind(&self.location_namespace)
                .bind(&self.location_name)
                .bind(&self.metadata)
                .bind(self.created_at)
                .bind(self.updated_at)
                .execute(executor)
                .await?;
            Ok(())
        }
    }

    fn list<'e, E>(
        limit: u64,
        offset: u64,
        executor: E,
    ) -> impl std::future::Future<Output = Result<Vec<Self>>> + Send
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        async move {
            let query = "SELECT * FROM gridwalk.layers ORDER BY created_at DESC LIMIT $1 OFFSET $2";

            let layers = sqlx::query_as::<_, Layer>(query)
                .bind(limit as i64)
                .bind(offset as i64)
                .fetch_all(executor)
                .await?;
            Ok(layers)
        }
    }

    fn get<'e, E>(id: Uuid, executor: E) -> impl std::future::Future<Output = Result<Self>> + Send
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        async move {
            let query = "SELECT * FROM gridwalk.layers WHERE id = $1";

            let layer = sqlx::query_as::<_, Layer>(query)
                .bind(id)
                .fetch_one(executor)
                .await?;
            Ok(layer)
        }
    }

    fn exists<'e, E>(
        id: Uuid,
        executor: E,
    ) -> impl std::future::Future<Output = Result<bool>> + Send
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        async move {
            let query = "SELECT EXISTS(SELECT 1 FROM gridwalk.layers WHERE id = $1) AS exists";

            let row: (bool,) = sqlx::query_as(query).bind(id).fetch_one(executor).await?;
            Ok(row.0)
        }
    }
}
