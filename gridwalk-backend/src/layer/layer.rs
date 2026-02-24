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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Layer {
    pub id: Uuid,
    pub status: LayerStatus,
    pub name: String,
    pub layer_category: gridwalk_core::LayerCategory,
    pub location_namespace: String,
    pub location_name: String,
    pub geometry_field: Option<String>,
    pub srid: Option<gridwalk_core::Srid>,
    pub min_zoom: Option<u8>,
    pub max_zoom: Option<u8>,
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
                let srid_opt: Option<i32> = row.try_get("srid")?;
                match srid_opt {
                    Some(srid_val) => Some(srid_val.to_string().parse().map_err(|e| {
                        sqlx::Error::Decode(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Invalid srid value: {} - {}", srid_val, e),
                        )))
                    })?),
                    None => None,
                }
            },
            min_zoom: row.try_get::<Option<i32>, _>("min_zoom")?.map(|v| v as u8),
            max_zoom: row.try_get::<Option<i32>, _>("max_zoom")?.map(|v| v as u8),
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
            let query = "INSERT INTO gridwalk.layers (id, status, name, layer_category, location_namespace, location_name, geometry_field, srid, min_zoom, max_zoom, metadata, created_at, updated_at) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) \
                         ON CONFLICT (id) DO UPDATE SET \
                         status = EXCLUDED.status, \
                         name = EXCLUDED.name, \
                         layer_category = EXCLUDED.layer_category, \
                         location_namespace = EXCLUDED.location_namespace, \
                         location_name = EXCLUDED.location_name, \
                         geometry_field = EXCLUDED.geometry_field, \
                         srid = EXCLUDED.srid, \
                         min_zoom = EXCLUDED.min_zoom, \
                         max_zoom = EXCLUDED.max_zoom, \
                         metadata = EXCLUDED.metadata, \
                         updated_at = EXCLUDED.updated_at";

            sqlx::query(query)
                .bind(self.id)
                .bind(self.status.to_string())
                .bind(&self.name)
                .bind(self.layer_category.to_string())
                .bind(&self.location_namespace)
                .bind(&self.location_name)
                .bind(&self.geometry_field)
                .bind(self.srid.map(i32::from))
                .bind(self.min_zoom.map(|v| v as i16))
                .bind(self.max_zoom.map(|v| v as i16))
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
        layer_category: gridwalk_core::LayerCategory,
        executor: E,
    ) -> impl std::future::Future<Output = Result<Vec<Self>>> + Send
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        async move {
            let query = "SELECT * FROM gridwalk.layers WHERE layer_category = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3";

            let layers = sqlx::query_as::<_, Layer>(query)
                .bind(layer_category.to_string())
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
