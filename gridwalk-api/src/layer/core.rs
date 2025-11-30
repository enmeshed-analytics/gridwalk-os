use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Row};
use strum_macros::{Display, EnumString};
use uuid::Uuid;

#[derive(Clone, Debug, Display, Serialize, Deserialize, EnumString, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LayerStatus {
    Uploading,
    Processing,
    Ready,
    Error,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Layer {
    pub id: Uuid,
    pub status: LayerStatus,
    pub name: String,
    pub upload_type: Option<String>,
    pub total_size: Option<i64>,
    pub current_offset: i64,
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
            upload_type: row.try_get("upload_type")?,
            total_size: row.try_get::<Option<i64>, _>("total_size")?,
            current_offset: row.try_get::<i64, _>("current_offset")?,
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
            let query = "INSERT INTO gridwalk.layers (id, status, name, upload_type, total_size, current_offset, created_at, updated_at) \
                         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
                         ON CONFLICT (id) DO UPDATE SET \
                         status = EXCLUDED.status, \
                         name = EXCLUDED.name, \
                         upload_type = EXCLUDED.upload_type, \
                         total_size = EXCLUDED.total_size, \
                         current_offset = EXCLUDED.current_offset, \
                         updated_at = EXCLUDED.updated_at";

            sqlx::query(query)
                .bind(self.id)
                .bind(self.status.to_string())
                .bind(&self.name)
                .bind(&self.upload_type)
                .bind(self.total_size)
                .bind(self.current_offset)
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
}
