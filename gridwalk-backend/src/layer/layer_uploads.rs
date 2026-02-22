use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Row};
use strum_macros::{Display, EnumString};
use uuid::Uuid;

#[derive(Clone, Debug, Display, Serialize, Deserialize, EnumString, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum LayerUploadStatus {
    Uploading,  // Initial upload phase - still receiving data
    Uploaded,   // Upload complete, ready for processing
    Processing, // Data received, processing layer data (converting to geospatial data store format)
    Processed,  // Processing complete
    Publishing, // Publishing to data store
    Error,
    Cancelled,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerUpload {
    pub id: Uuid,
    pub status: LayerUploadStatus,
    pub name: String,
    pub upload_type: Option<String>,
    pub total_size: Option<i64>,
    pub current_offset: i64,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl<'r> FromRow<'r, PgRow> for LayerUpload {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        Ok(LayerUpload {
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

impl LayerUpload {
    pub async fn save<'e, E>(&self, executor: E) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        // Query to insert a new row
        let query = "INSERT INTO gridwalk.layer_uploads (id, status, name, upload_type, total_size, current_offset, created_at, updated_at) \
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

    pub async fn get<'e, E>(executor: E, id: Uuid) -> Result<Self>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let row: Self = sqlx::query_as("SELECT * FROM gridwalk.layer_uploads WHERE id = $1")
            .bind(id)
            .fetch_one(executor)
            .await?;
        Ok(row)
    }

    pub async fn delete<'e, E>(&self, executor: E) -> Result<()>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        sqlx::query("DELETE FROM gridwalk.layer_uploads WHERE id = $1")
            .bind(self.id)
            .execute(executor)
            .await?;
        Ok(())
    }
}
