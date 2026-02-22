use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine as _, engine::general_purpose};
use rand::Rng;
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

/// Encrypts a string using AES-GCM encryption
/// Returns base64 encoded string containing nonce + ciphertext
pub fn encrypt_string(
    plaintext: &str,
    encryption_key: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Derive a 256-bit key from the encryption key using SHA-256
    let mut hasher = <Sha256 as Digest>::new();
    hasher.update(encryption_key.as_bytes());
    let key_bytes = hasher.finalize();
    let key: Key<Aes256Gcm> = key_bytes.into();

    // Create cipher instance
    let cipher = Aes256Gcm::new(&key);

    // Generate a random 96-bit nonce
    let mut rng = rand::rng();
    let nonce_bytes: [u8; 12] = rng.random();
    let nonce = Nonce::from(nonce_bytes);

    // Encrypt the plaintext
    let ciphertext = cipher
        .encrypt(&nonce, plaintext.as_bytes())
        .map_err(|e| format!("Encryption failed: {}", e))?;

    // Combine nonce + ciphertext and encode as base64
    let mut result = Vec::new();
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);

    Ok(general_purpose::STANDARD.encode(result))
}

/// Decrypts a base64 encoded string containing nonce + ciphertext
/// Returns the original plaintext string
pub fn decrypt_string(encrypted_data: &str, encryption_key: &str) -> Result<String, String> {
    // Base64 decode the input
    let combined_data = general_purpose::STANDARD
        .decode(encrypted_data)
        .map_err(|e| format!("Failed to decode base64: {}", e))?;

    // Check minimum length (12 bytes for nonce + at least some ciphertext)
    if combined_data.len() < 12 {
        return Err("Invalid encrypted data: too short".to_string());
    }

    // Extract nonce (first 12 bytes) and ciphertext (remaining bytes)
    let (nonce_bytes, ciphertext) = combined_data.split_at(12);
    let nonce = Nonce::from(<[u8; 12]>::try_from(nonce_bytes).unwrap());

    // Derive the same 256-bit key from the encryption key using SHA-256
    let mut hasher = <Sha256 as Digest>::new();
    hasher.update(encryption_key.as_bytes());
    let key_bytes = hasher.finalize();
    let key: Key<Aes256Gcm> = key_bytes.into();

    // Create cipher instance
    let cipher = Aes256Gcm::new(&key);

    // Decrypt the ciphertext
    let plaintext_bytes = cipher
        .decrypt(&nonce, ciphertext)
        .map_err(|e| format!("Decryption failed: {}", e))?;

    // Convert bytes back to string
    String::from_utf8(plaintext_bytes)
        .map_err(|e| format!("Failed to convert decrypted data to string: {}", e))
}

pub async fn save_os_credentials<'e, E>(
    executor: E,
    encryption_key: &str,
    api_key: &str,
    api_secret: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let encrypted_api_key = encrypt_string(api_key, encryption_key)?;
    let encrypted_api_secret = encrypt_string(api_secret, encryption_key)?;

    let credentials = json!({
        "api_key": encrypted_api_key,
        "api_secret": encrypted_api_secret
    });

    let query = "INSERT INTO gridwalk.external_services (id, service_name, credentials, created_at, updated_at) \
                 VALUES ($1, $2, $3, $4, $5) \
                 ON CONFLICT (service_name) DO UPDATE SET \
                 credentials = EXCLUDED.credentials, \
                 updated_at = EXCLUDED.updated_at";

    sqlx::query(query)
        .bind(Uuid::new_v4())
        .bind("uk_ordnance_survey")
        .bind(credentials)
        .bind(chrono::Utc::now())
        .bind(chrono::Utc::now())
        .execute(executor)
        .await?;
    Ok(())
}

pub async fn get_os_credentials<'e, E>(
    executor: E,
    encryption_key: &str,
) -> Result<(String, String), Box<dyn std::error::Error + Send + Sync>>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    let row: (serde_json::Value,) = sqlx::query_as(
        "SELECT credentials FROM gridwalk.external_services WHERE service_name = $1",
    )
    .bind("uk_ordnance_survey")
    .fetch_one(executor)
    .await?;

    let credentials = &row.0;
    let encrypted_api_key = credentials
        .get("api_key")
        .and_then(|v| v.as_str())
        .ok_or("Missing api_key in credentials")?;
    let encrypted_api_secret = credentials
        .get("api_secret")
        .and_then(|v| v.as_str())
        .ok_or("Missing api_secret in credentials")?;

    let api_key = decrypt_string(encrypted_api_key, encryption_key)?;
    let api_secret = decrypt_string(encrypted_api_secret, encryption_key)?;

    Ok((api_key, api_secret))
}
