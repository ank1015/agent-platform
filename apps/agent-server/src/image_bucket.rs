use agent_contracts::{AssetPublisher, AssetUpload, PublishedAsset};
use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{Path, State},
    http::{HeaderValue, StatusCode, header},
    response::Response,
    routing::get,
};
use bytes::Bytes as ByteBuffer;
use google_cloud_storage::client::Storage;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use url::Url;

const MAX_IMAGE_BYTES: usize = 5 * 1024 * 1024;
const IMAGE_PATH: &str = "/media/images/{key}";

#[derive(Clone)]
pub struct LocalImageBucket {
    public_base_url: String,
    images: Arc<RwLock<HashMap<String, StoredImage>>>,
}

#[derive(Clone)]
struct StoredImage {
    content_type: HeaderValue,
    bytes: Bytes,
}

impl LocalImageBucket {
    pub fn new(public_base_url: impl Into<String>) -> Result<Self, String> {
        let public_base_url = normalize_public_base_url(&public_base_url.into())?;
        Ok(Self {
            public_base_url,
            images: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    pub fn validate_public_base_url(value: &str) -> Result<(), String> {
        normalize_public_base_url(value).map(|_| ())
    }

    fn url(&self, key: &str) -> String {
        format!("{}/media/images/{key}", self.public_base_url)
    }
}

#[async_trait]
impl AssetPublisher for LocalImageBucket {
    async fn publish(&self, image: AssetUpload) -> Result<PublishedAsset, String> {
        let prepared = prepare_upload(&image)?;
        let content_type = HeaderValue::from_str(&image.content_type)
            .map_err(|_| "invalid image content type".to_string())?;
        let stored = StoredImage {
            content_type,
            bytes: Bytes::copy_from_slice(image.bytes.as_ref()),
        };
        let mut images = self.images.write().await;
        if let Some(existing) = images.get(&prepared.key) {
            if existing.bytes != stored.bytes || existing.content_type != stored.content_type {
                return Err("content-addressed image key already contains different data".into());
            }
        } else {
            images.insert(prepared.key.clone(), stored);
        }
        Ok(PublishedAsset {
            url: self.url(&prepared.key),
            sha256: prepared.sha256,
            size_bytes: image.bytes.len(),
        })
    }
}

pub struct GcsImageBucket {
    client: Storage,
    http: reqwest::Client,
    bucket: String,
    bucket_resource: String,
    public_base_url: String,
}

impl GcsImageBucket {
    pub async fn new(bucket: impl Into<String>) -> Result<Self, String> {
        let bucket = bucket.into();
        validate_bucket_name(&bucket)?;
        let client = Storage::builder()
            .build()
            .await
            .map_err(|error| format!("could not initialize Google Cloud Storage: {error}"))?;
        Ok(Self {
            client,
            http: reqwest::Client::new(),
            bucket_resource: format!("projects/_/buckets/{bucket}"),
            public_base_url: format!("https://storage.googleapis.com/{bucket}"),
            bucket,
        })
    }

    pub fn validate_bucket_name(value: &str) -> Result<(), String> {
        validate_bucket_name(value)
    }

    fn url(&self, key: &str) -> String {
        format!("{}/{key}", self.public_base_url)
    }

    async fn publicly_exists(&self, url: &str) -> Result<bool, String> {
        let response = self
            .http
            .head(url)
            .send()
            .await
            .map_err(|error| format!("could not check published image: {error}"))?;
        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            status => Err(format!(
                "public image lookup returned unexpected HTTP status {status}"
            )),
        }
    }
}

#[async_trait]
impl AssetPublisher for GcsImageBucket {
    async fn publish(&self, image: AssetUpload) -> Result<PublishedAsset, String> {
        let prepared = prepare_upload(&image)?;
        let url = self.url(&prepared.key);
        if self.publicly_exists(&url).await? {
            return Ok(PublishedAsset {
                url,
                sha256: prepared.sha256,
                size_bytes: image.bytes.len(),
            });
        }

        let upload = self
            .client
            .write_object(
                self.bucket_resource.clone(),
                prepared.key.clone(),
                ByteBuffer::copy_from_slice(image.bytes.as_ref()),
            )
            .set_if_generation_match(0)
            .set_cache_control("public,max-age=31536000,immutable")
            .set_content_disposition("inline")
            .set_content_type(image.content_type.clone())
            .send_buffered()
            .await;
        if let Err(error) = upload
            && !self.publicly_exists(&url).await?
        {
            return Err(format!(
                "could not upload image to Google Cloud Storage bucket `{}`: {error}",
                self.bucket
            ));
        }
        Ok(PublishedAsset {
            url,
            sha256: prepared.sha256,
            size_bytes: image.bytes.len(),
        })
    }
}

struct PreparedUpload {
    key: String,
    sha256: String,
}

fn prepare_upload(image: &AssetUpload) -> Result<PreparedUpload, String> {
    if image.bytes.is_empty() || image.bytes.len() > MAX_IMAGE_BYTES {
        return Err("image must contain 1 byte through 5 MiB".into());
    }
    let extension = match image.content_type.as_str() {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        value => return Err(format!("unsupported image content type `{value}`")),
    };
    let sha256 = format!("{:x}", Sha256::digest(&image.bytes));
    Ok(PreparedUpload {
        key: format!("{sha256}.{extension}"),
        sha256,
    })
}

fn validate_bucket_name(value: &str) -> Result<(), String> {
    let bytes = value.as_bytes();
    if !(3..=63).contains(&bytes.len())
        || !bytes
            .first()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        || !bytes
            .last()
            .is_some_and(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
        || !bytes.iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
    {
        return Err("invalid Google Cloud Storage bucket name".into());
    }
    Ok(())
}

pub(crate) fn router(bucket: Arc<LocalImageBucket>) -> Router<crate::AppState> {
    Router::new()
        .route(IMAGE_PATH, get(get_image))
        .with_state(bucket)
}

async fn get_image(
    State(bucket): State<Arc<LocalImageBucket>>,
    Path(key): Path<String>,
) -> Response {
    if validate_key(&key).is_err() {
        return not_found();
    }
    let Some(image) = bucket.images.read().await.get(&key).cloned() else {
        return not_found();
    };
    let mut response = Response::new(Body::from(image.bytes));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, image.content_type);
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    response.headers_mut().insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

fn not_found() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .expect("static image not-found response is valid")
}

fn normalize_public_base_url(value: &str) -> Result<String, String> {
    let url =
        Url::parse(value).map_err(|error| format!("invalid image public base URL: {error}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("image public base URL must use http or https".into());
    }
    if url.host_str().is_none() || url.query().is_some() || url.fragment().is_some() {
        return Err("image public base URL must have a host and no query or fragment".into());
    }
    Ok(value.trim_end_matches('/').to_string())
}

fn validate_key(key: &str) -> Result<(), String> {
    let Some((digest, extension)) = key.rsplit_once('.') else {
        return Err("image key must include an extension".into());
    };
    if digest.len() != 64
        || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        || !matches!(extension, "png" | "jpg" | "gif" | "webp")
    {
        return Err("invalid content-addressed image key".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn public_url_validation_rejects_non_http_urls() {
        assert!(LocalImageBucket::new("http://127.0.0.1:8080").is_ok());
        assert!(LocalImageBucket::new("data:image/png;base64,abc").is_err());
    }

    #[tokio::test]
    async fn uploaded_images_are_publicly_readable_without_authentication() {
        let bucket = Arc::new(LocalImageBucket::new("http://127.0.0.1:8080/").unwrap());
        let upload = AssetUpload {
            content_type: "image/png".into(),
            bytes: Arc::from(&b"png-bytes"[..]),
        };
        let key = prepare_upload(&upload).unwrap().key;
        let published = bucket.publish(upload).await.unwrap();
        assert_eq!(
            published.url,
            format!("http://127.0.0.1:8080/media/images/{key}")
        );
        assert_eq!(published.size_bytes, 9);

        let response = get_image(State(bucket), Path(key)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "image/png");
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            &b"png-bytes"[..]
        );
    }

    #[test]
    fn gcs_bucket_names_are_validated() {
        assert!(GcsImageBucket::validate_bucket_name("agent-platform-images-123").is_ok());
        assert!(GcsImageBucket::validate_bucket_name("UPPERCASE").is_err());
        assert!(GcsImageBucket::validate_bucket_name("-invalid").is_err());
    }
}
