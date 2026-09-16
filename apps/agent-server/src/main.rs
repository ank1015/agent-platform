use agent_server::{
    Command, Config, GcsImageBucket, ImageBackend, LocalImageBucket, build_registry,
    serve_with_asset_publisher,
};
use agent_store::Store;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| "could not install the process TLS crypto provider")?;
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "agent_server=info,agent_runtime=info,agent_store=warn".into()),
        )
        .init();

    let config = Config::parse();
    config.validate()?;
    let server_settings = config.server_settings();
    let store = Store::connect(&config.database_url, config.pool_config()).await?;
    match config.command {
        Command::Migrate => store.migrate().await?,
        Command::Serve { listen, tokens, .. } => {
            let settings = server_settings.expect("serve settings exist for the serve command");
            let registry = build_registry()?;
            match settings.image_backend {
                ImageBackend::Local => {
                    let bucket = std::sync::Arc::new(LocalImageBucket::new(
                        settings.image_public_base_url.clone(),
                    )?);
                    serve_with_asset_publisher(
                        listen,
                        store,
                        registry,
                        tokens,
                        settings,
                        bucket.clone(),
                        Some(bucket),
                    )
                    .await?
                }
                ImageBackend::Gcs => {
                    let bucket_name = settings
                        .image_bucket
                        .clone()
                        .expect("validated GCS settings include a bucket");
                    let bucket = std::sync::Arc::new(GcsImageBucket::new(bucket_name).await?);
                    serve_with_asset_publisher(
                        listen, store, registry, tokens, settings, bucket, None,
                    )
                    .await?
                }
            }
        }
    }
    Ok(())
}
