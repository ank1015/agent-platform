use agent_server::{Command, Config, build_registry, serve};
use agent_store::Store;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
            serve(
                listen,
                store,
                build_registry()?,
                tokens,
                server_settings.expect("serve settings exist for the serve command"),
            )
            .await?
        }
    }
    Ok(())
}
