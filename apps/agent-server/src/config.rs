use crate::{ImageBackend, ServerSettings, validate_service_tokens};
use agent_gateways::{GatewayCallbackConfig, GatewayConnectionConfig};
use agent_runtime::{
    CompletionSettings, DispatcherSettings, RequestCleanupSettings, RetryPolicy, SchedulerSettings,
    WaitExpirationSettings,
};
use agent_store::PoolConfig;
use clap::{Parser, Subcommand};
use std::{net::SocketAddr, time::Duration};

#[derive(Parser)]
#[command(name = "agent-server")]
pub struct Config {
    #[arg(long, env = "AGENT_DATABASE_URL", hide_env_values = true)]
    pub database_url: String,

    #[arg(long, env = "AGENT_DATABASE_MAX_CONNECTIONS", default_value_t = 10)]
    pub database_max_connections: u32,

    #[arg(
        long,
        env = "AGENT_DATABASE_ACQUIRE_TIMEOUT_MS",
        default_value_t = 5_000
    )]
    pub database_acquire_timeout_ms: u64,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    Migrate,
    Serve {
        #[arg(long, env = "AGENT_LISTEN", default_value = "127.0.0.1:8080")]
        listen: SocketAddr,

        #[arg(
            long,
            env = "AGENT_IMAGE_BACKEND",
            value_enum,
            default_value_t = ImageBackend::Local
        )]
        image_backend: ImageBackend,

        #[arg(long, env = "AGENT_IMAGE_BUCKET")]
        image_bucket: Option<String>,

        #[arg(
            long,
            env = "AGENT_IMAGE_PUBLIC_BASE_URL",
            default_value = "http://127.0.0.1:8080"
        )]
        image_public_base_url: String,

        #[arg(
            long,
            env = "AGENT_SERVICE_TOKENS",
            hide_env_values = true,
            value_delimiter = ',',
            required = true
        )]
        tokens: Vec<String>,

        #[arg(
            long,
            env = "AGENT_MAX_BODY_BYTES",
            default_value_t = crate::DEFAULT_MAX_BODY_BYTES
        )]
        max_body_bytes: usize,

        #[arg(long, env = "AGENT_MAX_CONCURRENT_SSE", default_value_t = 200)]
        max_concurrent_sse: usize,

        #[arg(long, env = "AGENT_SHUTDOWN_GRACE_MS", default_value_t = 30_000)]
        shutdown_grace_ms: u64,

        #[arg(
            long,
            env = "AGENT_SCHEDULER_ENABLED",
            default_value_t = true,
            action = clap::ArgAction::Set
        )]
        scheduler_enabled: bool,

        #[arg(long, env = "AGENT_WAIT_EXPIRATION_ENABLED", default_value_t = true, action = clap::ArgAction::Set)]
        wait_expiration_enabled: bool,

        #[arg(long, env = "AGENT_WAIT_EXPIRATION_POLL_MS", default_value_t = 100)]
        wait_expiration_poll_ms: u64,

        #[arg(long, env = "AGENT_WAIT_EXPIRATION_BATCH_SIZE", default_value_t = 100)]
        wait_expiration_batch_size: i64,

        #[arg(long, env = "AGENT_REQUEST_CLEANUP_ENABLED", default_value_t = true, action = clap::ArgAction::Set)]
        request_cleanup_enabled: bool,

        #[arg(long, env = "AGENT_REQUEST_CLEANUP_POLL_MS", default_value_t = 60_000)]
        request_cleanup_poll_ms: u64,

        #[arg(long, env = "AGENT_REQUEST_CLEANUP_BATCH_SIZE", default_value_t = 100)]
        request_cleanup_batch_size: i64,

        #[arg(long, env = "AGENT_UPDATE_RETENTION_MS", default_value_t = 604_800_000)]
        update_retention_ms: u64,

        #[arg(long, env = "AGENT_UPDATE_CLEANUP_POLL_MS", default_value_t = 60_000)]
        update_cleanup_poll_ms: u64,

        #[arg(
            long,
            env = "AGENT_MAX_CONCURRENT_HANDLERS",
            default_value_t = agent_runtime::DEFAULT_MAX_CONCURRENT_HANDLERS
        )]
        max_concurrent_handlers: usize,

        #[arg(long, env = "AGENT_SCHEDULER_POLL_MS", default_value_t = 100)]
        scheduler_poll_ms: u64,

        #[arg(long, env = "AGENT_HANDLER_LEASE_MS", default_value_t = 30_000)]
        handler_lease_ms: u64,

        #[arg(long, env = "AGENT_HANDLER_LEASE_RENEWAL_MS", default_value_t = 10_000)]
        handler_lease_renewal_ms: u64,

        #[arg(
            long,
            env = "AGENT_HANDLER_RETRY_DELAYS_MS",
            value_delimiter = ',',
            default_value = "1000,5000"
        )]
        handler_retry_delays_ms: Vec<u64>,

        #[arg(
            long,
            env = "AGENT_HANDLER_COMMIT_RETRY_DELAYS_MS",
            value_delimiter = ',',
            default_value = "10,50"
        )]
        handler_commit_retry_delays_ms: Vec<u64>,

        #[arg(
            long,
            env = "AGENT_INFRASTRUCTURE_RETRY_DELAY_MS",
            default_value_t = 1_000
        )]
        infrastructure_retry_delay_ms: u64,

        #[arg(long, env = "AGENT_OWNERSHIP_LOSS_GRACE_MS", default_value_t = 100)]
        ownership_loss_grace_ms: u64,

        #[arg(
            long,
            env = "AGENT_GATEWAY_CONNECTIONS",
            hide_env_values = true,
            default_value = "[]"
        )]
        gateway_connections: String,

        #[arg(
            long,
            env = "AGENT_GATEWAY_CALLBACKS",
            hide_env_values = true,
            default_value = "[]"
        )]
        gateway_callbacks: String,

        #[arg(long, env = "AGENT_CALLBACK_TOLERANCE_MS", default_value_t = 300_000)]
        callback_tolerance_ms: u64,

        #[arg(long, env = "AGENT_MAX_CONCURRENT_RESULTS", default_value_t = 32)]
        max_concurrent_results: usize,

        #[arg(long, env = "AGENT_RESULT_FALLBACK_MS", default_value_t = 30_000)]
        result_fallback_ms: u64,

        #[arg(long, env = "AGENT_RESULT_RETRY_MS", default_value_t = 5_000)]
        result_retry_ms: u64,

        #[arg(
            long,
            env = "AGENT_UNMATCHED_CALLBACK_RETENTION_MS",
            default_value_t = 86_400_000
        )]
        unmatched_callback_retention_ms: u64,

        #[arg(long, env = "AGENT_DISPATCHER_ENABLED", default_value_t = true, action = clap::ArgAction::Set)]
        dispatcher_enabled: bool,

        #[arg(long, env = "AGENT_MAX_CONCURRENT_DISPATCHES", default_value_t = agent_runtime::DEFAULT_MAX_CONCURRENT_DISPATCHES)]
        max_concurrent_dispatches: usize,

        #[arg(long, env = "AGENT_DISPATCH_POLL_MS", default_value_t = 100)]
        dispatch_poll_ms: u64,

        #[arg(long, env = "AGENT_OPERATION_LEASE_MS", default_value_t = 30_000)]
        operation_lease_ms: u64,

        #[arg(
            long,
            env = "AGENT_OPERATION_LEASE_RENEWAL_MS",
            default_value_t = 10_000
        )]
        operation_lease_renewal_ms: u64,

        #[arg(
            long,
            env = "AGENT_OPERATION_RETRY_DELAYS_MS",
            value_delimiter = ',',
            default_value = "1000,5000,30000"
        )]
        operation_retry_delays_ms: Vec<u64>,

        #[arg(
            long,
            env = "AGENT_OPERATION_DEPENDENCY_DELAY_MS",
            default_value_t = 250
        )]
        operation_dependency_delay_ms: u64,

        #[arg(
            long,
            env = "AGENT_OPERATION_REQUEST_RETENTION_MS",
            default_value_t = 604_800_000
        )]
        operation_request_retention_ms: u64,

        #[arg(
            long,
            env = "AGENT_OPERATION_RESULT_CHECK_DELAY_MS",
            default_value_t = 1_000
        )]
        operation_result_check_delay_ms: u64,
    },
}

impl Config {
    pub fn validate(&self) -> Result<(), crate::ServerError> {
        if self.database_url.is_empty() {
            return Err(crate::ServerError::InvalidConfiguration(
                "database URL must not be empty",
            ));
        }
        if self.database_max_connections == 0 {
            return Err(crate::ServerError::InvalidConfiguration(
                "database maximum connections must be greater than zero",
            ));
        }
        if self.database_acquire_timeout_ms == 0 {
            return Err(crate::ServerError::InvalidConfiguration(
                "database acquire timeout must be greater than zero",
            ));
        }
        if let Command::Serve { tokens, .. } = &self.command {
            validate_service_tokens(tokens)?;
            self.gateway_connections()?;
            self.gateway_callbacks()?;
            self.server_settings()
                .expect("serve settings exist for the serve command")
                .validate()?;
        }
        Ok(())
    }

    pub fn server_settings(&self) -> Option<ServerSettings> {
        let Command::Serve {
            max_body_bytes,
            max_concurrent_sse,
            image_backend,
            image_bucket,
            image_public_base_url,
            shutdown_grace_ms,
            scheduler_enabled,
            wait_expiration_enabled,
            wait_expiration_poll_ms,
            wait_expiration_batch_size,
            request_cleanup_enabled,
            request_cleanup_poll_ms,
            request_cleanup_batch_size,
            update_retention_ms,
            update_cleanup_poll_ms,
            max_concurrent_handlers,
            scheduler_poll_ms,
            handler_lease_ms,
            handler_lease_renewal_ms,
            handler_retry_delays_ms,
            handler_commit_retry_delays_ms,
            infrastructure_retry_delay_ms,
            ownership_loss_grace_ms,
            gateway_connections,
            gateway_callbacks,
            callback_tolerance_ms,
            max_concurrent_results,
            result_fallback_ms,
            result_retry_ms,
            unmatched_callback_retention_ms,
            dispatcher_enabled,
            max_concurrent_dispatches,
            dispatch_poll_ms,
            operation_lease_ms,
            operation_lease_renewal_ms,
            operation_retry_delays_ms,
            operation_dependency_delay_ms,
            operation_request_retention_ms,
            operation_result_check_delay_ms,
            ..
        } = &self.command
        else {
            return None;
        };
        Some(ServerSettings {
            max_body_bytes: *max_body_bytes,
            max_concurrent_sse: *max_concurrent_sse,
            shutdown_grace: Duration::from_millis(*shutdown_grace_ms),
            scheduler: SchedulerSettings {
                enabled: *scheduler_enabled,
                max_concurrent_handlers: *max_concurrent_handlers,
                poll_interval: Duration::from_millis(*scheduler_poll_ms),
                lease_duration: Duration::from_millis(*handler_lease_ms),
                lease_renewal_interval: Duration::from_millis(*handler_lease_renewal_ms),
                retry_policy: RetryPolicy {
                    retry_delays: handler_retry_delays_ms
                        .iter()
                        .copied()
                        .map(Duration::from_millis)
                        .collect(),
                },
                commit_retry_delays: handler_commit_retry_delays_ms
                    .iter()
                    .copied()
                    .map(Duration::from_millis)
                    .collect(),
                infrastructure_retry_delay: Duration::from_millis(*infrastructure_retry_delay_ms),
                ownership_loss_grace: Duration::from_millis(*ownership_loss_grace_ms),
                shutdown_grace: Duration::from_millis(*shutdown_grace_ms),
            },
            wait_expiration: WaitExpirationSettings {
                enabled: *wait_expiration_enabled,
                poll_interval: Duration::from_millis(*wait_expiration_poll_ms),
                batch_size: *wait_expiration_batch_size,
            },
            request_cleanup: RequestCleanupSettings {
                enabled: *request_cleanup_enabled,
                poll_interval: Duration::from_millis(*request_cleanup_poll_ms),
                batch_size: *request_cleanup_batch_size,
            },
            update_retention: Duration::from_millis(*update_retention_ms),
            update_cleanup_poll: Duration::from_millis(*update_cleanup_poll_ms),
            dispatcher: DispatcherSettings {
                enabled: *dispatcher_enabled,
                max_concurrent_dispatches: *max_concurrent_dispatches,
                poll_interval: Duration::from_millis(*dispatch_poll_ms),
                lease_duration: Duration::from_millis(*operation_lease_ms),
                lease_renewal_interval: Duration::from_millis(*operation_lease_renewal_ms),
                retry_delays: operation_retry_delays_ms
                    .iter()
                    .copied()
                    .map(Duration::from_millis)
                    .collect(),
                dependency_delay: Duration::from_millis(*operation_dependency_delay_ms),
                request_retention: Duration::from_millis(*operation_request_retention_ms),
                result_check_delay: Duration::from_millis(*operation_result_check_delay_ms),
                shutdown_grace: Duration::from_millis(*shutdown_grace_ms),
            },
            gateway_connections: serde_json::from_str(gateway_connections)
                .expect("gateway connections were validated"),
            callback_connections: serde_json::from_str(gateway_callbacks)
                .expect("gateway callbacks were validated"),
            callback_tolerance: Duration::from_millis(*callback_tolerance_ms),
            image_backend: *image_backend,
            image_bucket: image_bucket.clone(),
            image_public_base_url: image_public_base_url.clone(),
            completion: CompletionSettings {
                max_concurrent_results: *max_concurrent_results,
                poll_interval: Duration::from_millis(*dispatch_poll_ms),
                lease_duration: Duration::from_millis(*operation_lease_ms),
                lease_renewal_interval: Duration::from_millis(*operation_lease_renewal_ms),
                fallback_interval: Duration::from_millis(*result_fallback_ms),
                retry_delay: Duration::from_millis(*result_retry_ms),
                unmatched_retention: Duration::from_millis(*unmatched_callback_retention_ms),
                request_retention: Duration::from_millis(*operation_request_retention_ms),
                infrastructure_retry_delay: Duration::from_millis(*infrastructure_retry_delay_ms),
                shutdown_grace: Duration::from_millis(*shutdown_grace_ms),
                ..CompletionSettings::default()
            },
        })
    }

    fn gateway_connections(&self) -> Result<Vec<GatewayConnectionConfig>, crate::ServerError> {
        let Command::Serve {
            gateway_connections,
            ..
        } = &self.command
        else {
            return Ok(Vec::new());
        };
        serde_json::from_str(gateway_connections).map_err(|_| {
            crate::ServerError::InvalidConfiguration(
                "gateway connections must be a valid JSON array",
            )
        })
    }

    fn gateway_callbacks(&self) -> Result<Vec<GatewayCallbackConfig>, crate::ServerError> {
        let Command::Serve {
            gateway_callbacks, ..
        } = &self.command
        else {
            return Ok(Vec::new());
        };
        serde_json::from_str(gateway_callbacks).map_err(|_| {
            crate::ServerError::InvalidConfiguration("gateway callbacks must be a valid JSON array")
        })
    }

    pub fn pool_config(&self) -> PoolConfig {
        PoolConfig {
            max_connections: self.database_max_connections,
            acquire_timeout: Duration::from_millis(self.database_acquire_timeout_ms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn migration_does_not_require_service_tokens() {
        let config = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "migrate",
        ])
        .unwrap();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn serve_rejects_invalid_limits_and_tokens() {
        let missing_token = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "serve",
        ]);
        assert!(missing_token.is_err());

        let zero_limit = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "serve",
            "--tokens",
            "token",
            "--max-body-bytes",
            "0",
        ])
        .unwrap();
        assert!(zero_limit.validate().is_err());

        let invalid_lease = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "serve",
            "--tokens",
            "token",
            "--handler-lease-ms",
            "100",
            "--handler-lease-renewal-ms",
            "100",
        ])
        .unwrap();
        assert!(invalid_lease.validate().is_err());

        let valid_gateway = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "serve",
            "--tokens",
            "token",
            "--gateway-connections",
            r#"[{"id":"llm","kind":"llm","base_url":"https://gateway.example/","bearer_token":"gateway-token"}]"#,
        ])
        .unwrap();
        assert!(valid_gateway.validate().is_ok());

        let valid_callback = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "serve",
            "--tokens",
            "token",
            "--gateway-connections",
            r#"[{"id":"llm","kind":"llm","base_url":"https://gateway.example/","bearer_token":"gateway-token"}]"#,
            "--gateway-callbacks",
            r#"[{"id":"llm","kind":"llm","secrets":["current","previous"]}]"#,
        ])
        .unwrap();
        assert!(valid_callback.validate().is_ok());

        let unmatched_callback = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "serve",
            "--tokens",
            "token",
            "--gateway-callbacks",
            r#"[{"id":"llm","kind":"llm","secrets":["secret"]}]"#,
        ])
        .unwrap();
        assert!(unmatched_callback.validate().is_err());

        let invalid_gateway = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "serve",
            "--tokens",
            "token",
            "--gateway-connections",
            "not-json",
        ])
        .unwrap();
        assert!(invalid_gateway.validate().is_err());

        let missing_gcs_bucket = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "serve",
            "--tokens",
            "token",
            "--image-backend",
            "gcs",
        ])
        .unwrap();
        assert!(missing_gcs_bucket.validate().is_err());

        let valid_gcs_bucket = Config::try_parse_from([
            "agent-server",
            "--database-url",
            "postgresql:///agent_platform",
            "serve",
            "--tokens",
            "token",
            "--image-backend",
            "gcs",
            "--image-bucket",
            "agent-platform-images-123",
        ])
        .unwrap();
        assert!(valid_gcs_bucket.validate().is_ok());
    }
}
