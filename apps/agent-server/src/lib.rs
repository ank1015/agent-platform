mod config;
mod error;
mod routes;

pub use config::*;
pub use error::*;

use agent_gateways::{
    CallbackVerifierRegistry, GatewayCallbackConfig, GatewayConnectionConfig, GatewayRegistry,
};
use agent_runtime::{
    CompletionRuntime, CompletionSettings, DispatcherSettings, HarnessRegistry,
    OperationDispatcher, RequestCleanupSettings, RequestCleanupWorker, SchedulerSettings,
    SessionScheduler, WaitExpirationSettings, WaitExpirationWorker,
};
use agent_store::Store;
use axum::{Router, extract::DefaultBodyLimit, middleware, routing::get};
use std::{
    future::{Future, IntoFuture},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub const DEFAULT_MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct ServerSettings {
    pub max_body_bytes: usize,
    pub max_concurrent_sse: usize,
    pub shutdown_grace: Duration,
    pub scheduler: SchedulerSettings,
    pub dispatcher: DispatcherSettings,
    pub completion: CompletionSettings,
    pub wait_expiration: WaitExpirationSettings,
    pub request_cleanup: RequestCleanupSettings,
    pub update_retention: Duration,
    pub update_cleanup_poll: Duration,
    pub gateway_connections: Vec<GatewayConnectionConfig>,
    pub callback_connections: Vec<GatewayCallbackConfig>,
    pub callback_tolerance: Duration,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_concurrent_sse: 200,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            scheduler: SchedulerSettings::default(),
            dispatcher: DispatcherSettings::default(),
            completion: CompletionSettings::default(),
            wait_expiration: WaitExpirationSettings::default(),
            request_cleanup: RequestCleanupSettings::default(),
            update_retention: Duration::from_secs(7 * 24 * 60 * 60),
            update_cleanup_poll: Duration::from_secs(60),
            gateway_connections: Vec::new(),
            callback_connections: Vec::new(),
            callback_tolerance: Duration::from_secs(300),
        }
    }
}

impl ServerSettings {
    pub fn validate(self) -> Result<Self, ServerError> {
        if self.max_body_bytes == 0 {
            return Err(ServerError::InvalidConfiguration(
                "maximum request body size must be greater than zero",
            ));
        }
        if self.max_concurrent_sse == 0 || self.max_concurrent_sse > 10_000 {
            return Err(ServerError::InvalidConfiguration(
                "maximum concurrent SSE subscriptions must be 1..10000",
            ));
        }
        if self.shutdown_grace.is_zero() {
            return Err(ServerError::InvalidConfiguration(
                "shutdown grace period must be greater than zero",
            ));
        }
        self.scheduler.validate()?;
        self.dispatcher.validate()?;
        self.completion.validate()?;
        self.wait_expiration.validate()?;
        self.request_cleanup.validate()?;
        if self.update_retention.is_zero()
            || self.update_cleanup_poll.is_zero()
            || chrono::Duration::from_std(self.update_retention).is_err()
        {
            return Err(ServerError::InvalidConfiguration(
                "update retention and cleanup interval must be valid positive durations",
            ));
        }
        if self.callback_tolerance.is_zero() {
            return Err(ServerError::InvalidConfiguration(
                "callback tolerance must be greater than zero",
            ));
        }
        GatewayRegistry::from_configs(self.gateway_connections.clone())?;
        CallbackVerifierRegistry::from_configs(self.callback_connections.clone())?;
        for callback in &self.callback_connections {
            let Some(connection) = self
                .gateway_connections
                .iter()
                .find(|connection| connection.id == callback.id)
            else {
                return Err(ServerError::InvalidConfiguration(
                    "each callback connection must have a matching gateway connection",
                ));
            };
            if connection.kind != callback.kind {
                return Err(ServerError::InvalidConfiguration(
                    "callback and gateway connection kinds must match",
                ));
            }
        }
        Ok(self)
    }
}

#[derive(Clone)]
pub struct Readiness(Arc<ReadinessState>);

struct ReadinessState {
    accepting: AtomicBool,
    draining: CancellationToken,
}

impl Readiness {
    fn accepting() -> Self {
        Self(Arc::new(ReadinessState {
            accepting: AtomicBool::new(true),
            draining: CancellationToken::new(),
        }))
    }

    pub fn begin_draining(&self) {
        self.0.accepting.store(false, Ordering::Release);
        self.0.draining.cancel();
    }

    pub fn is_accepting(&self) -> bool {
        self.0.accepting.load(Ordering::Acquire)
    }

    pub(crate) fn draining(&self) -> CancellationToken {
        self.0.draining.clone()
    }
}

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub registry: Arc<HarnessRegistry>,
    pub(crate) readiness: Readiness,
    pub(crate) callback_verifiers: Arc<CallbackVerifierRegistry>,
    pub(crate) callback_tolerance: Duration,
    pub(crate) metrics: Arc<routes::metrics::Metrics>,
    tokens: Arc<[String]>,
}

pub fn build_registry() -> Result<HarnessRegistry, agent_runtime::RuntimeError> {
    Ok(HarnessRegistry::new())
}

pub fn app(store: Store, registry: HarnessRegistry, tokens: Vec<String>) -> Router {
    app_with_settings(store, registry, tokens, ServerSettings::default()).0
}

pub fn app_with_settings(
    store: Store,
    registry: HarnessRegistry,
    tokens: Vec<String>,
    settings: ServerSettings,
) -> (Router, Readiness) {
    app_with_shared_registry(store, Arc::new(registry), tokens, settings)
}

fn app_with_shared_registry(
    store: Store,
    registry: Arc<HarnessRegistry>,
    tokens: Vec<String>,
    settings: ServerSettings,
) -> (Router, Readiness) {
    let readiness = Readiness::accepting();
    let callback_verifiers = Arc::new(
        CallbackVerifierRegistry::from_configs(settings.callback_connections.clone())
            .expect("server settings were validated"),
    );
    let state = AppState {
        store,
        registry,
        readiness: readiness.clone(),
        callback_verifiers,
        callback_tolerance: settings.callback_tolerance,
        metrics: Arc::new(routes::metrics::Metrics::new(settings.max_concurrent_sse)),
        tokens: tokens.into(),
    };
    let authenticated_api = Router::new()
        .route("/metrics", get(routes::metrics::get))
        .merge(routes::harnesses::router())
        .merge(routes::sessions::router())
        .merge(routes::inputs::router())
        .merge(routes::waits::router())
        .merge(routes::history::router())
        .merge(routes::inspection::router())
        .merge(routes::updates::router())
        .merge(routes::processing::router())
        .fallback(routes::not_found)
        .method_not_allowed_fallback(routes::method_not_allowed)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            routes::request::admit,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            routes::auth::authenticate,
        ));
    let callbacks = routes::callbacks::router().layer(middleware::from_fn_with_state(
        state.clone(),
        routes::request::admit,
    ));
    let api = Router::new().merge(callbacks).merge(authenticated_api);

    let router = Router::new()
        .route("/healthz", get(routes::health::health))
        .route("/readyz", get(routes::health::ready))
        .nest("/v1", api)
        .fallback(routes::not_found)
        .method_not_allowed_fallback(routes::method_not_allowed)
        .layer(DefaultBodyLimit::max(settings.max_body_bytes))
        .layer(middleware::from_fn_with_state(
            state.metrics.clone(),
            routes::metrics::record,
        ))
        .layer(middleware::from_fn(routes::request::context))
        .with_state(state);
    (router, readiness)
}

pub fn validate_service_tokens(tokens: &[String]) -> Result<(), ServerError> {
    if tokens.is_empty()
        || tokens
            .iter()
            .any(|token| token.is_empty() || token.contains(char::is_whitespace))
    {
        return Err(ServerError::InvalidServiceToken);
    }
    Ok(())
}

pub async fn serve(
    listen: SocketAddr,
    store: Store,
    registry: HarnessRegistry,
    tokens: Vec<String>,
    settings: ServerSettings,
) -> Result<(), ServerError> {
    serve_with_shutdown(listen, store, registry, tokens, settings, shutdown_signal()).await
}

pub async fn serve_with_shutdown<F>(
    listen: SocketAddr,
    store: Store,
    registry: HarnessRegistry,
    tokens: Vec<String>,
    settings: ServerSettings,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    validate_service_tokens(&tokens)?;
    let settings = settings.validate()?;
    if !store.schema_ready().await? {
        return Err(ServerError::SchemaNotReady);
    }
    let listener = tokio::net::TcpListener::bind(listen).await?;
    run_listener(listener, store, registry, tokens, settings, shutdown).await
}

pub async fn serve_on_listener_with_shutdown<F>(
    listener: tokio::net::TcpListener,
    store: Store,
    registry: HarnessRegistry,
    tokens: Vec<String>,
    settings: ServerSettings,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    validate_service_tokens(&tokens)?;
    let settings = settings.validate()?;
    if !store.schema_ready().await? {
        return Err(ServerError::SchemaNotReady);
    }
    run_listener(listener, store, registry, tokens, settings, shutdown).await
}

async fn run_listener<F>(
    listener: tokio::net::TcpListener,
    store: Store,
    registry: HarnessRegistry,
    tokens: Vec<String>,
    settings: ServerSettings,
    shutdown: F,
) -> Result<(), ServerError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let address = listener.local_addr()?;
    let registry = Arc::new(registry);
    let scheduler =
        SessionScheduler::new(store.clone(), registry.clone(), settings.scheduler.clone())?;
    let gateways = Arc::new(GatewayRegistry::from_configs(
        settings.gateway_connections.clone(),
    )?);
    let dispatcher =
        OperationDispatcher::new(store.clone(), gateways.clone(), settings.dispatcher.clone())?;
    let completion = CompletionRuntime::new(store.clone(), gateways, settings.completion.clone())?;
    let wait_expiration =
        WaitExpirationWorker::new(store.clone(), settings.wait_expiration.clone())?;
    let request_cleanup =
        RequestCleanupWorker::new(store.clone(), settings.request_cleanup.clone())?;
    let (router, readiness) =
        app_with_shared_registry(store.clone(), registry, tokens, settings.clone());
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    tracing::info!(%address, "agent server listening");

    let server = axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            server_stop.cancelled().await;
        })
        .into_future();
    let runtime_stop = stop.clone();
    let runtime = async move {
        let scheduler_stop = runtime_stop.clone();
        let dispatcher_stop = runtime_stop.clone();
        let wait_stop = runtime_stop.clone();
        let request_cleanup_stop = runtime_stop.clone();
        let updates_stop = runtime_stop.clone();
        let update_store = store.clone();
        let update_retention = settings.update_retention;
        let update_cleanup_poll = settings.update_cleanup_poll;
        tokio::try_join!(
            async {
                scheduler
                    .run(scheduler_stop)
                    .await
                    .map_err(ServerError::Scheduler)
            },
            async {
                dispatcher
                    .run(dispatcher_stop)
                    .await
                    .map_err(ServerError::Dispatcher)
            },
            async {
                completion
                    .run(runtime_stop)
                    .await
                    .map_err(ServerError::Completion)
            },
            async {
                wait_expiration
                    .run(wait_stop)
                    .await
                    .map_err(ServerError::WaitExpiration)
            },
            async {
                request_cleanup
                    .run(request_cleanup_stop)
                    .await
                    .map_err(ServerError::RequestCleanup)
            },
            async {
                run_update_cleanup(
                    update_store,
                    update_retention,
                    update_cleanup_poll,
                    updates_stop,
                )
                .await;
                Ok::<(), ServerError>(())
            },
        )?;
        Ok::<(), ServerError>(())
    };
    tokio::pin!(server);
    tokio::pin!(runtime);
    tokio::pin!(shutdown);
    tokio::select! {
        _ = &mut shutdown => {
            readiness.begin_draining();
            stop.cancel();
            match tokio::time::timeout(settings.shutdown_grace, async {
                let (server, runtime) = tokio::join!(&mut server, &mut runtime);
                server.map_err(ServerError::Io)?;
                runtime
            }).await {
                Ok(result) => result,
                Err(_) => Err(ServerError::ShutdownTimedOut),
            }
        }
        result = &mut runtime => {
            readiness.begin_draining();
            stop.cancel();
            let server_result = tokio::time::timeout(settings.shutdown_grace, &mut server)
                .await
                .map_err(|_| ServerError::ShutdownTimedOut)?
                .map_err(ServerError::Io);
            match result {
                Err(error) => Err(error),
                Ok(()) => server_result.and(Err(ServerError::RuntimeStopped)),
            }
        }
        result = &mut server => {
            readiness.begin_draining();
            stop.cancel();
            tokio::time::timeout(settings.shutdown_grace, &mut runtime)
                .await
                .map_err(|_| ServerError::ShutdownTimedOut)?
                ?;
            result.map_err(ServerError::Io)
        }
    }
}

async fn run_update_cleanup(
    store: Store,
    retention: Duration,
    poll: Duration,
    stop: CancellationToken,
) {
    let age = chrono::Duration::from_std(retention).expect("validated update retention");
    loop {
        let work = store.prune_updates(chrono::Utc::now() - age, 100);
        tokio::select! {
            biased;
            _=stop.cancelled()=>return,
            result=work=>if let Err(error)=result{tracing::warn!(%error,"could not prune update log");},
        }
        tokio::select! {
            _=stop.cancelled()=>return,
            _=tokio::time::sleep(poll)=>{},
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("at least one non-empty service token without whitespace is required")]
    InvalidServiceToken,
    #[error("invalid server configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("database migrations have not been applied exactly")]
    SchemaNotReady,
    #[error("graceful shutdown exceeded its configured deadline")]
    ShutdownTimedOut,
    #[error("a required runtime worker stopped unexpectedly")]
    RuntimeStopped,
    #[error(transparent)]
    Scheduler(#[from] agent_runtime::SchedulerError),
    #[error(transparent)]
    Dispatcher(#[from] agent_runtime::DispatcherError),
    #[error(transparent)]
    Completion(#[from] agent_runtime::CompletionError),
    #[error(transparent)]
    WaitExpiration(#[from] agent_runtime::WaitExpirationError),
    #[error(transparent)]
    RequestCleanup(#[from] agent_runtime::RequestCleanupError),
    #[error(transparent)]
    Gateway(#[from] agent_gateways::GatewayConfigError),
    #[error(transparent)]
    Store(#[from] agent_store::StoreError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler")
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = ctrl_c => {}, _ = terminate => {} }
}
