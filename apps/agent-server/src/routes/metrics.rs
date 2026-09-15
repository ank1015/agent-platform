use crate::{ApiError, AppState};
use axum::{
    extract::{Request, State},
    http::{StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::Semaphore;

const LATENCY_BUCKETS_MS: [u64; 5] = [5, 25, 100, 500, 2_000];

pub struct Metrics {
    requests: [AtomicU64; 5],
    latency_buckets: [AtomicU64; 6],
    latency_sum_micros: AtomicU64,
    pub sse_slots: Arc<Semaphore>,
    pub sse_capacity: usize,
}

impl Metrics {
    pub fn new(max_sse: usize) -> Self {
        Self {
            requests: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum_micros: AtomicU64::new(0),
            sse_slots: Arc::new(Semaphore::new(max_sse)),
            sse_capacity: max_sse,
        }
    }
}

pub async fn record(State(metrics): State<Arc<Metrics>>, request: Request, next: Next) -> Response {
    let start = Instant::now();
    let response = next.run(request).await;
    let class = usize::from(response.status().as_u16() / 100)
        .saturating_sub(1)
        .min(4);
    metrics.requests[class].fetch_add(1, Ordering::Relaxed);
    let micros = start.elapsed().as_micros().min(u64::MAX as u128) as u64;
    metrics
        .latency_sum_micros
        .fetch_add(micros, Ordering::Relaxed);
    let millis = micros / 1_000;
    let bucket = LATENCY_BUCKETS_MS
        .iter()
        .position(|bound| millis <= *bound)
        .unwrap_or(5);
    metrics.latency_buckets[bucket].fetch_add(1, Ordering::Relaxed);
    response
}

pub async fn get(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let snapshot = state.store.operational_metrics().await?;
    let metrics = &state.metrics;
    let mut lines = String::new();
    for (index, class) in ["1xx", "2xx", "3xx", "4xx", "5xx"].iter().enumerate() {
        lines.push_str(&format!(
            "agent_http_requests_total{{status_class=\"{class}\"}} {}\n",
            metrics.requests[index].load(Ordering::Relaxed)
        ));
    }
    for (index, bound) in ["5", "25", "100", "500", "2000", "+Inf"].iter().enumerate() {
        let count: u64 = metrics.latency_buckets[..=index]
            .iter()
            .map(|bucket| bucket.load(Ordering::Relaxed))
            .sum();
        lines.push_str(&format!(
            "agent_http_duration_ms_bucket{{le=\"{bound}\"}} {count}\n"
        ));
    }
    let count: u64 = metrics
        .requests
        .iter()
        .map(|bucket| bucket.load(Ordering::Relaxed))
        .sum();
    lines.push_str(&format!(
        "agent_http_duration_ms_count {count}\nagent_http_duration_ms_sum {}\n",
        metrics.latency_sum_micros.load(Ordering::Relaxed) as f64 / 1_000.0
    ));
    lines.push_str(&format!(
        "agent_sse_subscriptions {}\n",
        metrics.sse_capacity - metrics.sse_slots.available_permits()
    ));
    lines.push_str(&format!("agent_handlers_active {}\nagent_operation_claims_active {}\nagent_session_events_pending {}\nagent_session_events_blocked {}\nagent_oldest_pending_event_age_seconds {}\nagent_operations_pending {}\nagent_operations_submitting {}\nagent_operations_accepted {}\nagent_waits_overdue {}\nagent_operation_requests_cleanup_ready {}\nagent_operation_requests_removed_total {}\n", snapshot.active_handlers, snapshot.active_operation_claims, snapshot.pending_events, snapshot.blocked_events, snapshot.oldest_pending_event_age_seconds.max(0.0), snapshot.pending_operations, snapshot.submitting_operations, snapshot.accepted_operations, snapshot.overdue_waits, snapshot.cleanup_ready_requests, snapshot.removed_requests));
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        lines,
    ))
}
