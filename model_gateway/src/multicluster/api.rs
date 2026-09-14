//! The routing table a control plane pushes, and the HTTP surface it pushes to.
//!
//! The gateway does not go looking for its cross-cluster routing table. A
//! control plane that already watches the cluster, resolves placement against
//! quota, and computes the weights programs this gateway directly, the same way
//! every comparable gateway is programmed: the control plane owns the
//! Kubernetes types and the data plane owns a neutral resource model.
//!
//! What that buys, concretely: no API-server credentials in a serving pod, no
//! API server on the serving path, one component that knows what it programmed
//! and can report it, and a gateway that can be programmed by something other
//! than Kubernetes.
//!
//! What it costs is fan-out. The control plane must reach every replica, and a
//! replica that was down during a push, or started after it, has an empty
//! table until the next one. The answer is not cleverness here, it is a
//! publisher that re-states the whole table on a timer: every push is a full
//! statement of intent, applying one is idempotent, and
//! [`RoutingStatus::age_seconds`] makes a publisher that stopped pushing
//! visible rather than silent.
//!
//! Readiness is deliberately not gated on having a table. A gateway removed
//! from its Service endpoints for being unready is a gateway the publisher can
//! no longer reach, which would turn one missed push into a permanent one.

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, Notify};
use tracing::info;

use super::{
    apply::apply_desired,
    table::{desired_homes, index, validate, RoutingError, RoutingService, RoutingTableSpec},
};
use crate::{app_context::AppContext, observability::metrics::Metrics};

/// How soon to come back after a push that could not be fully applied.
///
/// Short, because the gap it covers is a live one: the backend is holding a
/// share the control plane has already stopped asking for.
const RETRY_DELAY: Duration = Duration::from_millis(250);

/// What the gateway currently holds, and how long it has held it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingStatus {
    /// Bumped every time the table changes. A publisher that sees this
    /// unchanged after a push knows the push was a no-op, and one that sees it
    /// go backwards knows the gateway restarted.
    pub generation: u64,
    /// Services currently routed.
    pub services: usize,
    /// Backends across all services.
    pub backends: usize,
    /// When the table last changed, RFC 3339.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_applied: Option<String>,
    /// Seconds since the table last changed. Absent before the first push.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_seconds: Option<u64>,
}

/// The current table, with the status that describes it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingTableView {
    #[serde(flatten)]
    pub status: RoutingStatus,
    pub services: Vec<RoutingService>,
}

/// The table as held, plus when it got that way.
#[derive(Debug, Default)]
struct RoutingSnapshot {
    generation: u64,
    applied_at: Option<DateTime<Utc>>,
    services: BTreeMap<String, RoutingService>,
}

impl RoutingSnapshot {
    fn status(&self) -> RoutingStatus {
        let backends = self.services.values().map(|s| s.backends.len()).sum();
        RoutingStatus {
            generation: self.generation,
            services: self.services.len(),
            backends,
            last_applied: self.applied_at.map(|t| t.to_rfc3339()),
            age_seconds: self
                .applied_at
                .map(|t| u64::try_from((Utc::now() - t).num_seconds().max(0)).unwrap_or(0)),
        }
    }
}

/// The pushed routing table and everything that acts on it.
pub struct RoutingState {
    app_context: Arc<AppContext>,
    /// Read lock-free on every status read; replaced wholesale on every accepted
    /// push so a reader never observes a half-applied table.
    snapshot: ArcSwap<RoutingSnapshot>,
    /// Serializes writers. Upserting one service is a read-modify-write over
    /// the whole table, and two concurrent ones would otherwise interleave into
    /// a table neither publisher stated.
    writer: Mutex<()>,
    /// Pushes accepted since start, whether or not they changed anything.
    accepted: AtomicU64,
    /// Raised when a push could not be fully applied, so the reconciler comes
    /// back promptly instead of waiting out its interval.
    retry: Notify,
    /// How often to re-apply the current table even with nothing new pushed.
    reconcile_interval: Duration,
}

impl std::fmt::Debug for RoutingState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoutingState")
            .field("status", &self.snapshot.load().status())
            .finish_non_exhaustive()
    }
}

impl RoutingState {
    pub fn new(app_context: Arc<AppContext>, reconcile_interval: Duration) -> Self {
        Self {
            app_context,
            snapshot: ArcSwap::from_pointee(RoutingSnapshot::default()),
            writer: Mutex::new(()),
            accepted: AtomicU64::new(0),
            retry: Notify::new(),
            reconcile_interval,
        }
    }

    /// What the gateway holds right now.
    pub fn status(&self) -> RoutingStatus {
        self.snapshot.load().status()
    }

    /// The whole table, for a publisher reconciling against it.
    pub fn view(&self) -> RoutingTableView {
        let snapshot = self.snapshot.load();
        RoutingTableView {
            status: snapshot.status(),
            services: snapshot.services.values().cloned().collect(),
        }
    }

    /// Replace the entire table.
    ///
    /// Unconditional by design. This is the path a publisher uses to resync
    /// after a restart or a missed push, so refusing it on a version comparison
    /// would let one bad comparison wedge the gateway permanently out of date.
    pub async fn replace(&self, spec: RoutingTableSpec) -> Result<RoutingStatus, RoutingError> {
        let next = index(spec)?;
        validate(&next)?;
        let _guard = self.writer.lock().await;
        Ok(self.commit(next, "full replace").await)
    }

    /// Add or update one service, leaving the rest of the table alone.
    pub async fn upsert(
        &self,
        name: &str,
        service: RoutingService,
    ) -> Result<RoutingStatus, RoutingError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(RoutingError::UnnamedService);
        }
        let mut service = service;
        service.service = if service.service.trim().is_empty() {
            name.to_string()
        } else {
            service.service.trim().to_string()
        };
        if service.service != name {
            return Err(RoutingError::NameMismatch {
                path: name.to_string(),
                body: service.service,
            });
        }

        let _guard = self.writer.lock().await;
        let current = self.snapshot.load();

        // Refuse a push that is strictly behind what is already applied. Equal
        // is accepted: a publisher retrying after a timeout on a write that
        // actually landed must succeed, not be told it lost a race with itself.
        if let (Some(offered), Some(applied)) = (
            service.generation,
            current.services.get(name).and_then(|s| s.generation),
        ) {
            if offered < applied {
                return Err(RoutingError::StaleGeneration {
                    service: name.to_string(),
                    offered,
                    applied,
                });
            }
        }

        let mut next = current.services.clone();
        next.insert(name.to_string(), service);
        validate(&next)?;
        Ok(self.commit(next, "service upsert").await)
    }

    /// Remove one service and everything it routed to.
    ///
    /// Removing a service that is not there succeeds. A publisher tearing down
    /// a service it already tore down, or one this replica never heard of
    /// because it missed the push that created it, has arrived at the state it
    /// asked for, and reporting that as an error only invites a retry loop.
    pub async fn remove(&self, name: &str) -> RoutingStatus {
        let name = name.trim();
        let _guard = self.writer.lock().await;
        let current = self.snapshot.load();
        if !current.services.contains_key(name) {
            return current.status();
        }
        let mut next = current.services.clone();
        next.remove(name);
        // Removing can only shrink a table that already validated, so there is
        // nothing left to check.
        self.commit(next, "service delete").await
    }

    /// Swap in a validated table and reconcile the backends to match.
    ///
    /// The caller holds the writer lock and has already validated `next`.
    async fn commit(&self, next: BTreeMap<String, RoutingService>, reason: &str) -> RoutingStatus {
        let current = self.snapshot.load();
        if current.services == next {
            // Nothing changed. Do not bump the generation or the applied time:
            // a publisher re-stating the same table on a timer would otherwise
            // look like continuous churn and hide real changes.
            self.accepted.fetch_add(1, Ordering::Relaxed);
            return current.status();
        }

        let snapshot = RoutingSnapshot {
            generation: current.generation + 1,
            applied_at: Some(Utc::now()),
            services: next,
        };
        let homes = desired_homes(&snapshot.services);
        let status = snapshot.status();
        // Swap before reconciling so a concurrent read never sees the old table
        // alongside newly registered backends.
        self.snapshot.store(Arc::new(snapshot));
        self.accepted.fetch_add(1, Ordering::Relaxed);

        info!(
            "Routing table generation {} applied by {reason}: {} service(s), {} backend(s)",
            status.generation, status.services, status.backends
        );
        Metrics::set_routing_table_generation(status.generation);
        Metrics::set_routing_table_services(status.services);

        if apply_desired(&homes, "control plane push", &self.app_context).await > 0 {
            // Some backend was mid-registration and could not be changed. The
            // push itself succeeded, so the publisher has no reason to retry;
            // coming back ourselves is the only thing that closes the gap.
            self.retry.notify_one();
        }
        status
    }

    /// Re-apply the current table without changing it.
    ///
    /// The push path is otherwise edge-triggered: a change that arrives while
    /// that backend is mid-registration is skipped, and with no second pass it
    /// would stay skipped until something else happened to move. This is that
    /// second pass, and it also repairs a failed registry job or a backend that
    /// was changed out from under the table.
    pub async fn resync(&self) -> usize {
        let _guard = self.writer.lock().await;
        let homes = desired_homes(&self.snapshot.load().services);
        apply_desired(&homes, "routing table resync", &self.app_context).await
    }

    /// Re-apply the table on a timer, and report how stale it is.
    ///
    /// Two jobs, one loop, because both are about the same thing: a pushed
    /// table is only correct for as long as someone keeps checking. The timer
    /// makes the push path level-triggered, and the gauge makes a control plane
    /// that has stopped pushing visible rather than silent. Age only grows with
    /// the clock, so a gauge written solely when a push lands would read zero
    /// forever and hide exactly the failure it exists to show.
    ///
    /// Runs for the life of the process; dropping the handle stops it.
    #[expect(
        clippy::disallowed_methods,
        reason = "the routing table must keep reconciling for the lifetime of the server"
    )]
    pub fn spawn_reconciler(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let state = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(state.reconcile_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = state.retry.notified() => {
                        tokio::time::sleep(RETRY_DELAY).await;
                    }
                    _ = interval.tick() => {}
                }
                if state.resync().await > 0 {
                    state.retry.notify_one();
                }
                if let Some(age) = state.status().age_seconds {
                    Metrics::set_routing_table_age_seconds(age);
                }
            }
        })
    }
}

// ── HTTP surface ─────────────────────────────────────────────────────────

/// A refused push, rendered for the publisher that sent it.
struct RoutingErrorResponse(RoutingError);

impl IntoResponse for RoutingErrorResponse {
    fn into_response(self) -> Response {
        let status = match self.0 {
            RoutingError::EndpointConflict { .. } | RoutingError::StaleGeneration { .. } => {
                StatusCode::CONFLICT
            }
            _ => StatusCode::BAD_REQUEST,
        };
        let body = json!({
            "error": self.0.to_string(),
            "code": self.0.code(),
            "retryable": self.0.is_retryable(),
        });
        (status, Json(body)).into_response()
    }
}

/// The routing API, to be nested under a versioned prefix and wrapped in
/// control-plane auth by the caller.
pub fn router<S: Clone + Send + Sync + 'static>(state: Arc<RoutingState>) -> Router<S> {
    Router::new()
        .route("/status", get(get_status))
        .route("/services", get(get_services).put(put_services))
        .route(
            "/services/{service}",
            get(get_service).put(put_service).delete(delete_service),
        )
        .with_state(state)
}

async fn get_status(State(state): State<Arc<RoutingState>>) -> Response {
    Json(state.status()).into_response()
}

async fn get_services(State(state): State<Arc<RoutingState>>) -> Response {
    Json(state.view()).into_response()
}

async fn get_service(
    State(state): State<Arc<RoutingState>>,
    Path(service): Path<String>,
) -> Response {
    match state
        .view()
        .services
        .into_iter()
        .find(|s| s.service == service.trim())
    {
        Some(found) => Json(found).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": format!("service '{service}' is not in the routing table"),
                "code": "service_not_found",
                "retryable": false,
            })),
        )
            .into_response(),
    }
}

async fn put_services(
    State(state): State<Arc<RoutingState>>,
    Json(spec): Json<RoutingTableSpec>,
) -> Response {
    match state.replace(spec).await {
        Ok(status) => Json(status).into_response(),
        Err(err) => RoutingErrorResponse(err).into_response(),
    }
}

async fn put_service(
    State(state): State<Arc<RoutingState>>,
    Path(service): Path<String>,
    Json(body): Json<RoutingService>,
) -> Response {
    match state.upsert(&service, body).await {
        Ok(status) => Json(status).into_response(),
        Err(err) => RoutingErrorResponse(err).into_response(),
    }
}

async fn delete_service(
    State(state): State<Arc<RoutingState>>,
    Path(service): Path<String>,
) -> Response {
    Json(state.remove(&service).await).into_response()
}
