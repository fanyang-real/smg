//! Writing a desired set of remote homes into the worker registry.
//!
//! This is where multi-cluster routing stops being a description and starts
//! being backends, and it is deliberately the only place that does. Both ways a
//! routing table can arrive — pushed to [`super::api`] by a control plane, or
//! read from the cluster by [`super::discovery`] — reduce to the same
//! [`DesiredHome`] set and hand it here. Keeping one writer means the two
//! sources cannot drift in how a weight change, a departure, or a re-keyed
//! model is applied.
//!
//! Applying is level-triggered: every pass states the whole desired set and
//! the plan is the difference from what is registered. A missed push, a failed
//! job, or a dropped watch event therefore self-heals on the next pass rather
//! than leaving the backend set permanently wrong.

use std::sync::Arc;

use openai_protocol::worker::{ConnectionMode, WorkerSpec, WorkerType};
use tracing::{debug, error, info, warn};

use super::{
    projection::DesiredHome,
    sync::{plan_sync, HomeSync},
    HOME_CLUSTER_LABEL, HOME_SOURCE_LABEL,
};
use crate::{
    app_context::AppContext,
    policies::ROUTING_WEIGHT_LABEL,
    worker::WorkerOrigin,
    workflow::{Job, JobQueue, WorkerRegistrationMode},
};

/// Reconcile the registry so exactly `desired` is held as remote homes.
///
/// `origin` names where the table came from, for logs only.
///
/// Returns how many changes were deferred because that backend already had a
/// registry job in flight. A caller that has no periodic pass of its own must
/// come back when this is non-zero, or the deferred change is lost until
/// something else moves.
pub async fn apply_desired(
    desired: &[DesiredHome],
    origin: &str,
    app_context: &Arc<AppContext>,
) -> usize {
    let registered = registered_homes(app_context);
    let plan = plan_sync(desired, &registered);
    if plan.is_empty() {
        debug!(
            "Multi-cluster routing steady from {origin}: {} home(s)",
            desired.len()
        );
        return 0;
    }

    let Some(job_queue) = app_context.worker_job_queue.get() else {
        warn!(
            "JobQueue not initialized; deferring {} addition(s), {} reweight(s), {} removal(s)",
            plan.add.len(),
            plan.reweight.len(),
            plan.remove.len()
        );
        return plan.add.len() + plan.reweight.len() + plan.remove.len();
    };

    // One in-flight job per backend: a pending or processing job already owns
    // that backend's transition, and a second would race it.
    let in_flight = |url: &str| {
        job_queue
            .get_status(url)
            .is_some_and(|status| status.status == "pending" || status.status == "processing")
    };

    info!(
        "Reconciling remote homes from {origin}: {} to add, {} to reweight, {} to remove",
        plan.add.len(),
        plan.reweight.len(),
        plan.remove.len()
    );

    let mut deferred = 0usize;

    for url in &plan.remove {
        if in_flight(url) {
            deferred += 1;
            continue;
        }
        info!("Removing remote home {url}: no longer in the routing table");
        let job = Job::RemoveWorker {
            url: url.clone(),
            expected_revision: None,
        };
        if let Err(e) = job_queue.submit(job).await {
            error!("Failed to submit removal for remote home {url}: {e}");
        }
    }

    // Adds and reweights are both upserts on the same URL, which the registry
    // applies in place; they are logged apart because one is a membership
    // change and the other is a share movement, and reading them as the same
    // event would hide normal weight drift inside registration noise.
    for home in &plan.add {
        if in_flight(&home.url) {
            deferred += 1;
            continue;
        }
        info!(
            "Registering remote home {} (cluster {}, model {}, weight {})",
            home.url, home.cluster, home.model_id, home.weight
        );
        submit_upsert(job_queue, home, app_context).await;
    }
    for home in &plan.reweight {
        if in_flight(&home.url) {
            deferred += 1;
            continue;
        }
        info!(
            "Reweighting remote home {} (cluster {}) to {}",
            home.url, home.cluster, home.weight
        );
        submit_upsert(job_queue, home, app_context).await;
    }

    if deferred > 0 {
        debug!("{deferred} change(s) deferred behind in-flight registry work from {origin}");
    }
    deferred
}

async fn submit_upsert(
    job_queue: &Arc<JobQueue>,
    home: &DesiredHome,
    app_context: &Arc<AppContext>,
) {
    let job = Job::AddWorker {
        config: Box::new(build_worker_spec(home, app_context)),
        registration_mode: WorkerRegistrationMode::Upsert,
    };
    if let Err(e) = job_queue.submit(job).await {
        error!("Failed to submit remote home {}: {e}", home.url);
    }
}

/// The remote homes this gateway currently holds.
///
/// Identified by the source label rather than by URL shape: a gateway may hold
/// locally discovered workers and remote homes at once, and reconciling one set
/// must never remove a backend belonging to the other. Mesh-replicated entries
/// are excluded because they belong to a peer's registry, not this one.
pub fn registered_homes(app_context: &AppContext) -> Vec<HomeSync> {
    app_context
        .worker_registry
        .get_all_with_ids()
        .into_iter()
        .filter_map(|(id, worker)| {
            if app_context.worker_registry.origin_of(&id) != Some(WorkerOrigin::Local) {
                return None;
            }
            let labels = &worker.metadata().spec.labels;
            labels.get(HOME_SOURCE_LABEL)?;
            Some(HomeSync {
                url: worker.url().to_string(),
                weight: crate::policies::worker_weight(worker.as_ref()),
                model_id: worker.model_id().to_string(),
            })
        })
        .collect()
}

fn build_worker_spec(home: &DesiredHome, app_context: &AppContext) -> WorkerSpec {
    let mut spec = WorkerSpec::new(home.url.clone());
    spec.worker_type = WorkerType::Regular;
    // A home is reached through its cluster's ingress over HTTP, so there is
    // nothing to probe for: declaring the mode skips connection-mode detection
    // and its gRPC probe against a public endpoint.
    spec.connection_mode = ConnectionMode::Http;
    spec.labels
        .insert(HOME_CLUSTER_LABEL.to_string(), home.cluster.clone());
    spec.labels
        .insert(HOME_SOURCE_LABEL.to_string(), home.source.clone());
    spec.labels
        .insert(ROUTING_WEIGHT_LABEL.to_string(), home.weight.to_string());
    spec.labels
        .insert("served_model_name".to_string(), home.model_id.clone());
    spec.api_key.clone_from(&app_context.router_config.api_key);
    spec
}
