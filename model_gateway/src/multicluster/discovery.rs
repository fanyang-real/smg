//! Keeping the gateway's backend set in step with the published routing tables.
//!
//! Structurally this mirrors [`crate::service_discovery`]: a reflector holds a
//! consistent view of the watched objects, every watch event and a periodic
//! tick trigger one level-triggered pass, and the pass submits registry work
//! for the gap. Level-triggered rather than edge-triggered is the important
//! part — a missed event, a failed job, or a relist all self-heal on the next
//! pass instead of leaving the backend set permanently wrong.
//!
//! ## On watching the API server from the data plane
//!
//! Every comparable gateway puts the translation from Kubernetes objects to
//! data-plane config in a control-plane process and ships the result to the
//! proxy, rather than letting each proxy replica watch the API server itself.
//! That is the better production shape: it keeps API credentials out of the
//! serving pods, keeps the API server off the serving path, and lets one
//! component report what it programmed.
//!
//! Watching directly is chosen here deliberately and narrowly, because it needs
//! no new control-plane surface and no new transport, and the scaling cost is
//! bounded by the number of gateway replicas.
//!
//! The choice is also contained. [`reconcile_from`] takes the tables as a plain
//! slice and knows nothing about where they came from, so replacing the watch
//! with a pushed or polled snapshot later changes this file's top half and
//! nothing else — not the projection, not the sync plan, not the routing.

use std::{collections::HashMap, sync::Arc, time::Duration};

use futures::StreamExt;
use kube::{
    api::Api,
    runtime::{
        reflector::{self, Store},
        watcher::{watcher, Config as WatcherConfig},
        WatchStreamExt,
    },
    Client,
};
use rustls::crypto::ring;
use tokio::{sync::Notify, task, time};
use tracing::{error, info, warn};

use super::{apply::apply_desired, projection::project, trafficmap::TrafficMap};
use crate::app_context::AppContext;

/// How the gateway finds and filters the routing tables it should obey.
///
/// Every value is supplied by the operator. There is no default namespace,
/// selector, or interval that would make an unconfigured gateway start routing
/// somewhere unintended.
#[derive(Debug, Clone)]
pub struct MultiClusterConfig {
    /// Whether to consume published routing tables at all.
    pub enabled: bool,
    /// Namespace to watch. `None` watches every namespace, which needs
    /// cluster-wide read on the routing tables.
    pub namespace: Option<String>,
    /// Label selector narrowing which routing tables this gateway obeys. Empty
    /// means all of them in scope.
    pub selector: HashMap<String, String>,
    /// How often to reconcile even without a watch event, so a dropped event or
    /// a failed job cannot leave the backend set wrong indefinitely.
    pub check_interval: Duration,
    /// How long to let a burst of watch events settle before reconciling.
    pub settle_interval: Duration,
}

impl MultiClusterConfig {
    /// The selector in the form the API server expects.
    pub fn list_label_selector(&self) -> String {
        let mut pairs: Vec<String> = self
            .selector
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect();
        // Sorted so the watch URL is stable across restarts and across the
        // arbitrary iteration order of the map.
        pairs.sort();
        pairs.join(",")
    }
}

impl Default for MultiClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            namespace: None,
            selector: HashMap::new(),
            check_interval: Duration::from_secs(30),
            settle_interval: Duration::from_secs(1),
        }
    }
}

/// Start consuming published routing tables using the ambient cluster
/// credentials.
pub async fn start_multicluster_discovery(
    config: MultiClusterConfig,
    app_context: Arc<AppContext>,
) -> Result<task::JoinHandle<()>, kube::Error> {
    if !config.enabled {
        return Err(kube::Error::Api(
            kube::core::Status::failure("Multi-cluster routing is disabled", "ConfigurationError")
                .with_code(400)
                .boxed(),
        ));
    }
    let _ = ring::default_provider().install_default();
    let client = Client::try_default().await?;
    Ok(run(client, config, app_context))
}

/// Run against an injected client, for tests that drive a scripted API server.
#[cfg(feature = "test-util")]
pub fn start_multicluster_discovery_with_client(
    client: Client,
    config: MultiClusterConfig,
    app_context: Arc<AppContext>,
) -> task::JoinHandle<()> {
    let _ = ring::default_provider().install_default();
    run(client, config, app_context)
}

#[expect(
    clippy::disallowed_methods,
    reason = "discovery runs for the lifetime of the server; shutdown is by dropping the handle"
)]
fn run(
    client: Client,
    config: MultiClusterConfig,
    app_context: Arc<AppContext>,
) -> task::JoinHandle<()> {
    task::spawn(async move {
        let scope = config.namespace.as_deref().unwrap_or("(all namespaces)");
        info!(
            "Starting multi-cluster routing | namespace: '{}' | selector: '{}'",
            scope,
            config.list_label_selector()
        );

        let api: Api<TrafficMap> = match &config.namespace {
            Some(namespace) => Api::namespaced(client, namespace),
            None => Api::all(client),
        };
        let config = Arc::new(config);

        let mut watcher_config = WatcherConfig::default();
        let selector = config.list_label_selector();
        if !selector.is_empty() {
            watcher_config = watcher_config.labels(&selector);
        }

        let (store, writer) = reflector::store::<TrafficMap>();
        let notify = Arc::new(Notify::new());

        let mut stream = watcher(api, watcher_config)
            // Nothing here reads managed fields, and they dominate object size
            // in a long-lived store.
            .modify(|tm| tm.metadata.managed_fields = None)
            .default_backoff()
            .reflect(writer)
            .boxed();

        let driver = {
            let notify = Arc::clone(&notify);
            async move {
                while let Some(item) = stream.next().await {
                    match item {
                        Ok(_) => notify.notify_one(),
                        Err(e) => {
                            error!("Routing-table watcher error (auto-retrying with backoff): {e}");
                        }
                    }
                }
                error!("Routing-table watcher stream ended; multi-cluster routing is now frozen");
            }
        };

        let reconciler = async {
            if store.wait_until_ready().await.is_err() {
                error!(
                    "Routing-table watcher dropped before initial sync; reconciliation disabled"
                );
                return;
            }
            info!(
                "Routing tables synced, reconciling on change and every {}s",
                config.check_interval.as_secs()
            );
            let mut interval = time::interval(config.check_interval);
            interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = notify.notified() => {
                        // Coalesce bursts: a placement change rewrites many
                        // tables at once and one pass should absorb all of them.
                        time::sleep(config.settle_interval).await;
                    }
                    _ = interval.tick() => {}
                }
                reconcile(&store, &app_context).await;
            }
        };

        tokio::join!(driver, reconciler);
    })
}

async fn reconcile(store: &Store<TrafficMap>, app_context: &Arc<AppContext>) {
    let snapshot: Vec<TrafficMap> = store.state().iter().map(|tm| (**tm).clone()).collect();
    reconcile_from(&snapshot, app_context).await;
}

/// One reconcile pass over a set of routing tables.
///
/// The seam between "where the tables came from" and "what the gateway does
/// about them": projection turns the observed tables into a desired backend
/// set, and the shared applier writes it. Nothing below this line knows the
/// tables were watched rather than pushed.
pub async fn reconcile_from(maps: &[TrafficMap], app_context: &Arc<AppContext>) {
    let projection = project(maps);
    for warning in &projection.warnings {
        warn!("Routing table: {warning}");
    }
    apply_desired(&projection.homes, "cluster watch", app_context).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(selector: &[(&str, &str)]) -> MultiClusterConfig {
        MultiClusterConfig {
            selector: selector
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn multi_cluster_routing_is_off_until_it_is_configured_on() {
        assert!(!MultiClusterConfig::default().enabled);
    }

    #[test]
    fn an_empty_selector_watches_every_table_in_scope() {
        assert_eq!(config(&[]).list_label_selector(), "");
    }

    #[test]
    fn a_selector_renders_in_the_form_the_api_server_expects() {
        assert_eq!(
            config(&[("tier", "global")]).list_label_selector(),
            "tier=global"
        );
    }

    #[test]
    fn selector_rendering_is_stable_across_map_iteration_order() {
        // An unstable watch URL would relist on every restart for no reason.
        let rendered = config(&[("b", "2"), ("a", "1"), ("c", "3")]).list_label_selector();
        assert_eq!(rendered, "a=1,b=2,c=3");
    }
}
