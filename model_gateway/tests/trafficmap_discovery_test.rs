//! Multi-cluster routing integration tests against a scripted fake API server
//! serving TrafficMap routing tables.
//!
//! The real watcher, reflector, projection, sync planner, job queue, and worker
//! registry run end to end; only the Kubernetes API and the remote cluster
//! ingresses are test doubles. Each remote "home" is a mock engine standing in
//! for another cluster's ingress.
#![allow(clippy::unwrap_used, clippy::allow_attributes)]

mod common;

use std::{
    collections::BTreeMap,
    convert::Infallible,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    body::{Body, Bytes},
    extract::{RawQuery, State},
    http::header,
    response::Response,
    routing::any,
    Router,
};
use common::mock_worker::{HealthStatus, MockWorker, MockWorkerConfig, WorkerType};
use rustls::crypto::ring;
use smg::{
    app_context::AppContext,
    config::RouterConfig,
    multicluster::{
        start_multicluster_discovery_with_client, MultiClusterConfig, HOME_CLUSTER_LABEL,
        HOME_SOURCE_LABEL,
    },
    policies::ROUTING_WEIGHT_LABEL,
};
use tokio::sync::broadcast;

const NAMESPACE: &str = "team-a";

// ========== Fake Kubernetes API server (trafficmaps only) ==========

#[derive(Clone)]
struct FakeState {
    maps: Arc<Mutex<BTreeMap<String, serde_json::Value>>>,
    resource_version: Arc<AtomicU64>,
    watch_tx: Arc<Mutex<broadcast::Sender<String>>>,
}

struct FakeK8s {
    state: FakeState,
    base_url: String,
}

impl FakeK8s {
    #[expect(clippy::disallowed_methods, reason = "test infrastructure")]
    async fn start() -> Self {
        let state = FakeState {
            maps: Arc::new(Mutex::new(BTreeMap::new())),
            resource_version: Arc::new(AtomicU64::new(1)),
            watch_tx: Arc::new(Mutex::new(broadcast::channel(64).0)),
        };
        let app = Router::new()
            .route(
                "/apis/ome.io/v1beta1/namespaces/{namespace}/trafficmaps",
                any(trafficmaps_handler),
            )
            .route("/apis/ome.io/v1beta1/trafficmaps", any(trafficmaps_handler))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            state,
            base_url: format!("http://{addr}"),
        }
    }

    fn client(&self) -> kube::Client {
        let _ = ring::default_provider().install_default();
        let config = kube::Config::new(self.base_url.parse().unwrap());
        kube::Client::try_from(config).unwrap()
    }

    fn next_rv(&self) -> String {
        (self.state.resource_version.fetch_add(1, Ordering::SeqCst) + 1).to_string()
    }

    /// Publish (or republish) a routing table with the given homes.
    fn publish(&self, name: &str, service: &str, homes: &[(&str, u16, i32)]) {
        let entries: Vec<serde_json::Value> = homes
            .iter()
            .map(|(cluster, port, weight)| {
                serde_json::json!({
                    "cluster": cluster,
                    "endpoint": format!("http://127.0.0.1:{port}"),
                    "weight": weight,
                    "healthy": *weight > 0,
                    "capacity": {"allocated": weight, "ready": weight},
                })
            })
            .collect();
        let map = serde_json::json!({
            "apiVersion": "ome.io/v1beta1",
            "kind": "TrafficMap",
            "metadata": {
                "name": name,
                "namespace": NAMESPACE,
                "resourceVersion": self.next_rv(),
            },
            "spec": {
                "service": service,
                "mode": "Split",
                "entries": entries,
            },
        });
        let replaced = self
            .state
            .maps
            .lock()
            .unwrap()
            .insert(name.to_string(), map.clone())
            .is_some();
        self.send_event(if replaced { "MODIFIED" } else { "ADDED" }, &map);
    }

    fn delete(&self, name: &str) {
        let removed = self.state.maps.lock().unwrap().remove(name);
        if let Some(map) = removed {
            self.send_event("DELETED", &map);
        }
    }

    fn send_event(&self, event_type: &str, map: &serde_json::Value) {
        let line = format!(
            "{}\n",
            serde_json::json!({ "type": event_type, "object": map })
        );
        let _ = self.state.watch_tx.lock().unwrap().send(line);
    }
}

async fn trafficmaps_handler(
    State(state): State<FakeState>,
    RawQuery(query): RawQuery,
) -> Response {
    let query = query.unwrap_or_default();
    if query.contains("watch=true") {
        let rx = state.watch_tx.lock().unwrap().subscribe();
        let stream = futures::stream::unfold(rx, |mut rx| async move {
            loop {
                match rx.recv().await {
                    Ok(line) => return Some((Ok::<Bytes, Infallible>(Bytes::from(line)), rx)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        });
        Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from_stream(stream))
            .unwrap()
    } else {
        let items: Vec<serde_json::Value> = state.maps.lock().unwrap().values().cloned().collect();
        let list = serde_json::json!({
            "kind": "TrafficMapList",
            "apiVersion": "ome.io/v1beta1",
            "metadata": {
                "resourceVersion": state.resource_version.load(Ordering::SeqCst).to_string()
            },
            "items": items,
        });
        Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(list.to_string()))
            .unwrap()
    }
}

// ========== Harness ==========

fn multicluster_config() -> MultiClusterConfig {
    MultiClusterConfig {
        enabled: true,
        namespace: Some(NAMESPACE.to_string()),
        check_interval: Duration::from_millis(300),
        settle_interval: Duration::from_millis(50),
        ..Default::default()
    }
}

async fn test_context() -> Arc<AppContext> {
    let config = RouterConfig::builder()
        .worker_startup_timeout_secs(2)
        .build_unchecked();
    common::create_test_context(config).await
}

/// A mock engine standing in for one remote cluster's ingress.
async fn start_home() -> (MockWorker, u16) {
    let mut worker = MockWorker::new(MockWorkerConfig {
        port: 0,
        worker_type: WorkerType::Regular,
        health_status: HealthStatus::Healthy,
        response_delay_ms: 0,
        fail_rate: 0.0,
    });
    let url = worker.start().await.unwrap();
    let port = url.rsplit(':').next().unwrap().parse().unwrap();
    (worker, port)
}

async fn wait_for(
    app_context: &AppContext,
    what: &str,
    timeout: Duration,
    predicate: impl Fn(&[String]) -> bool,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let urls = app_context.worker_registry.get_all_urls();
        if predicate(&urls) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}; registry: {urls:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll until the home registered for `port` carries `weight`.
async fn wait_for_weight(app_context: &AppContext, port: u16, weight: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let found = app_context
            .worker_registry
            .get_all()
            .into_iter()
            .find(|w| w.url().ends_with(&format!("127.0.0.1:{port}")));
        if let Some(worker) = &found {
            if worker.metadata().spec.labels.get(ROUTING_WEIGHT_LABEL) == Some(&weight.to_string())
            {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for home :{port} to reach weight {weight}; \
             found {:?}",
            found.map(|w| w.metadata().spec.labels.clone())
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn has_url(urls: &[String], port: u16) -> bool {
    urls.iter()
        .any(|url| url.ends_with(&format!("127.0.0.1:{port}")))
}

// ========== Scenarios ==========

#[tokio::test]
async fn published_homes_register_as_weighted_backends() {
    let fake = FakeK8s::start().await;
    let app_context = test_context().await;
    let (_east, east_port) = start_home().await;
    let (_west, west_port) = start_home().await;

    fake.publish(
        "llama",
        "llama",
        &[
            ("cluster-east", east_port, 70),
            ("cluster-west", west_port, 30),
        ],
    );

    let handle = start_multicluster_discovery_with_client(
        fake.client(),
        multicluster_config(),
        Arc::clone(&app_context),
    );

    wait_for(
        &app_context,
        "both remote homes registered",
        Duration::from_secs(20),
        |urls| has_url(urls, east_port) && has_url(urls, west_port),
    )
    .await;

    // The published weight and the home's provenance ride on the backend, so
    // routing and observability both have what they need.
    for worker in app_context.worker_registry.get_all() {
        let labels = &worker.metadata().spec.labels;
        assert_eq!(
            labels.get(HOME_SOURCE_LABEL),
            Some(&format!("{NAMESPACE}/llama")),
            "home {} missing its source table",
            worker.url()
        );
        let cluster = labels.get(HOME_CLUSTER_LABEL).expect("cluster label");
        let expected_weight = if cluster == "cluster-east" {
            "70"
        } else {
            "30"
        };
        assert_eq!(
            labels.get(ROUTING_WEIGHT_LABEL),
            Some(&expected_weight.to_string()),
            "home {} in {cluster} carries the wrong weight",
            worker.url()
        );
    }

    handle.abort();
}

#[tokio::test]
async fn a_weight_change_is_applied_without_dropping_the_backend() {
    let fake = FakeK8s::start().await;
    let app_context = test_context().await;
    let (_east, east_port) = start_home().await;
    let (_west, west_port) = start_home().await;

    fake.publish(
        "llama",
        "llama",
        &[
            ("cluster-east", east_port, 70),
            ("cluster-west", west_port, 30),
        ],
    );
    let handle = start_multicluster_discovery_with_client(
        fake.client(),
        multicluster_config(),
        Arc::clone(&app_context),
    );
    wait_for_weight(&app_context, east_port, "70").await;

    // Shift the split. Both homes must stay registered throughout.
    fake.publish(
        "llama",
        "llama",
        &[
            ("cluster-east", east_port, 40),
            ("cluster-west", west_port, 60),
        ],
    );
    wait_for_weight(&app_context, east_port, "40").await;
    wait_for_weight(&app_context, west_port, "60").await;

    let urls = app_context.worker_registry.get_all_urls();
    assert!(
        has_url(&urls, east_port) && has_url(&urls, west_port),
        "a reweight must not deregister a home; registry: {urls:?}"
    );

    handle.abort();
}

#[tokio::test]
async fn a_health_gated_home_drops_to_zero_but_stays_registered() {
    // The control plane gates an unhealthy home by weighting it 0. Dropping the
    // backend instead would make its return cost a cold reconnect.
    let fake = FakeK8s::start().await;
    let app_context = test_context().await;
    let (_east, east_port) = start_home().await;
    let (_west, west_port) = start_home().await;

    fake.publish(
        "llama",
        "llama",
        &[
            ("cluster-east", east_port, 50),
            ("cluster-west", west_port, 50),
        ],
    );
    let handle = start_multicluster_discovery_with_client(
        fake.client(),
        multicluster_config(),
        Arc::clone(&app_context),
    );
    wait_for_weight(&app_context, west_port, "50").await;

    fake.publish(
        "llama",
        "llama",
        &[
            ("cluster-east", east_port, 100),
            ("cluster-west", west_port, 0),
        ],
    );
    wait_for_weight(&app_context, west_port, "0").await;

    let urls = app_context.worker_registry.get_all_urls();
    assert!(
        has_url(&urls, west_port),
        "a zero-weighted home must stay registered; registry: {urls:?}"
    );

    handle.abort();
}

#[tokio::test]
async fn a_home_dropped_from_the_table_is_deregistered() {
    let fake = FakeK8s::start().await;
    let app_context = test_context().await;
    let (_east, east_port) = start_home().await;
    let (_west, west_port) = start_home().await;

    fake.publish(
        "llama",
        "llama",
        &[
            ("cluster-east", east_port, 50),
            ("cluster-west", west_port, 50),
        ],
    );
    let handle = start_multicluster_discovery_with_client(
        fake.client(),
        multicluster_config(),
        Arc::clone(&app_context),
    );
    wait_for(
        &app_context,
        "both homes registered",
        Duration::from_secs(20),
        |urls| has_url(urls, east_port) && has_url(urls, west_port),
    )
    .await;

    // Placement moved off the west cluster entirely.
    fake.publish("llama", "llama", &[("cluster-east", east_port, 100)]);
    wait_for(
        &app_context,
        "the departed home is deregistered",
        Duration::from_secs(20),
        |urls| has_url(urls, east_port) && !has_url(urls, west_port),
    )
    .await;

    handle.abort();
}

#[tokio::test]
async fn deleting_the_table_removes_every_home_it_published() {
    let fake = FakeK8s::start().await;
    let app_context = test_context().await;
    let (_east, east_port) = start_home().await;

    fake.publish("llama", "llama", &[("cluster-east", east_port, 100)]);
    let handle = start_multicluster_discovery_with_client(
        fake.client(),
        multicluster_config(),
        Arc::clone(&app_context),
    );
    wait_for(
        &app_context,
        "the home is registered",
        Duration::from_secs(20),
        |urls| has_url(urls, east_port),
    )
    .await;

    fake.delete("llama");
    wait_for(
        &app_context,
        "the home is removed with its table",
        Duration::from_secs(20),
        |urls| !has_url(urls, east_port),
    )
    .await;

    handle.abort();
}

#[tokio::test]
async fn two_services_each_keep_their_own_homes() {
    let fake = FakeK8s::start().await;
    let app_context = test_context().await;
    let (_alpha, alpha_port) = start_home().await;
    let (_beta, beta_port) = start_home().await;

    fake.publish("alpha", "alpha", &[("cluster-east", alpha_port, 100)]);
    fake.publish("beta", "beta", &[("cluster-east", beta_port, 100)]);

    let handle = start_multicluster_discovery_with_client(
        fake.client(),
        multicluster_config(),
        Arc::clone(&app_context),
    );
    wait_for(
        &app_context,
        "both services' homes registered",
        Duration::from_secs(20),
        |urls| has_url(urls, alpha_port) && has_url(urls, beta_port),
    )
    .await;

    // Retiring one service must not disturb the other.
    fake.delete("alpha");
    wait_for(
        &app_context,
        "only the retired service's home is removed",
        Duration::from_secs(20),
        |urls| !has_url(urls, alpha_port) && has_url(urls, beta_port),
    )
    .await;

    handle.abort();
}
