//! Integration tests for the cross-cluster routing push API.
//!
//! The real axum router, routing store, sync planner, job queue, and worker
//! registry run end to end over real HTTP; only the remote cluster ingresses
//! are test doubles. Each "home" is a mock engine standing in for another
//! cluster's ingress.
#![allow(clippy::unwrap_used, clippy::allow_attributes)]

mod common;

use std::{sync::Arc, time::Duration};

use axum::Router;
use common::mock_worker::{HealthStatus, MockWorker, MockWorkerConfig, WorkerType};
use serde_json::{json, Value};
use smg::{
    app_context::AppContext,
    config::RouterConfig,
    multicluster::{api as routing_api, RoutingState, HOME_CLUSTER_LABEL, HOME_SOURCE_LABEL},
    policies::ROUTING_WEIGHT_LABEL,
};

/// A running gateway routing API plus the context its pushes land in.
struct Harness {
    base_url: String,
    app_context: Arc<AppContext>,
    client: reqwest::Client,
}

impl Harness {
    #[expect(clippy::disallowed_methods, reason = "test infrastructure")]
    async fn start() -> Self {
        let config = RouterConfig::builder()
            .worker_startup_timeout_secs(2)
            .build_unchecked();
        let app_context = common::create_test_context(config).await;
        // A short interval so the level-triggered second pass, which repairs a
        // change that raced an in-flight registration, runs inside the test.
        let state = Arc::new(RoutingState::new(
            Arc::clone(&app_context),
            Duration::from_millis(300),
        ));
        state.spawn_reconciler();

        // Mount exactly as the server does, minus the auth layer: these tests
        // are about the API's own behaviour, and auth is covered where it is
        // applied.
        let app: Router = Router::new().nest("/v1/routing", routing_api::router(state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        Self {
            base_url: format!("http://{addr}"),
            app_context,
            client: reqwest::Client::new(),
        }
    }

    async fn put_table(&self, body: Value) -> (reqwest::StatusCode, Value) {
        let resp = self
            .client
            .put(format!("{}/v1/routing/services", self.base_url))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap_or(Value::Null))
    }

    async fn put_service(&self, name: &str, body: Value) -> (reqwest::StatusCode, Value) {
        let resp = self
            .client
            .put(format!("{}/v1/routing/services/{name}", self.base_url))
            .json(&body)
            .send()
            .await
            .unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap_or(Value::Null))
    }

    async fn delete_service(&self, name: &str) -> (reqwest::StatusCode, Value) {
        let resp = self
            .client
            .delete(format!("{}/v1/routing/services/{name}", self.base_url))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap_or(Value::Null))
    }

    async fn get(&self, path: &str) -> (reqwest::StatusCode, Value) {
        let resp = self
            .client
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .unwrap();
        let status = resp.status();
        (status, resp.json().await.unwrap_or(Value::Null))
    }

    async fn wait_for(&self, what: &str, predicate: impl Fn(&[String]) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let urls = self.app_context.worker_registry.get_all_urls();
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

    async fn wait_for_weight(&self, port: u16, weight: &str) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let found = self
                .app_context
                .worker_registry
                .get_all()
                .into_iter()
                .find(|w| w.url().ends_with(&format!("127.0.0.1:{port}")));
            if let Some(worker) = &found {
                if worker.metadata().spec.labels.get(ROUTING_WEIGHT_LABEL)
                    == Some(&weight.to_string())
                {
                    return;
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for :{port} to reach weight {weight}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
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

fn home(name: &str, port: u16, weight: u32) -> Value {
    json!({"name": name, "url": format!("http://127.0.0.1:{port}"), "weight": weight})
}

fn has_url(urls: &[String], port: u16) -> bool {
    urls.iter()
        .any(|url| url.ends_with(&format!("127.0.0.1:{port}")))
}

// ── the empty gateway ────────────────────────────────────────────────────

#[tokio::test]
async fn a_gateway_that_has_never_been_pushed_to_says_so_plainly() {
    // A publisher reads this to decide whether it must resync, so an
    // unprogrammed gateway has to be distinguishable from an idle one.
    let h = Harness::start().await;
    let (status, body) = h.get("/v1/routing/status").await;
    assert_eq!(status, 200);
    assert_eq!(body["generation"], 0);
    assert_eq!(body["services"], 0);
    assert_eq!(body["backends"], 0);
    assert!(body.get("last_applied").is_none(), "{body}");
    assert!(body.get("age_seconds").is_none(), "{body}");
}

// ── pushing a table ──────────────────────────────────────────────────────

#[tokio::test]
async fn a_pushed_table_becomes_weighted_backends() {
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    let (_west, west) = start_home().await;

    let (status, body) = h
        .put_table(json!({"services": [
            {"service": "llama", "backends": [home("east", east, 70), home("west", west, 30)]}
        ]}))
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["generation"], 1);
    assert_eq!(body["services"], 1);
    assert_eq!(body["backends"], 2);

    h.wait_for("both homes registered", |urls| {
        has_url(urls, east) && has_url(urls, west)
    })
    .await;

    for worker in h.app_context.worker_registry.get_all() {
        let labels = &worker.metadata().spec.labels;
        assert_eq!(
            labels.get(HOME_SOURCE_LABEL),
            Some(&"api/llama".to_string())
        );
        let cluster = labels.get(HOME_CLUSTER_LABEL).expect("cluster label");
        let expected = if cluster == "east" { "70" } else { "30" };
        assert_eq!(
            labels.get(ROUTING_WEIGHT_LABEL),
            Some(&expected.to_string())
        );
    }
}

#[tokio::test]
async fn the_model_override_is_what_the_backend_answers_to() {
    let h = Harness::start().await;
    let (_east, east) = start_home().await;

    let (status, _) = h
        .put_table(json!({"services": [
            {"service": "llama-3-70b", "model": "meta-llama/Llama-3-70B",
             "backends": [home("east", east, 1)]}
        ]}))
        .await;
    assert_eq!(status, 200);
    h.wait_for("home registered", |urls| has_url(urls, east))
        .await;

    let worker = h
        .app_context
        .worker_registry
        .get_all()
        .into_iter()
        .find(|w| w.url().ends_with(&format!("127.0.0.1:{east}")))
        .expect("home registered");
    assert_eq!(
        worker.metadata().spec.labels.get("served_model_name"),
        Some(&"meta-llama/Llama-3-70B".to_string())
    );
}

#[tokio::test]
async fn re_pushing_an_unchanged_table_does_not_look_like_a_change() {
    // A publisher re-states the whole table on a timer. If every restatement
    // bumped the generation, the one signal that means "something moved" would
    // be pure noise.
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    let table = json!({"services": [
        {"service": "llama", "backends": [home("east", east, 100)]}
    ]});

    let (_, first) = h.put_table(table.clone()).await;
    assert_eq!(first["generation"], 1);
    let applied = first["last_applied"].clone();

    for _ in 0..3 {
        let (status, body) = h.put_table(table.clone()).await;
        assert_eq!(status, 200);
        assert_eq!(body["generation"], 1, "an unchanged push must not bump");
        assert_eq!(body["last_applied"], applied);
    }
}

#[tokio::test]
async fn a_full_replace_removes_the_services_it_leaves_out() {
    let h = Harness::start().await;
    let (_alpha, alpha) = start_home().await;
    let (_beta, beta) = start_home().await;

    h.put_table(json!({"services": [
        {"service": "alpha", "backends": [home("east", alpha, 1)]},
        {"service": "beta", "backends": [home("east", beta, 1)]}
    ]}))
    .await;
    h.wait_for("both services registered", |urls| {
        has_url(urls, alpha) && has_url(urls, beta)
    })
    .await;

    let (status, body) = h
        .put_table(json!({"services": [
            {"service": "alpha", "backends": [home("east", alpha, 1)]}
        ]}))
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["services"], 1);
    h.wait_for("the omitted service is torn down", |urls| {
        has_url(urls, alpha) && !has_url(urls, beta)
    })
    .await;
}

#[tokio::test]
async fn a_weight_change_moves_the_share_without_dropping_the_backend() {
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    let (_west, west) = start_home().await;

    h.put_table(json!({"services": [
        {"service": "llama", "backends": [home("east", east, 70), home("west", west, 30)]}
    ]}))
    .await;
    h.wait_for_weight(east, "70").await;

    h.put_table(json!({"services": [
        {"service": "llama", "backends": [home("east", east, 40), home("west", west, 60)]}
    ]}))
    .await;
    h.wait_for_weight(east, "40").await;
    h.wait_for_weight(west, "60").await;

    let urls = h.app_context.worker_registry.get_all_urls();
    assert!(
        has_url(&urls, east) && has_url(&urls, west),
        "a reweight must not deregister a home; registry: {urls:?}"
    );
}

// ── per-service operations ───────────────────────────────────────────────

#[tokio::test]
async fn upserting_one_service_leaves_the_rest_of_the_table_alone() {
    let h = Harness::start().await;
    let (_alpha, alpha) = start_home().await;
    let (_beta, beta) = start_home().await;

    h.put_table(json!({"services": [
        {"service": "alpha", "backends": [home("east", alpha, 1)]}
    ]}))
    .await;
    h.wait_for("alpha registered", |urls| has_url(urls, alpha))
        .await;

    let (status, body) = h
        .put_service(
            "beta",
            json!({"service": "beta", "backends": [home("east", beta, 1)]}),
        )
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["services"], 2);
    h.wait_for("both services present", |urls| {
        has_url(urls, alpha) && has_url(urls, beta)
    })
    .await;
}

#[tokio::test]
async fn a_service_may_be_upserted_without_naming_itself_in_the_body() {
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    let (status, body) = h
        .put_service("llama", json!({"backends": [home("east", east, 5)]}))
        .await;
    assert_eq!(status, 200, "{body}");
    let (_, view) = h.get("/v1/routing/services").await;
    assert_eq!(view["services"][0]["service"], "llama");
}

#[tokio::test]
async fn a_body_naming_a_different_service_than_the_path_is_refused() {
    let h = Harness::start().await;
    let (status, body) = h
        .put_service("llama", json!({"service": "mistral", "backends": []}))
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], "name_mismatch");
    assert_eq!(body["retryable"], false);
}

#[tokio::test]
async fn deleting_a_service_tears_down_exactly_its_backends() {
    let h = Harness::start().await;
    let (_alpha, alpha) = start_home().await;
    let (_beta, beta) = start_home().await;

    h.put_table(json!({"services": [
        {"service": "alpha", "backends": [home("east", alpha, 1)]},
        {"service": "beta", "backends": [home("east", beta, 1)]}
    ]}))
    .await;
    h.wait_for("both registered", |urls| {
        has_url(urls, alpha) && has_url(urls, beta)
    })
    .await;

    let (status, body) = h.delete_service("alpha").await;
    assert_eq!(status, 200);
    assert_eq!(body["services"], 1);
    h.wait_for("only alpha is torn down", |urls| {
        !has_url(urls, alpha) && has_url(urls, beta)
    })
    .await;
}

#[tokio::test]
async fn deleting_a_service_that_was_never_there_succeeds() {
    // A publisher tearing down a service this replica never heard of has
    // arrived at the state it asked for. Reporting an error would only start
    // a retry loop over nothing.
    let h = Harness::start().await;
    let (status, body) = h.delete_service("ghost").await;
    assert_eq!(status, 200);
    assert_eq!(body["generation"], 0, "a no-op delete must not bump");
}

// ── reading back ─────────────────────────────────────────────────────────

#[tokio::test]
async fn the_table_reads_back_as_it_was_pushed() {
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    h.put_table(json!({"services": [
        {"service": "llama", "generation": 7,
         "backends": [{"name": "east", "url": format!("http://127.0.0.1:{east}"),
                       "weight": 70, "healthy": true,
                       "metadata": {"allocated": "7"}}]}
    ]}))
    .await;

    let (status, body) = h.get("/v1/routing/services").await;
    assert_eq!(status, 200);
    assert_eq!(body["generation"], 1);
    let svc = &body["services"][0];
    assert_eq!(svc["service"], "llama");
    assert_eq!(svc["generation"], 7);
    assert_eq!(svc["backends"][0]["weight"], 70);
    assert_eq!(svc["backends"][0]["healthy"], true);
    assert_eq!(svc["backends"][0]["metadata"]["allocated"], "7");
}

#[tokio::test]
async fn one_service_can_be_read_back_on_its_own() {
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    h.put_table(json!({"services": [
        {"service": "llama", "backends": [home("east", east, 1)]}
    ]}))
    .await;

    let (status, body) = h.get("/v1/routing/services/llama").await;
    assert_eq!(status, 200);
    assert_eq!(body["service"], "llama");

    let (status, body) = h.get("/v1/routing/services/ghost").await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], "service_not_found");
}

#[tokio::test]
async fn status_reports_an_age_once_a_table_has_been_applied() {
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    h.put_table(json!({"services": [
        {"service": "llama", "backends": [home("east", east, 1)]}
    ]}))
    .await;

    let (_, body) = h.get("/v1/routing/status").await;
    assert!(body["last_applied"].is_string(), "{body}");
    assert!(body["age_seconds"].is_u64(), "{body}");
}

// ── refusing a bad table ─────────────────────────────────────────────────

#[tokio::test]
async fn two_services_claiming_one_url_is_refused_as_a_conflict() {
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    let (status, body) = h
        .put_table(json!({"services": [
            {"service": "alpha", "backends": [home("east", east, 1)]},
            {"service": "beta", "backends": [home("east", east, 1)]}
        ]}))
        .await;
    assert_eq!(status, 409);
    assert_eq!(body["code"], "endpoint_conflict");
    assert_eq!(body["retryable"], true);
}

#[tokio::test]
async fn a_refused_push_leaves_the_previous_table_intact() {
    // Rejecting the whole push is only safe if nothing partial landed.
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    h.put_table(json!({"services": [
        {"service": "llama", "backends": [home("east", east, 1)]}
    ]}))
    .await;
    h.wait_for("the good table applied", |urls| has_url(urls, east))
        .await;

    let (status, _) = h
        .put_table(json!({"services": [
            {"service": "llama", "backends": [home("east", east, 1)]},
            {"service": "broken", "backends": [{"name": "x", "url": "not-a-url"}]}
        ]}))
        .await;
    assert_eq!(status, 400);

    let (_, body) = h.get("/v1/routing/status").await;
    assert_eq!(
        body["generation"], 1,
        "the refused push must not have landed"
    );
    assert_eq!(body["services"], 1);
    assert!(has_url(&h.app_context.worker_registry.get_all_urls(), east));
}

#[tokio::test]
async fn a_backend_url_without_a_scheme_is_refused() {
    let h = Harness::start().await;
    let (status, body) = h
        .put_table(json!({"services": [
            {"service": "llama", "backends": [{"name": "east", "url": "east.example.com"}]}
        ]}))
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], "unusable_url");
    assert_eq!(body["retryable"], false);
}

#[tokio::test]
async fn an_unnamed_service_is_refused() {
    let h = Harness::start().await;
    let (status, body) = h
        .put_table(json!({"services": [{"service": "  ", "backends": []}]}))
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], "unnamed_service");
}

// ── out-of-order pushes ──────────────────────────────────────────────────

#[tokio::test]
async fn a_push_that_arrives_behind_what_is_applied_is_refused() {
    let h = Harness::start().await;
    let (_east, east) = start_home().await;

    let (status, _) = h
        .put_service(
            "llama",
            json!({"generation": 5, "backends": [home("east", east, 70)]}),
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = h
        .put_service(
            "llama",
            json!({"generation": 4, "backends": [home("east", east, 10)]}),
        )
        .await;
    assert_eq!(status, 409);
    assert_eq!(body["code"], "stale_generation");
    assert_eq!(body["retryable"], true);

    // And the late arrival changed nothing.
    let (_, view) = h.get("/v1/routing/services/llama").await;
    assert_eq!(view["backends"][0]["weight"], 70);
}

#[tokio::test]
async fn re_sending_the_same_generation_is_an_accepted_retry_not_a_conflict() {
    // A publisher whose write landed but whose response was lost must be able
    // to retry. Treating that as a lost race would wedge it.
    let h = Harness::start().await;
    let (_east, east) = start_home().await;
    let body = json!({"generation": 5, "backends": [home("east", east, 70)]});

    let (first, _) = h.put_service("llama", body.clone()).await;
    assert_eq!(first, 200);
    let (second, payload) = h.put_service("llama", body).await;
    assert_eq!(second, 200, "{payload}");
}

#[tokio::test]
async fn a_full_replace_is_never_refused_for_being_out_of_order() {
    // Full replace is the resync path. Making it conditional would let one bad
    // comparison wedge a gateway permanently out of date.
    let h = Harness::start().await;
    let (_east, east) = start_home().await;

    h.put_service(
        "llama",
        json!({"generation": 9, "backends": [home("east", east, 70)]}),
    )
    .await;

    let (status, body) = h
        .put_table(json!({"services": [
            {"service": "llama", "generation": 1, "backends": [home("east", east, 10)]}
        ]}))
        .await;
    assert_eq!(status, 200, "{body}");
    h.wait_for_weight(east, "10").await;
}
