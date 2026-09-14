//! Routing across clusters from a control-plane-computed traffic split.
//!
//! The gateway's other backend source, [`crate::service_discovery`], watches
//! Pods and produces workers that are *engine processes*: the router knows
//! their cache state, their queue depth, their prefill/decode role, and routes
//! on that. This module produces workers that are *whole clusters*, reached
//! through their ingress. Nothing local is known about them, and nothing local
//! should be: the split across them is a capacity plan computed by a control
//! plane that can see every cluster, and the gateway's job is to enforce it.
//!
//! The two sources compose rather than compete. A gateway deployed globally
//! holds only remote homes; a gateway deployed inside a cluster holds only
//! local workers; one deployed with both routes to whichever the request's
//! model resolves to.
//!
//! # How a routing table arrives
//!
//! [`api`] is the way it is meant to arrive: a control plane pushes the table
//! to this gateway's HTTP API. That is the split every comparable gateway
//! makes. Translating Kubernetes objects into data-plane config is the control
//! plane's job, so serving pods hold no API credentials, the API server is not
//! on the serving path, and one component knows what it programmed and can say
//! so.
//!
//! [`discovery`] is the alternative: the gateway watches the routing tables in
//! the cluster itself. It needs no control-plane integration at all, which
//! makes it the right thing for a single replica, a development cluster, or a
//! control plane that has no publisher yet. It does not scale to a fleet, for
//! the reasons the push API's documentation gives.
//!
//! The two are mutually exclusive. Each states a complete desired backend set
//! on every pass, so running both would leave them deleting each other's
//! backends on alternate reconciles.
//!
//! # Layering
//!
//! - [`table`] and [`trafficmap`] are the two input contracts: the neutral one
//!   a control plane pushes, and the Kubernetes one the watcher reads.
//! - [`projection`] and [`table`] both reduce their input to the same
//!   [`DesiredHome`] set. Both are pure, so every routing decision this module
//!   makes is testable without a cluster or a server.
//! - [`sync`] plans the registry change; [`apply`] is the single writer that
//!   makes it. Neither knows where the table came from.

pub mod api;
pub mod apply;
pub mod discovery;
pub mod projection;
pub mod sync;
pub mod table;
pub mod trafficmap;

pub use api::{RoutingState, RoutingStatus, RoutingTableView};
pub use apply::apply_desired;
#[cfg(feature = "test-util")]
pub use discovery::start_multicluster_discovery_with_client;
pub use discovery::{reconcile_from, start_multicluster_discovery, MultiClusterConfig};
pub use projection::{project, total_weight, DesiredHome, Projection};
pub use sync::{plan_sync, HomeSync, SyncPlan};
pub use table::{RoutingBackend, RoutingError, RoutingService, RoutingTableSpec};
pub use trafficmap::{TrafficMap, TrafficMapEntry, TrafficMapSpec, MODEL_ID_ANNOTATION};

/// Worker label naming the cluster a remote home lives in.
///
/// Carried so metrics, logs, and `GET /workers` can attribute traffic to a
/// cluster. Routing never reads it — the weight decides the share, and a home's
/// identity must not quietly become a second input to that.
pub const HOME_CLUSTER_LABEL: &str = "home_cluster";

/// Worker label naming the routing table a remote home came from.
pub const HOME_SOURCE_LABEL: &str = "home_source";

/// Why a gateway cannot take its routing table from two places at once.
pub const BOTH_SOURCES_ERROR: &str = "multi-cluster routing accepts a pushed table or watches \
    for one, not both: each states the complete backend set on every pass, so together they \
    would delete each other's backends on alternate reconciles";
