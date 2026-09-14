//! Routing across clusters from a control-plane-published traffic split.
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
//! The layering is deliberate and worth stating, because it is the thing that
//! keeps this small:
//!
//! - [`trafficmap`] is the wire contract, and nothing more.
//! - [`projection`] turns observed tables into a desired backend set. It is
//!   pure, so every routing decision this module makes is testable without a
//!   cluster.
//! - [`sync`] is the only part that touches the apiserver or mutates the
//!   registry, and it holds no policy of its own.

pub mod discovery;
pub mod projection;
pub mod sync;
pub mod trafficmap;

#[cfg(feature = "test-util")]
pub use discovery::start_multicluster_discovery_with_client;
pub use discovery::{reconcile_from, start_multicluster_discovery, MultiClusterConfig};
pub use projection::{project, total_weight, DesiredHome, Projection};
pub use sync::{plan_sync, HomeSync, SyncPlan};
pub use trafficmap::{TrafficMap, TrafficMapEntry, TrafficMapSpec, MODEL_ID_ANNOTATION};

/// Worker label naming the cluster a remote home lives in.
///
/// Carried so metrics, logs, and `GET /workers` can attribute traffic to a
/// cluster. Routing never reads it — the weight decides the share, and a home's
/// identity must not quietly become a second input to that.
pub const HOME_CLUSTER_LABEL: &str = "home_cluster";

/// Worker label naming the routing table a remote home came from.
pub const HOME_SOURCE_LABEL: &str = "home_source";
