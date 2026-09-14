//! The `TrafficMap` consumption contract.
//!
//! A `TrafficMap` is a per-service, control-plane-generated routing table: one
//! entry per serving *home* (a workload cluster), carrying that home's
//! externally-addressable endpoint and a capacity-planned, health-gated weight.
//! The control plane owns the weight math — placement apportionment intersected
//! with quota, scaled by a per-home capacity factor, gated on readiness and on
//! an end-to-end probe. A consumer applies the published weights verbatim and
//! re-derives none of it.
//!
//! These types mirror only the subset of the contract a gateway reads. They are
//! deliberately a narrow, independently-versioned view rather than an import of
//! the producer's full API surface: the gateway depends on the routing
//! contract, not on the control plane's internal schema, and unknown fields
//! deserialize away so the producer can add fields without breaking consumers.

use std::borrow::Cow;

use k8s_openapi::{apimachinery::pkg::apis::meta::v1::ObjectMeta, NamespaceResourceScope};
use kube::Resource;
use serde::{Deserialize, Serialize};

/// API group the routing contract lives in.
pub const TRAFFIC_MAP_GROUP: &str = "ome.io";
/// API version of the routing contract.
pub const TRAFFIC_MAP_VERSION: &str = "v1beta1";
/// Kind of the routing-table object.
pub const TRAFFIC_MAP_KIND: &str = "TrafficMap";
/// Plural resource name, as the apiserver exposes it.
pub const TRAFFIC_MAP_PLURAL: &str = "trafficmaps";

/// Annotation naming the model id the routed service answers to.
///
/// A `TrafficMap` names a *logical service*, which is a control-plane identity;
/// inference requests select a backend by the `model` field in their body,
/// which is a serving identity. Those coincide often enough to make
/// `spec.service` a sane fallback and differ often enough that assuming they
/// always match would silently misroute. The annotation is the explicit
/// override for when they differ.
pub const MODEL_ID_ANNOTATION: &str = "smg.ai/model-id";

/// How the control plane apportioned replicas across homes.
///
/// Carried so a consumer can read the routing intent without reading the
/// source service. It does not change how weights are applied — the published
/// weight is already final under every mode.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PlacementMode {
    /// Exactly one home serves; the map still lists it so cutover is expressible.
    Single,
    /// Every home is a full replica of the service.
    All,
    /// Homes carry an apportioned share of the fleet.
    Split,
    /// A mode this consumer does not know. Weights still apply verbatim.
    #[serde(untagged)]
    Unknown(String),
}

/// The computed routing table for one logical service.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficMapSpec {
    /// The routed logical service.
    #[serde(default)]
    pub service: String,

    /// The placement mode the table was computed under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<PlacementMode>,

    /// One entry per serving home. Empty is a valid, meaningful state: the
    /// service is routed but currently has no home to route to.
    #[serde(default)]
    pub entries: Vec<TrafficMapEntry>,

    /// The source-service generation this table was computed from, so a
    /// consumer can see staleness relative to its source.
    ///
    /// Named explicitly: the published field keeps the acronym uppercase, which
    /// no automatic case convention reproduces.
    #[serde(
        rename = "observedISVCGeneration",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub observed_isvc_generation: Option<i64>,
}

/// One serving home's routing target and the provenance of its weight.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficMapEntry {
    /// The workload cluster serving this home. Unique within a table.
    #[serde(default)]
    pub cluster: String,

    /// The home's externally-addressable URL. Absent means the home has no
    /// address yet and cannot be routed to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,

    /// The home's final relative traffic share. Applied verbatim.
    #[serde(default)]
    pub weight: i32,

    /// The control plane's health verdict for this home.
    #[serde(default)]
    pub healthy: bool,

    /// Why the weight is what it is. Read for observability only — the weight
    /// already accounts for all of it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<TrafficMapCapacity>,

    /// What the control plane's end-to-end probe last observed for this home.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<TrafficMapProbe>,
}

/// Weight provenance: the inputs the control plane resolved the weight from.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficMapCapacity {
    /// Replicas this home is intended to run.
    #[serde(default)]
    pub allocated: i32,
    /// Live ready replicas on this home.
    #[serde(default)]
    pub ready: i32,
    /// Per-replica relative serving capacity, as a quantity string.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub factor: Option<String>,
    /// Which input produced `allocated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// The home's own servable-capacity report, when it answered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reported: Option<i32>,
}

/// The control plane's end-to-end probe result for one home.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficMapProbe {
    /// `Passing`, `Failing`, or `Unknown`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    /// Unbroken failing verdicts.
    #[serde(default)]
    pub consecutive_failures: i32,
    /// What the probe observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Publisher-owned realization status. A consumer that programs itself from the
/// table reports back through this.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficMapStatus {
    /// Whether the active publisher has realized this map.
    #[serde(default)]
    pub programmed: bool,
    /// The spec generation last realized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_traffic_map_generation: Option<i64>,
}

/// A routing table as served by the apiserver.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrafficMap {
    #[serde(flatten)]
    pub types: TypeMeta,
    #[serde(default)]
    pub metadata: ObjectMeta,
    #[serde(default)]
    pub spec: TrafficMapSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<TrafficMapStatus>,
}

/// `apiVersion`/`kind`, kept so round-tripping an object preserves them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
}

// Implemented by hand rather than derived: deriving `CustomResource` would make
// this crate the *owner* of the schema and pull in a schema-generation
// dependency, when all a consumer needs is the group/version/kind coordinates
// to open a typed watch.
impl Resource for TrafficMap {
    type DynamicType = ();
    type Scope = NamespaceResourceScope;

    fn kind((): &()) -> Cow<'_, str> {
        TRAFFIC_MAP_KIND.into()
    }

    fn group((): &()) -> Cow<'_, str> {
        TRAFFIC_MAP_GROUP.into()
    }

    fn version((): &()) -> Cow<'_, str> {
        TRAFFIC_MAP_VERSION.into()
    }

    fn plural((): &()) -> Cow<'_, str> {
        TRAFFIC_MAP_PLURAL.into()
    }

    fn meta(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn meta_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}

impl TrafficMap {
    /// The model id requests must carry to reach this service: the explicit
    /// annotation when set, else the routed service name.
    pub fn model_id(&self) -> &str {
        let annotated = self
            .metadata
            .annotations
            .as_ref()
            .and_then(|a| a.get(MODEL_ID_ANNOTATION))
            .map(|s| s.trim())
            .filter(|s| !s.is_empty());
        match annotated {
            Some(id) => id,
            None => self.spec.service.trim(),
        }
    }

    /// Namespace-qualified name, for logging and for keying homes by source.
    pub fn source_key(&self) -> String {
        let ns = self.metadata.namespace.as_deref().unwrap_or_default();
        let name = self.metadata.name.as_deref().unwrap_or_default();
        format!("{ns}/{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
      "apiVersion": "ome.io/v1beta1",
      "kind": "TrafficMap",
      "metadata": {
        "name": "llama-3-70b",
        "namespace": "team-a",
        "generation": 5
      },
      "spec": {
        "service": "llama-3-70b",
        "mode": "Split",
        "observedISVCGeneration": 42,
        "entries": [
          {
            "cluster": "cluster-east",
            "endpoint": "https://llama-3-70b.team-a.east.example.com",
            "weight": 70,
            "healthy": true,
            "capacity": {
              "allocated": 7,
              "ready": 7,
              "factor": "1",
              "source": "ControlPlane"
            },
            "probe": {
              "result": "Passing",
              "consecutiveFailures": 0
            }
          },
          {
            "cluster": "cluster-west",
            "endpoint": "https://llama-3-70b.team-a.west.example.com",
            "weight": 30,
            "healthy": true,
            "capacity": { "allocated": 3, "ready": 3 }
          }
        ]
      },
      "status": { "programmed": true, "observedTrafficMapGeneration": 5 }
    }"#;

    #[test]
    fn deserializes_the_published_contract() {
        let tm: TrafficMap = serde_json::from_str(SAMPLE).expect("sample parses");
        assert_eq!(tm.spec.service, "llama-3-70b");
        assert_eq!(tm.spec.mode, Some(PlacementMode::Split));
        assert_eq!(tm.spec.observed_isvc_generation, Some(42));
        assert_eq!(tm.spec.entries.len(), 2);

        let east = &tm.spec.entries[0];
        assert_eq!(east.cluster, "cluster-east");
        assert_eq!(
            east.endpoint.as_deref(),
            Some("https://llama-3-70b.team-a.east.example.com")
        );
        assert_eq!(east.weight, 70);
        assert!(east.healthy);

        let capacity = east.capacity.as_ref().expect("capacity present");
        assert_eq!(capacity.allocated, 7);
        assert_eq!(capacity.ready, 7);
        assert_eq!(capacity.factor.as_deref(), Some("1"));
        assert_eq!(capacity.source.as_deref(), Some("ControlPlane"));

        let probe = east.probe.as_ref().expect("probe present");
        assert_eq!(probe.result.as_deref(), Some("Passing"));

        // A home may publish no probe at all; that is not a parse failure.
        assert!(tm.spec.entries[1].probe.is_none());

        let status = tm.status.as_ref().expect("status present");
        assert!(status.programmed);
    }

    #[test]
    fn an_empty_table_is_a_valid_state_not_an_error() {
        let json = r#"{"metadata":{"name":"svc","namespace":"ns"},"spec":{"service":"svc"}}"#;
        let tm: TrafficMap = serde_json::from_str(json).expect("empty table parses");
        assert!(tm.spec.entries.is_empty());
        assert_eq!(tm.spec.mode, None);
    }

    #[test]
    fn unknown_fields_and_modes_do_not_break_the_consumer() {
        // The producer may add fields and enum values; a consumer pinned to an
        // older view must keep routing rather than fail closed.
        let json = r#"{
          "metadata": {"name": "svc", "namespace": "ns"},
          "spec": {
            "service": "svc",
            "mode": "Sharded",
            "someFutureField": {"a": 1},
            "entries": [{"cluster": "c", "endpoint": "https://c.example.com",
                         "weight": 5, "healthy": true, "futureEntryField": true}]
          }
        }"#;
        let tm: TrafficMap = serde_json::from_str(json).expect("forward-compatible parse");
        assert_eq!(tm.spec.mode, Some(PlacementMode::Unknown("Sharded".into())));
        assert_eq!(tm.spec.entries[0].weight, 5);
    }

    #[test]
    fn model_id_prefers_the_annotation_over_the_service_name() {
        let json = r#"{
          "metadata": {"name": "svc", "namespace": "ns",
                       "annotations": {"smg.ai/model-id": "meta-llama/Llama-3-70B"}},
          "spec": {"service": "svc"}
        }"#;
        let tm: TrafficMap = serde_json::from_str(json).expect("parses");
        assert_eq!(tm.model_id(), "meta-llama/Llama-3-70B");
    }

    #[test]
    fn model_id_falls_back_to_the_service_name() {
        let json = r#"{"metadata":{"name":"svc","namespace":"ns"},"spec":{"service":"my-svc"}}"#;
        let tm: TrafficMap = serde_json::from_str(json).expect("parses");
        assert_eq!(tm.model_id(), "my-svc");
    }

    #[test]
    fn a_blank_annotation_does_not_shadow_the_service_name() {
        let json = r#"{
          "metadata": {"name": "svc", "namespace": "ns",
                       "annotations": {"smg.ai/model-id": "   "}},
          "spec": {"service": "my-svc"}
        }"#;
        let tm: TrafficMap = serde_json::from_str(json).expect("parses");
        assert_eq!(tm.model_id(), "my-svc");
    }

    #[test]
    fn resource_coordinates_address_the_published_crd() {
        assert_eq!(TrafficMap::group(&()), "ome.io");
        assert_eq!(TrafficMap::version(&()), "v1beta1");
        assert_eq!(TrafficMap::kind(&()), "TrafficMap");
        assert_eq!(TrafficMap::plural(&()), "trafficmaps");
        assert_eq!(
            TrafficMap::url_path(&(), Some("team-a")),
            "/apis/ome.io/v1beta1/namespaces/team-a/trafficmaps"
        );
    }

    #[test]
    fn source_key_qualifies_the_name_with_its_namespace() {
        let tm: TrafficMap = serde_json::from_str(SAMPLE).expect("parses");
        assert_eq!(tm.source_key(), "team-a/llama-3-70b");
    }
}
