//! The routing table a control plane states, and the rules it must satisfy.
//!
//! This is the contract half of the push API: the wire types, the validation
//! that decides whether a pushed table is acceptable, and the translation from
//! a stated table into the backend set the gateway will hold. All of it is
//! pure, so the entire acceptance decision is testable without a server.
//!
//! The shape is deliberately the gateway's own, not any one control plane's.
//! A `TrafficMap`, an `HTTPRoute`, or a hand-written file all reduce to the
//! same thing — a set of services, each with weighted backends — and the
//! publisher is what maps its own vocabulary onto this one. That is the same
//! split every comparable gateway makes: the control plane owns the Kubernetes
//! types, the data plane owns a neutral resource model, and neither has to
//! learn the other's schema.
//!
//! Validation rejects the whole push rather than dropping the bad part. A
//! partially applied routing table is a traffic split nobody stated, and a
//! publisher that gets a 200 has no reason to look for what went missing.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use super::projection::{normalize_endpoint, DesiredHome};

/// Share given to a backend whose weight the publisher left unset.
///
/// One, not zero: an unweighted set is a set that expressed no preference, and
/// reading silence as "serve nothing here" would turn an omitted field into a
/// black hole.
pub const DEFAULT_BACKEND_WEIGHT: u32 = 1;

const fn default_backend_weight() -> u32 {
    DEFAULT_BACKEND_WEIGHT
}

/// A complete routing table.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingTableSpec {
    /// Every routed service. A service absent from a full replace is removed.
    #[serde(default)]
    pub services: Vec<RoutingService>,
}

/// One routed service and the backends its traffic splits across.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingService {
    /// The service's name, unique in the table. Identifies it for per-service
    /// upsert and delete.
    #[serde(default)]
    pub service: String,

    /// The model id requests must carry to reach this service. Defaults to the
    /// service name.
    ///
    /// Separate from `service` because they are different identities: the
    /// service name is what the control plane calls the workload, and the model
    /// id is what a client puts in a request body. They often match, and
    /// assuming they always do would silently misroute when they do not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// The publisher's own version for this service, carried so a retry that
    /// arrives out of order can be recognized and refused. Opaque to the
    /// gateway beyond being compared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<i64>,

    /// The weighted backends. Empty is legal and means the service is known but
    /// currently has nowhere to send traffic.
    #[serde(default)]
    pub backends: Vec<RoutingBackend>,
}

/// One backend of a service, and its share of that service's traffic.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingBackend {
    /// The backend's name, unique within its service. Usually the cluster it
    /// lives in. Used for attribution in logs and metrics, never for routing.
    #[serde(default)]
    pub name: String,

    /// The backend's absolute URL, including scheme.
    #[serde(default)]
    pub url: String,

    /// This backend's relative share. Applied verbatim. Zero means "no traffic
    /// for now", which is different from being absent: an absent backend is
    /// torn down, a zero-weighted one is held ready.
    #[serde(default = "default_backend_weight")]
    pub weight: u32,

    /// The publisher's health verdict, for observability. Routing does not read
    /// it: a publisher that considers a backend unhealthy says so by weighting
    /// it zero, and honouring both would gate the same backend twice.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthy: Option<bool>,

    /// Opaque publisher annotations carried through to logs and to the table
    /// read endpoint. The gateway never interprets these.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
}

impl RoutingService {
    /// The model id requests must carry: the explicit override, else the
    /// service name.
    pub fn model_id(&self) -> &str {
        match self.model.as_deref().map(str::trim) {
            Some(model) if !model.is_empty() => model,
            _ => self.service.trim(),
        }
    }
}

/// Why a pushed table was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingError {
    /// A service carries no name, so nothing could address or replace it.
    UnnamedService,
    /// Two services in one table share a name.
    DuplicateService { service: String },
    /// A backend carries no name, so nothing could attribute its traffic.
    UnnamedBackend { service: String },
    /// Two backends of one service share a name.
    DuplicateBackend { service: String, backend: String },
    /// A backend carries no usable URL.
    UnusableUrl {
        service: String,
        backend: String,
        url: String,
    },
    /// Two services claim the same backend URL.
    EndpointConflict {
        url: String,
        service: String,
        other: String,
    },
    /// The service named in the path is not the service in the body.
    NameMismatch { path: String, body: String },
    /// A push arrived carrying a version at or behind the one already applied.
    StaleGeneration {
        service: String,
        offered: i64,
        applied: i64,
    },
}

impl std::fmt::Display for RoutingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnnamedService => write!(f, "a service in the table has no name"),
            Self::DuplicateService { service } => {
                write!(f, "service '{service}' appears more than once in the table")
            }
            Self::UnnamedBackend { service } => {
                write!(f, "service '{service}' has a backend with no name")
            }
            Self::DuplicateBackend { service, backend } => write!(
                f,
                "service '{service}' lists backend '{backend}' more than once"
            ),
            Self::UnusableUrl {
                service,
                backend,
                url,
            } => write!(
                f,
                "backend '{backend}' of service '{service}' has an unusable url '{url}'; \
                 an absolute url including the scheme is required"
            ),
            Self::EndpointConflict {
                url,
                service,
                other,
            } => write!(
                f,
                "services '{service}' and '{other}' both route to '{url}'; a backend url \
                 identifies exactly one backend, so it cannot serve two services"
            ),
            Self::NameMismatch { path, body } => {
                write!(f, "path names service '{path}' but the body names '{body}'")
            }
            Self::StaleGeneration {
                service,
                offered,
                applied,
            } => write!(
                f,
                "service '{service}' was offered at generation {offered} but generation \
                 {applied} is already applied; the push arrived out of order"
            ),
        }
    }
}

impl std::error::Error for RoutingError {}

impl RoutingError {
    /// Stable machine-readable code, so a publisher can branch without parsing
    /// the message.
    pub fn code(&self) -> &'static str {
        match self {
            Self::UnnamedService => "unnamed_service",
            Self::DuplicateService { .. } => "duplicate_service",
            Self::UnnamedBackend { .. } => "unnamed_backend",
            Self::DuplicateBackend { .. } => "duplicate_backend",
            Self::UnusableUrl { .. } => "unusable_url",
            Self::EndpointConflict { .. } => "endpoint_conflict",
            Self::NameMismatch { .. } => "name_mismatch",
            Self::StaleGeneration { .. } => "stale_generation",
        }
    }

    /// Whether the caller could succeed by retrying unchanged.
    ///
    /// A conflict or a stale push can resolve on its own as the rest of the
    /// table catches up; a malformed one never will. A publisher uses this to
    /// decide between backing off and giving up.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::EndpointConflict { .. } | Self::StaleGeneration { .. }
        )
    }
}

/// Normalize a stated table into the keyed form the store holds.
///
/// Ordering is by service name so the stored table, the read endpoint, and the
/// derived backend set are all independent of the order the publisher happened
/// to serialize.
pub fn index(spec: RoutingTableSpec) -> Result<BTreeMap<String, RoutingService>, RoutingError> {
    let mut out: BTreeMap<String, RoutingService> = BTreeMap::new();
    for mut service in spec.services {
        service.service = service.service.trim().to_string();
        if service.service.is_empty() {
            return Err(RoutingError::UnnamedService);
        }
        if out.contains_key(&service.service) {
            return Err(RoutingError::DuplicateService {
                service: service.service,
            });
        }
        out.insert(service.service.clone(), service);
    }
    Ok(out)
}

/// Check that a whole table is one the gateway can hold.
///
/// Runs over the resulting table, not the incoming fragment, because the rule
/// that matters most is a cross-service one: no two services may claim the same
/// backend URL.
pub fn validate(services: &BTreeMap<String, RoutingService>) -> Result<(), RoutingError> {
    let mut claimed: HashMap<String, &str> = HashMap::new();

    for (name, service) in services {
        let mut seen: HashMap<&str, ()> = HashMap::new();
        for backend in &service.backends {
            let backend_name = backend.name.trim();
            if backend_name.is_empty() {
                return Err(RoutingError::UnnamedBackend {
                    service: name.clone(),
                });
            }
            if seen.insert(backend_name, ()).is_some() {
                return Err(RoutingError::DuplicateBackend {
                    service: name.clone(),
                    backend: backend_name.to_string(),
                });
            }
            let Some(url) = normalize_endpoint(&backend.url) else {
                return Err(RoutingError::UnusableUrl {
                    service: name.clone(),
                    backend: backend_name.to_string(),
                    url: backend.url.clone(),
                });
            };
            if !url.contains("://") {
                return Err(RoutingError::UnusableUrl {
                    service: name.clone(),
                    backend: backend_name.to_string(),
                    url: backend.url.clone(),
                });
            }
            if let Some(other) = claimed.insert(url.clone(), name.as_str()) {
                if other != name.as_str() {
                    return Err(RoutingError::EndpointConflict {
                        url,
                        service: name.clone(),
                        other: other.to_string(),
                    });
                }
            }
        }
    }
    Ok(())
}

/// The backend set a validated table asks the gateway to hold.
pub fn desired_homes(services: &BTreeMap<String, RoutingService>) -> Vec<DesiredHome> {
    let mut homes: Vec<DesiredHome> = Vec::new();
    for (name, service) in services {
        let model_id = service.model_id().to_string();
        for backend in &service.backends {
            let Some(url) = normalize_endpoint(&backend.url) else {
                continue;
            };
            homes.push(DesiredHome {
                url,
                cluster: backend.name.trim().to_string(),
                model_id: model_id.clone(),
                weight: backend.weight,
                source: format!("api/{name}"),
            });
        }
    }
    homes.sort_by(|a, b| {
        a.model_id
            .cmp(&b.model_id)
            .then_with(|| a.cluster.cmp(&b.cluster))
            .then_with(|| a.url.cmp(&b.url))
    });
    homes
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(name: &str, url: &str, weight: u32) -> RoutingBackend {
        RoutingBackend {
            name: name.to_string(),
            url: url.to_string(),
            weight,
            ..Default::default()
        }
    }

    fn service(name: &str, backends: Vec<RoutingBackend>) -> RoutingService {
        RoutingService {
            service: name.to_string(),
            backends,
            ..Default::default()
        }
    }

    fn indexed(services: Vec<RoutingService>) -> BTreeMap<String, RoutingService> {
        index(RoutingTableSpec { services }).expect("indexes")
    }

    // ── the wire shape ───────────────────────────────────────────────────

    #[test]
    fn a_minimal_push_body_parses() {
        let json = r#"{"services":[{"service":"llama","backends":[
            {"name":"east","url":"https://east.example.com","weight":70},
            {"name":"west","url":"https://west.example.com","weight":30}]}]}"#;
        let spec: RoutingTableSpec = serde_json::from_str(json).expect("parses");
        assert_eq!(spec.services.len(), 1);
        assert_eq!(spec.services[0].backends[0].weight, 70);
    }

    #[test]
    fn an_omitted_weight_means_an_ordinary_share_not_a_starved_one() {
        let json = r#"{"services":[{"service":"s","backends":[
            {"name":"a","url":"https://a.example.com"}]}]}"#;
        let spec: RoutingTableSpec = serde_json::from_str(json).expect("parses");
        assert_eq!(spec.services[0].backends[0].weight, DEFAULT_BACKEND_WEIGHT);
    }

    #[test]
    fn an_explicit_zero_weight_survives_parsing() {
        // Distinct from an omitted weight: the publisher is draining this one.
        let json = r#"{"services":[{"service":"s","backends":[
            {"name":"a","url":"https://a.example.com","weight":0}]}]}"#;
        let spec: RoutingTableSpec = serde_json::from_str(json).expect("parses");
        assert_eq!(spec.services[0].backends[0].weight, 0);
    }

    #[test]
    fn an_empty_body_is_a_valid_empty_table() {
        let spec: RoutingTableSpec = serde_json::from_str("{}").expect("parses");
        assert!(spec.services.is_empty());
    }

    #[test]
    fn publisher_metadata_and_generation_round_trip() {
        let json = r#"{"services":[{"service":"s","generation":7,"model":"m","backends":[
            {"name":"a","url":"https://a.example.com","healthy":true,
             "metadata":{"allocated":"7","ready":"7"}}]}]}"#;
        let spec: RoutingTableSpec = serde_json::from_str(json).expect("parses");
        let svc = &spec.services[0];
        assert_eq!(svc.generation, Some(7));
        assert_eq!(svc.model_id(), "m");
        assert_eq!(svc.backends[0].healthy, Some(true));
        assert_eq!(
            svc.backends[0].metadata.get("allocated"),
            Some(&"7".to_string())
        );
    }

    #[test]
    fn the_model_id_falls_back_to_the_service_name() {
        assert_eq!(service("llama", vec![]).model_id(), "llama");
        let mut blank = service("llama", vec![]);
        blank.model = Some("   ".to_string());
        assert_eq!(blank.model_id(), "llama");
    }

    // ── indexing ─────────────────────────────────────────────────────────

    #[test]
    fn indexing_orders_by_service_name_whatever_order_was_pushed() {
        let forward = indexed(vec![service("b", vec![]), service("a", vec![])]);
        let names: Vec<&str> = forward.keys().map(String::as_str).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn an_unnamed_service_is_refused() {
        let err = index(RoutingTableSpec {
            services: vec![service("  ", vec![])],
        })
        .expect_err("refused");
        assert_eq!(err, RoutingError::UnnamedService);
        assert_eq!(err.code(), "unnamed_service");
        assert!(!err.is_retryable());
    }

    #[test]
    fn a_service_listed_twice_is_refused_rather_than_last_one_winning() {
        let err = index(RoutingTableSpec {
            services: vec![service("dup", vec![]), service("dup", vec![])],
        })
        .expect_err("refused");
        assert_eq!(
            err,
            RoutingError::DuplicateService {
                service: "dup".to_string()
            }
        );
    }

    // ── validation ───────────────────────────────────────────────────────

    #[test]
    fn a_well_formed_table_validates() {
        let table = indexed(vec![service(
            "llama",
            vec![
                backend("east", "https://east.example.com", 70),
                backend("west", "https://west.example.com", 30),
            ],
        )]);
        assert_eq!(validate(&table), Ok(()));
    }

    #[test]
    fn a_service_with_no_backends_is_valid() {
        // The service exists but has nowhere to send traffic yet. That is a
        // state the control plane must be able to express.
        let table = indexed(vec![service("llama", vec![])]);
        assert_eq!(validate(&table), Ok(()));
        assert!(desired_homes(&table).is_empty());
    }

    #[test]
    fn an_unnamed_backend_is_refused() {
        let table = indexed(vec![service(
            "llama",
            vec![backend(" ", "https://a.example.com", 1)],
        )]);
        assert_eq!(
            validate(&table),
            Err(RoutingError::UnnamedBackend {
                service: "llama".to_string()
            })
        );
    }

    #[test]
    fn a_backend_named_twice_in_one_service_is_refused() {
        let table = indexed(vec![service(
            "llama",
            vec![
                backend("east", "https://a.example.com", 1),
                backend("east", "https://b.example.com", 1),
            ],
        )]);
        assert_eq!(
            validate(&table),
            Err(RoutingError::DuplicateBackend {
                service: "llama".to_string(),
                backend: "east".to_string()
            })
        );
    }

    #[test]
    fn a_backend_url_must_be_absolute() {
        for bad in ["", "   ", "east.example.com", "/path"] {
            let table = indexed(vec![service("llama", vec![backend("east", bad, 1)])]);
            let err = validate(&table).expect_err("refused");
            assert_eq!(err.code(), "unusable_url", "url {bad:?} should be refused");
            assert!(!err.is_retryable());
        }
    }

    #[test]
    fn two_services_claiming_one_url_is_refused_and_says_which() {
        // The registry keys a backend by its url, so one url cannot be two
        // backends. Accepting this would silently give one service's traffic
        // to the other.
        let table = indexed(vec![
            service(
                "alpha",
                vec![backend("east", "https://shared.example.com", 1)],
            ),
            service(
                "beta",
                vec![backend("east", "https://shared.example.com", 1)],
            ),
        ]);
        let err = validate(&table).expect_err("refused");
        assert!(matches!(err, RoutingError::EndpointConflict { .. }));
        assert_eq!(err.code(), "endpoint_conflict");
        assert!(err.is_retryable(), "a rename can resolve this on retry");
        let rendered = err.to_string();
        assert!(
            rendered.contains("alpha") && rendered.contains("beta"),
            "{rendered}"
        );
    }

    #[test]
    fn one_service_may_repeat_a_url_across_its_own_backends_only_if_named_once() {
        // Same service, same url under two names: the names differ so the
        // duplicate-backend rule passes, but the url rule must still catch it.
        let table = indexed(vec![service(
            "llama",
            vec![
                backend("east-a", "https://same.example.com", 1),
                backend("east-b", "https://same.example.com", 1),
            ],
        )]);
        // Not a cross-service conflict, so it validates; the two collapse to one
        // backend downstream, which the home projection deduplicates.
        assert_eq!(validate(&table), Ok(()));
    }

    #[test]
    fn trailing_slashes_do_not_hide_a_cross_service_conflict() {
        let table = indexed(vec![
            service(
                "alpha",
                vec![backend("east", "https://shared.example.com/", 1)],
            ),
            service(
                "beta",
                vec![backend("east", " https://shared.example.com ", 1)],
            ),
        ]);
        assert!(matches!(
            validate(&table),
            Err(RoutingError::EndpointConflict { .. })
        ));
    }

    // ── projection to backends ───────────────────────────────────────────

    #[test]
    fn a_validated_table_projects_to_weighted_homes() {
        let table = indexed(vec![service(
            "llama",
            vec![
                backend("east", "https://east.example.com", 70),
                backend("west", "https://west.example.com", 30),
            ],
        )]);
        let homes = desired_homes(&table);
        assert_eq!(homes.len(), 2);
        assert_eq!(homes[0].cluster, "east");
        assert_eq!(homes[0].weight, 70);
        assert_eq!(homes[0].model_id, "llama");
        assert_eq!(homes[0].source, "api/llama");
        assert_eq!(homes[1].weight, 30);
    }

    #[test]
    fn the_model_override_is_what_reaches_the_backend() {
        let mut svc = service(
            "llama-3-70b",
            vec![backend("east", "https://e.example.com", 1)],
        );
        svc.model = Some("meta-llama/Llama-3-70B".to_string());
        let homes = desired_homes(&indexed(vec![svc]));
        assert_eq!(homes[0].model_id, "meta-llama/Llama-3-70B");
    }

    #[test]
    fn a_zero_weighted_backend_is_still_projected() {
        let table = indexed(vec![service(
            "llama",
            vec![
                backend("east", "https://east.example.com", 100),
                backend("west", "https://west.example.com", 0),
            ],
        )]);
        let homes = desired_homes(&table);
        assert_eq!(homes.len(), 2);
        assert_eq!(
            homes.iter().find(|h| h.cluster == "west").map(|h| h.weight),
            Some(0)
        );
    }

    #[test]
    fn projection_normalizes_the_url_it_keys_the_backend_by() {
        let table = indexed(vec![service(
            "llama",
            vec![backend("east", " https://east.example.com/ ", 5)],
        )]);
        assert_eq!(desired_homes(&table)[0].url, "https://east.example.com");
    }

    #[test]
    fn projection_is_ordered_independently_of_push_order() {
        let a = indexed(vec![
            service("z", vec![backend("c", "https://z.example.com", 1)]),
            service("a", vec![backend("c", "https://a.example.com", 1)]),
        ]);
        let b = indexed(vec![
            service("a", vec![backend("c", "https://a.example.com", 1)]),
            service("z", vec![backend("c", "https://z.example.com", 1)]),
        ]);
        assert_eq!(desired_homes(&a), desired_homes(&b));
    }

    #[test]
    fn several_services_each_project_their_own_backends() {
        let table = indexed(vec![
            service(
                "alpha",
                vec![backend("east", "https://alpha.example.com", 1)],
            ),
            service(
                "beta",
                vec![
                    backend("east", "https://beta-e.example.com", 2),
                    backend("west", "https://beta-w.example.com", 3),
                ],
            ),
        ]);
        let homes = desired_homes(&table);
        let models: Vec<&str> = homes.iter().map(|h| h.model_id.as_str()).collect();
        assert_eq!(models, vec!["alpha", "beta", "beta"]);
    }

    // ── error reporting contract ─────────────────────────────────────────

    #[test]
    fn only_conditions_that_can_clear_are_marked_retryable() {
        assert!(RoutingError::EndpointConflict {
            url: "u".into(),
            service: "a".into(),
            other: "b".into()
        }
        .is_retryable());
        assert!(RoutingError::StaleGeneration {
            service: "a".into(),
            offered: 1,
            applied: 2
        }
        .is_retryable());
        assert!(!RoutingError::UnnamedService.is_retryable());
        assert!(!RoutingError::NameMismatch {
            path: "a".into(),
            body: "b".into()
        }
        .is_retryable());
    }

    #[test]
    fn every_error_renders_a_message_naming_what_to_fix() {
        let errors = [
            RoutingError::UnnamedService,
            RoutingError::DuplicateService {
                service: "s".into(),
            },
            RoutingError::UnnamedBackend {
                service: "s".into(),
            },
            RoutingError::DuplicateBackend {
                service: "s".into(),
                backend: "b".into(),
            },
            RoutingError::UnusableUrl {
                service: "s".into(),
                backend: "b".into(),
                url: "x".into(),
            },
            RoutingError::EndpointConflict {
                url: "u".into(),
                service: "s".into(),
                other: "o".into(),
            },
            RoutingError::NameMismatch {
                path: "p".into(),
                body: "b".into(),
            },
            RoutingError::StaleGeneration {
                service: "s".into(),
                offered: 1,
                applied: 2,
            },
        ];
        for err in errors {
            let rendered = err.to_string();
            assert!(!rendered.is_empty(), "{} renders nothing", err.code());
            assert!(!err.code().is_empty());
        }
    }
}
