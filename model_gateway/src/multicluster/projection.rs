//! Projecting published routing tables onto the gateway's backend set.
//!
//! This is the whole decision layer of multi-cluster routing, and it is a pure
//! function of the observed tables: given every `TrafficMap` the gateway can
//! see, produce the set of remote homes it should hold open and the relative
//! share each one takes. Keeping it pure is what makes the routing behavior
//! testable without an apiserver, a network, or a clock.
//!
//! The weights are applied exactly as published. The control plane already
//! intersected placement with quota, scaled by per-home capacity, gated on
//! readiness and on its own end-to-end probe, and normalized the result. A
//! gateway that re-derived any of that would be running a second, divergent
//! planner against staler inputs.

use std::collections::{BTreeMap, HashMap};

use super::trafficmap::TrafficMap;

/// One remote home the gateway should be able to route to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DesiredHome {
    /// The home's endpoint, normalized for use as a stable backend key.
    pub url: String,
    /// The workload cluster this home lives in.
    pub cluster: String,
    /// The model id requests must carry to select this home's service.
    pub model_id: String,
    /// The home's published relative share. Zero means "serve no traffic here
    /// for now", not "forget this home".
    pub weight: u32,
    /// The routing table this home came from, as `namespace/name`.
    pub source: String,
}

/// The projected backend set, plus anything about the input a human needs to
/// know. Conflicts are reported rather than silently resolved: a dropped home
/// is a traffic-affecting event, and the only honest place to surface it is
/// where it was detected.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Projection {
    /// Homes to hold open, ordered deterministically.
    pub homes: Vec<DesiredHome>,
    /// Input problems worth logging. Never fatal.
    pub warnings: Vec<String>,
}

/// Normalize an endpoint into a stable backend key.
///
/// Trailing slashes and surrounding whitespace are cosmetic in a URL but not in
/// a map key, so two spellings of one home would otherwise open two backends.
fn normalize_endpoint(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_string())
}

/// Project every observed routing table onto the set of homes to route across.
///
/// Entries without an endpoint are skipped: a home with no address is not yet a
/// routing target, which is a normal state during placement, not an error.
/// Entries whose weight is zero are kept — the control plane is saying "no
/// traffic here right now", and dropping the backend would turn a weight change
/// into a connection teardown and a cold reconnect when the weight returns.
pub fn project(maps: &[TrafficMap]) -> Projection {
    let mut by_url: BTreeMap<String, DesiredHome> = BTreeMap::new();
    let mut warnings: Vec<String> = Vec::new();

    // Iterate in a stable order so which side of a conflict wins never depends
    // on watch-event arrival order.
    let mut ordered: Vec<&TrafficMap> = maps.iter().collect();
    ordered.sort_by_key(|m| m.source_key());

    for map in ordered {
        let source = map.source_key();
        let model_id = map.model_id().to_string();
        if model_id.is_empty() {
            warnings.push(format!(
                "{source}: routing table names no service and carries no model-id annotation; skipped"
            ));
            continue;
        }

        let mut seen_clusters: HashMap<&str, ()> = HashMap::new();
        for entry in &map.spec.entries {
            let cluster = entry.cluster.trim();
            if cluster.is_empty() {
                warnings.push(format!("{source}: entry with no cluster name; skipped"));
                continue;
            }
            if seen_clusters.insert(cluster, ()).is_some() {
                warnings.push(format!(
                    "{source}: cluster {cluster} appears more than once; kept the first"
                ));
                continue;
            }

            let Some(endpoint) = entry.endpoint.as_deref().and_then(normalize_endpoint) else {
                // A placed home that has not published an address yet.
                continue;
            };

            // A negative weight cannot mean less than no traffic.
            let weight = u32::try_from(entry.weight).unwrap_or(0);

            let home = DesiredHome {
                url: endpoint,
                cluster: cluster.to_string(),
                model_id: model_id.clone(),
                weight,
                source: source.clone(),
            };

            if let Some(existing) = by_url.get(&home.url) {
                if existing.source != home.source {
                    warnings.push(format!(
                        "{}: endpoint {} is already routed for {} by {}; skipped",
                        home.source, home.url, existing.model_id, existing.source
                    ));
                }
                continue;
            }
            by_url.insert(home.url.clone(), home);
        }
    }

    let mut homes: Vec<DesiredHome> = by_url.into_values().collect();
    homes.sort_by(|a, b| {
        a.model_id
            .cmp(&b.model_id)
            .then_with(|| a.cluster.cmp(&b.cluster))
            .then_with(|| a.url.cmp(&b.url))
    });

    Projection { homes, warnings }
}

/// Sum the published weights of a set of homes.
///
/// An all-zero total is treated by the selection path as "spread evenly", never
/// as "drop the traffic". The publisher's contract is that it never emits an
/// all-zero table for exactly that reason, so reaching zero means the contract
/// was broken somewhere upstream — and the safe reading of a broken routing
/// table is to keep serving, not to black-hole a live service.
pub fn total_weight(homes: &[DesiredHome]) -> u64 {
    homes.iter().map(|h| u64::from(h.weight)).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::multicluster::trafficmap::{TrafficMapEntry, TrafficMapSpec};

    fn map_with(ns: &str, name: &str, service: &str, entries: Vec<TrafficMapEntry>) -> TrafficMap {
        let mut tm = TrafficMap {
            spec: TrafficMapSpec {
                service: service.to_string(),
                entries,
                ..Default::default()
            },
            ..Default::default()
        };
        tm.metadata.namespace = Some(ns.to_string());
        tm.metadata.name = Some(name.to_string());
        tm
    }

    fn entry(cluster: &str, endpoint: Option<&str>, weight: i32, healthy: bool) -> TrafficMapEntry {
        TrafficMapEntry {
            cluster: cluster.to_string(),
            endpoint: endpoint.map(str::to_string),
            weight,
            healthy,
            ..Default::default()
        }
    }

    #[test]
    fn a_split_table_projects_both_homes_with_their_published_weights() {
        let maps = vec![map_with(
            "team-a",
            "llama",
            "llama",
            vec![
                entry("east", Some("https://east.example.com"), 70, true),
                entry("west", Some("https://west.example.com"), 30, true),
            ],
        )];
        let p = project(&maps);
        assert!(
            p.warnings.is_empty(),
            "unexpected warnings: {:?}",
            p.warnings
        );
        assert_eq!(p.homes.len(), 2);
        assert_eq!(p.homes[0].cluster, "east");
        assert_eq!(p.homes[0].weight, 70);
        assert_eq!(p.homes[0].model_id, "llama");
        assert_eq!(p.homes[0].source, "team-a/llama");
        assert_eq!(p.homes[1].cluster, "west");
        assert_eq!(p.homes[1].weight, 30);
        assert_eq!(total_weight(&p.homes), 100);
    }

    #[test]
    fn a_zero_weight_home_is_kept_as_a_backend_but_takes_no_share() {
        // Keeping it is the point: a health-gated home returns to service by a
        // weight change, and it should not also have to pay a cold reconnect.
        let maps = vec![map_with(
            "team-a",
            "llama",
            "llama",
            vec![
                entry("east", Some("https://east.example.com"), 100, true),
                entry("west", Some("https://west.example.com"), 0, false),
            ],
        )];
        let p = project(&maps);
        assert_eq!(p.homes.len(), 2);
        let west = p
            .homes
            .iter()
            .find(|h| h.cluster == "west")
            .expect("west kept");
        assert_eq!(west.weight, 0);
        assert_eq!(total_weight(&p.homes), 100);
    }

    #[test]
    fn a_home_without_an_endpoint_is_not_a_routing_target_yet() {
        let maps = vec![map_with(
            "team-a",
            "llama",
            "llama",
            vec![
                entry("east", Some("https://east.example.com"), 50, true),
                entry("west", None, 50, true),
            ],
        )];
        let p = project(&maps);
        assert_eq!(p.homes.len(), 1);
        assert_eq!(p.homes[0].cluster, "east");
        // Not a warning: an unaddressed home is normal mid-placement.
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn an_empty_table_projects_no_homes_and_raises_no_alarm() {
        let maps = vec![map_with("team-a", "llama", "llama", vec![])];
        let p = project(&maps);
        assert!(p.homes.is_empty());
        assert!(p.warnings.is_empty());
    }

    #[test]
    fn trailing_slashes_do_not_open_a_second_backend_for_one_home() {
        let maps = vec![
            map_with(
                "team-a",
                "llama",
                "llama",
                vec![entry("east", Some("https://east.example.com/"), 10, true)],
            ),
            map_with(
                "team-a",
                "zzz-llama",
                "llama",
                vec![entry(
                    "east",
                    Some("  https://east.example.com  "),
                    10,
                    true,
                )],
            ),
        ];
        let p = project(&maps);
        assert_eq!(p.homes.len(), 1);
        assert_eq!(p.homes[0].url, "https://east.example.com");
        assert_eq!(p.homes.len(), 1);
        assert_eq!(p.warnings.len(), 1, "the collision must be reported");
    }

    #[test]
    fn two_services_claiming_one_endpoint_is_reported_not_silently_merged() {
        let maps = vec![
            map_with(
                "team-a",
                "alpha",
                "alpha",
                vec![entry("east", Some("https://shared.example.com"), 10, true)],
            ),
            map_with(
                "team-a",
                "beta",
                "beta",
                vec![entry("east", Some("https://shared.example.com"), 10, true)],
            ),
        ];
        let p = project(&maps);
        assert_eq!(p.homes.len(), 1);
        assert_eq!(
            p.homes[0].model_id, "alpha",
            "stable order decides the winner"
        );
        assert_eq!(p.warnings.len(), 1);
        assert!(
            p.warnings[0].contains("already routed"),
            "warning should name the conflict: {}",
            p.warnings[0]
        );
    }

    #[test]
    fn the_winner_of_a_conflict_does_not_depend_on_input_order() {
        let alpha = map_with(
            "team-a",
            "alpha",
            "alpha",
            vec![entry("east", Some("https://shared.example.com"), 10, true)],
        );
        let beta = map_with(
            "team-a",
            "beta",
            "beta",
            vec![entry("east", Some("https://shared.example.com"), 10, true)],
        );
        let forward = project(&[alpha.clone(), beta.clone()]);
        let reverse = project(&[beta, alpha]);
        assert_eq!(forward.homes, reverse.homes);
    }

    #[test]
    fn a_duplicate_cluster_within_one_table_keeps_the_first_and_says_so() {
        let maps = vec![map_with(
            "team-a",
            "llama",
            "llama",
            vec![
                entry("east", Some("https://east-1.example.com"), 70, true),
                entry("east", Some("https://east-2.example.com"), 30, true),
            ],
        )];
        let p = project(&maps);
        assert_eq!(p.homes.len(), 1);
        assert_eq!(p.homes[0].url, "https://east-1.example.com");
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("more than once"));
    }

    #[test]
    fn a_table_naming_no_service_is_skipped_with_an_explanation() {
        let maps = vec![map_with(
            "team-a",
            "anon",
            "",
            vec![entry("east", Some("https://east.example.com"), 10, true)],
        )];
        let p = project(&maps);
        assert!(p.homes.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("names no service"));
    }

    #[test]
    fn an_entry_with_no_cluster_name_is_skipped_with_an_explanation() {
        let maps = vec![map_with(
            "team-a",
            "llama",
            "llama",
            vec![entry("  ", Some("https://east.example.com"), 10, true)],
        )];
        let p = project(&maps);
        assert!(p.homes.is_empty());
        assert_eq!(p.warnings.len(), 1);
        assert!(p.warnings[0].contains("no cluster name"));
    }

    #[test]
    fn a_negative_weight_is_clamped_rather_than_wrapping_to_a_huge_share() {
        let maps = vec![map_with(
            "team-a",
            "llama",
            "llama",
            vec![entry("east", Some("https://east.example.com"), -5, true)],
        )];
        let p = project(&maps);
        assert_eq!(p.homes[0].weight, 0);
    }

    #[test]
    fn several_services_each_keep_their_own_homes() {
        let maps = vec![
            map_with(
                "team-a",
                "alpha",
                "alpha",
                vec![entry(
                    "east",
                    Some("https://alpha-east.example.com"),
                    1,
                    true,
                )],
            ),
            map_with(
                "team-b",
                "beta",
                "beta",
                vec![
                    entry("east", Some("https://beta-east.example.com"), 2, true),
                    entry("west", Some("https://beta-west.example.com"), 3, true),
                ],
            ),
        ];
        let p = project(&maps);
        assert!(p.warnings.is_empty());
        assert_eq!(p.homes.len(), 3);
        let models: Vec<&str> = p.homes.iter().map(|h| h.model_id.as_str()).collect();
        assert_eq!(models, vec!["alpha", "beta", "beta"]);
    }

    #[test]
    fn projection_is_deterministic_across_shuffled_input() {
        let build = |order: [usize; 3]| {
            let all = [
                map_with(
                    "b",
                    "two",
                    "two",
                    vec![entry("w", Some("https://two-w.example.com"), 2, true)],
                ),
                map_with(
                    "a",
                    "one",
                    "one",
                    vec![entry("e", Some("https://one-e.example.com"), 1, true)],
                ),
                map_with(
                    "c",
                    "three",
                    "three",
                    vec![entry("n", Some("https://three-n.example.com"), 3, true)],
                ),
            ];
            let picked: Vec<TrafficMap> = order.iter().map(|i| all[*i].clone()).collect();
            project(&picked).homes
        };
        assert_eq!(build([0, 1, 2]), build([2, 1, 0]));
        assert_eq!(build([0, 1, 2]), build([1, 2, 0]));
    }
}
