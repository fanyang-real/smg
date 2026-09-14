//! Weighted selection over backends that carry an externally-assigned share.
//!
//! Every other policy here decides a share from what the gateway can observe:
//! queue depth, cached prefixes, in-flight counts. This one does the opposite —
//! it takes the share as given and only enforces it. That is the right shape
//! when the split is a *plan* rather than a measurement: a multi-cluster
//! traffic split is computed from placement and quota by a control plane that
//! can see the whole fleet, and a gateway that second-guessed it with local
//! load signals would chase runtime state instead of enforcing the plan.
//!
//! The weight rides on the worker as a label, so nothing about this policy is
//! specific to how the weight was produced.

use std::sync::Arc;

use rand::RngExt;

use super::{get_healthy_worker_indices, LoadBalancingPolicy, SelectWorkerInfo};
use crate::worker::Worker;

/// Worker label carrying the backend's relative traffic share.
///
/// Absent or unparseable means [`DEFAULT_WEIGHT`]: a backend that never had a
/// weight assigned should take an ordinary share, not be starved. Silently
/// reading a missing weight as zero would turn a labeling mistake into a
/// black-holed backend.
pub const ROUTING_WEIGHT_LABEL: &str = "routing_weight";

/// Share given to a backend that carries no usable weight label.
pub const DEFAULT_WEIGHT: u64 = 1;

/// Selects among available backends in proportion to their assigned weights.
#[derive(Debug, Default)]
pub struct WeightedPolicy;

impl WeightedPolicy {
    pub fn new() -> Self {
        Self
    }
}

/// Read a worker's assigned share.
pub fn worker_weight(worker: &dyn Worker) -> u64 {
    worker
        .metadata()
        .spec
        .labels
        .get(ROUTING_WEIGHT_LABEL)
        .map(|raw| raw.trim())
        .filter(|raw| !raw.is_empty())
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(DEFAULT_WEIGHT)
}

/// Choose a position in `weights` for a draw uniform over `[0, total)`.
///
/// Split out from the policy so the distribution is testable without a random
/// source: given the draw, the answer is fully determined.
///
/// An all-zero set spreads evenly rather than selecting nothing. Weights are a
/// statement about *relative* share, so a set that assigns none carries no
/// preference to honor — and refusing to choose would drop live traffic to
/// enforce a preference that was never expressed.
pub fn pick_by_weight(weights: &[u64], draw: u64) -> Option<usize> {
    if weights.is_empty() {
        return None;
    }
    let total: u64 = weights.iter().copied().sum();
    if total == 0 {
        let len = weights.len() as u64;
        return Some((draw % len) as usize);
    }
    let target = draw % total;
    let mut cumulative = 0u64;
    for (idx, weight) in weights.iter().enumerate() {
        cumulative += *weight;
        if target < cumulative {
            return Some(idx);
        }
    }
    // Unreachable while `target < total`, but returning the last positively
    // weighted backend is the correct answer if it ever were reached, and it
    // keeps this function total.
    weights.iter().rposition(|w| *w > 0)
}

impl LoadBalancingPolicy for WeightedPolicy {
    fn select_worker(
        &self,
        workers: &[Arc<dyn Worker>],
        _info: &SelectWorkerInfo,
    ) -> Option<usize> {
        let available = get_healthy_worker_indices(workers);
        if available.is_empty() {
            return None;
        }
        let weights: Vec<u64> = available
            .iter()
            .map(|idx| worker_weight(workers[*idx].as_ref()))
            .collect();
        let draw = rand::rng().random::<u64>();
        pick_by_weight(&weights, draw).map(|pos| available[pos])
    }

    fn name(&self) -> &'static str {
        "weighted"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use openai_protocol::worker::{HealthCheckConfig, WorkerStatus};

    use super::*;
    use crate::worker::{BasicWorkerBuilder, WorkerType};

    fn no_health_check() -> HealthCheckConfig {
        HealthCheckConfig {
            disable_health_check: true,
            ..Default::default()
        }
    }

    fn worker(url: &str, weight: Option<&str>) -> Arc<dyn Worker> {
        let mut labels = HashMap::new();
        if let Some(w) = weight {
            labels.insert(ROUTING_WEIGHT_LABEL.to_string(), w.to_string());
        }
        Arc::new(
            BasicWorkerBuilder::new(url)
                .worker_type(WorkerType::Regular)
                .health_config(no_health_check())
                .labels(labels)
                .build(),
        )
    }

    // ── the distribution itself ──────────────────────────────────────────

    #[test]
    fn every_draw_lands_in_the_band_its_weight_claims() {
        // 70/30 over draws 0..100: the first 70 go to index 0, the rest to 1.
        let weights = [70, 30];
        for draw in 0..70u64 {
            assert_eq!(pick_by_weight(&weights, draw), Some(0), "draw {draw}");
        }
        for draw in 70..100u64 {
            assert_eq!(pick_by_weight(&weights, draw), Some(1), "draw {draw}");
        }
    }

    #[test]
    fn the_draw_wraps_so_any_random_value_is_usable() {
        let weights = [70, 30];
        assert_eq!(pick_by_weight(&weights, 100), Some(0));
        assert_eq!(pick_by_weight(&weights, 170), Some(1));
        assert_eq!(
            pick_by_weight(&weights, u64::MAX),
            pick_by_weight(&weights, u64::MAX % 100)
        );
    }

    #[test]
    fn a_zero_weighted_backend_is_never_chosen() {
        let weights = [0, 5, 0];
        for draw in 0..50u64 {
            assert_eq!(pick_by_weight(&weights, draw), Some(1), "draw {draw}");
        }
    }

    #[test]
    fn an_all_zero_set_spreads_evenly_rather_than_dropping_traffic() {
        let weights = [0, 0, 0];
        let picks: Vec<Option<usize>> = (0..6).map(|d| pick_by_weight(&weights, d)).collect();
        assert_eq!(
            picks,
            vec![Some(0), Some(1), Some(2), Some(0), Some(1), Some(2)]
        );
    }

    #[test]
    fn no_backends_means_no_choice() {
        assert_eq!(pick_by_weight(&[], 0), None);
        assert_eq!(pick_by_weight(&[], 12345), None);
    }

    #[test]
    fn a_single_backend_always_wins_whatever_its_weight() {
        assert_eq!(pick_by_weight(&[1], 999), Some(0));
        assert_eq!(pick_by_weight(&[0], 999), Some(0));
        assert_eq!(pick_by_weight(&[u64::from(u32::MAX)], 7), Some(0));
    }

    #[test]
    fn the_observed_split_tracks_the_assigned_split() {
        let weights = [70, 20, 10];
        let mut counts = [0usize; 3];
        for draw in 0..1000u64 {
            let idx = pick_by_weight(&weights, draw).expect("a backend is chosen");
            counts[idx] += 1;
        }
        // Draws sweep whole periods of the total, so the split is exact.
        assert_eq!(counts, [700, 200, 100]);
    }

    #[test]
    fn large_weights_do_not_overflow_the_running_total() {
        let big = u64::from(u32::MAX);
        let weights = [big, big, big];
        assert!(pick_by_weight(&weights, u64::MAX).is_some());
    }

    // ── reading the weight off a worker ──────────────────────────────────

    #[test]
    fn an_unlabeled_backend_takes_an_ordinary_share_not_a_starved_one() {
        let w = worker("http://a:8000", None);
        assert_eq!(worker_weight(w.as_ref()), DEFAULT_WEIGHT);
    }

    #[test]
    fn an_unparseable_weight_falls_back_rather_than_starving_the_backend() {
        for bad in ["", "   ", "abc", "-5", "1.5", "99999999999999999999999"] {
            let w = worker("http://a:8000", Some(bad));
            assert_eq!(
                worker_weight(w.as_ref()),
                DEFAULT_WEIGHT,
                "weight label {bad:?} should fall back"
            );
        }
    }

    #[test]
    fn a_labeled_weight_is_read_verbatim() {
        let w = worker("http://a:8000", Some(" 42 "));
        assert_eq!(worker_weight(w.as_ref()), 42);
        let zero = worker("http://b:8000", Some("0"));
        assert_eq!(worker_weight(zero.as_ref()), 0);
    }

    // ── the policy end to end ────────────────────────────────────────────

    #[test]
    fn selection_only_ever_returns_an_available_backend() {
        let policy = WeightedPolicy::new();
        let workers = vec![
            worker("http://a:8000", Some("70")),
            worker("http://b:8000", Some("30")),
        ];
        for _ in 0..200 {
            let idx = policy
                .select_worker(&workers, &SelectWorkerInfo::default())
                .expect("a backend is chosen");
            assert!(idx < workers.len());
        }
    }

    #[test]
    fn an_unavailable_backend_is_skipped_even_when_it_carries_all_the_weight() {
        let policy = WeightedPolicy::new();
        let workers = vec![
            worker("http://a:8000", Some("1000")),
            worker("http://b:8000", Some("1")),
        ];
        workers[0].set_status(WorkerStatus::NotReady);
        for _ in 0..50 {
            assert_eq!(
                policy.select_worker(&workers, &SelectWorkerInfo::default()),
                Some(1)
            );
        }
    }

    #[test]
    fn no_available_backend_means_no_selection() {
        let policy = WeightedPolicy::new();
        let workers = vec![worker("http://a:8000", Some("5"))];
        workers[0].set_status(WorkerStatus::NotReady);
        assert_eq!(
            policy.select_worker(&workers, &SelectWorkerInfo::default()),
            None
        );
    }

    #[test]
    fn a_zero_weighted_backend_takes_no_traffic_while_a_weighted_peer_is_up() {
        let policy = WeightedPolicy::new();
        let workers = vec![
            worker("http://drained:8000", Some("0")),
            worker("http://live:8000", Some("100")),
        ];
        for _ in 0..200 {
            assert_eq!(
                policy.select_worker(&workers, &SelectWorkerInfo::default()),
                Some(1)
            );
        }
    }

    #[test]
    fn a_zero_weighted_backend_still_serves_when_it_is_the_only_one_left() {
        // The alternative is refusing to serve a live backend because a stale
        // plan says zero, which turns a weighting bug into an outage.
        let policy = WeightedPolicy::new();
        let workers = vec![
            worker("http://drained:8000", Some("0")),
            worker("http://live:8000", Some("100")),
        ];
        workers[1].set_status(WorkerStatus::NotReady);
        assert_eq!(
            policy.select_worker(&workers, &SelectWorkerInfo::default()),
            Some(0)
        );
    }

    #[test]
    fn the_policy_reports_its_name() {
        assert_eq!(WeightedPolicy::new().name(), "weighted");
    }

    #[test]
    fn both_backends_are_exercised_over_many_selections() {
        let policy = WeightedPolicy::new();
        let workers = vec![
            worker("http://a:8000", Some("50")),
            worker("http://b:8000", Some("50")),
        ];
        let mut seen = HashMap::new();
        for _ in 0..500 {
            if let Some(idx) = policy.select_worker(&workers, &SelectWorkerInfo::default()) {
                *seen.entry(idx).or_insert(0usize) += 1;
            }
        }
        assert_eq!(seen.len(), 2, "both backends should be selected: {seen:?}");
    }
}
