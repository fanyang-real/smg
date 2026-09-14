//! Reconciling the registered backend set against the published one.
//!
//! Planning the change is separated from making it for the usual reason — the
//! plan is where every judgement call lives and the apply is mechanical — and
//! the plan distinguishes three kinds of change rather than two.
//!
//! Adding a backend and reweighting one are separated because they are
//! different events even though the registry applies both as an upsert. A
//! reweight is the steady-state case: it happens on every placement or quota
//! change, it must be visible as a share movement rather than as backend churn,
//! and it is the case that can eventually skip the registration workflow
//! entirely. Conflating it with an add would bury normal weight drift in
//! registration noise.
//!
//! Removal is separated from both because it is the only destructive one.

use super::projection::DesiredHome;

/// A remote home as the gateway currently holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HomeSync {
    /// The backend key. Matches [`DesiredHome::url`].
    pub url: String,
    /// The share the backend is currently registered with.
    pub weight: u64,
    /// The model id the backend is currently registered under.
    pub model_id: String,
}

/// The change to make, split by cost.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncPlan {
    /// Homes to register. Newly published, or re-keyed onto another model.
    pub add: Vec<DesiredHome>,
    /// Homes already registered whose share moved. Cheap: relabel in place,
    /// keep the connections.
    pub reweight: Vec<DesiredHome>,
    /// Backend keys to drop: no longer published anywhere.
    pub remove: Vec<String>,
}

impl SyncPlan {
    /// Whether applying this plan would change anything.
    pub fn is_empty(&self) -> bool {
        self.add.is_empty() && self.reweight.is_empty() && self.remove.is_empty()
    }
}

/// Diff the published home set against the registered one.
///
/// A home whose model id changed is re-registered rather than relabeled: the
/// model id is what requests match a backend on, so changing it in place would
/// briefly route one model's traffic to another model's backend.
pub fn plan_sync(desired: &[DesiredHome], registered: &[HomeSync]) -> SyncPlan {
    let mut plan = SyncPlan::default();

    for home in desired {
        match registered.iter().find(|r| r.url == home.url) {
            None => plan.add.push(home.clone()),
            Some(current) if current.model_id != home.model_id => plan.add.push(home.clone()),
            Some(current) if current.weight != u64::from(home.weight) => {
                plan.reweight.push(home.clone());
            }
            Some(_) => {}
        }
    }

    for current in registered {
        if !desired.iter().any(|d| d.url == current.url) {
            plan.remove.push(current.url.clone());
        }
    }

    plan.remove.sort();
    plan
}

#[cfg(test)]
mod tests {
    use super::*;

    fn home(url: &str, model: &str, weight: u32) -> DesiredHome {
        DesiredHome {
            url: url.to_string(),
            cluster: "c".to_string(),
            model_id: model.to_string(),
            weight,
            source: "ns/tm".to_string(),
        }
    }

    fn registered(url: &str, model: &str, weight: u64) -> HomeSync {
        HomeSync {
            url: url.to_string(),
            weight,
            model_id: model.to_string(),
        }
    }

    #[test]
    fn an_empty_gateway_registers_every_published_home() {
        let desired = vec![home("https://a", "m", 70), home("https://b", "m", 30)];
        let plan = plan_sync(&desired, &[]);
        assert_eq!(plan.add, desired);
        assert!(plan.reweight.is_empty());
        assert!(plan.remove.is_empty());
    }

    #[test]
    fn a_steady_state_produces_no_work_at_all() {
        let desired = vec![home("https://a", "m", 70), home("https://b", "m", 30)];
        let current = vec![
            registered("https://a", "m", 70),
            registered("https://b", "m", 30),
        ];
        let plan = plan_sync(&desired, &current);
        assert!(plan.is_empty(), "unexpected work: {plan:?}");
    }

    #[test]
    fn a_weight_change_relabels_instead_of_reconnecting() {
        // The whole point of the split: 70 -> 65 must not tear down the backend.
        let desired = vec![home("https://a", "m", 65)];
        let current = vec![registered("https://a", "m", 70)];
        let plan = plan_sync(&desired, &current);
        assert!(plan.add.is_empty(), "a weight change must not re-register");
        assert!(plan.remove.is_empty());
        assert_eq!(plan.reweight, desired);
    }

    #[test]
    fn a_home_dropping_to_zero_is_a_reweight_not_a_removal() {
        // Health-gating a home is a weight change. Removing the backend would
        // make its return cost a cold reconnect.
        let desired = vec![home("https://a", "m", 0)];
        let current = vec![registered("https://a", "m", 70)];
        let plan = plan_sync(&desired, &current);
        assert!(plan.remove.is_empty());
        assert_eq!(plan.reweight.len(), 1);
        assert_eq!(plan.reweight[0].weight, 0);
    }

    #[test]
    fn a_home_that_stops_being_published_is_removed() {
        let desired = vec![home("https://a", "m", 100)];
        let current = vec![
            registered("https://a", "m", 100),
            registered("https://b", "m", 0),
        ];
        let plan = plan_sync(&desired, &current);
        assert!(plan.add.is_empty());
        assert!(plan.reweight.is_empty());
        assert_eq!(plan.remove, vec!["https://b".to_string()]);
    }

    #[test]
    fn a_home_re_keyed_onto_another_model_is_re_registered_not_relabeled() {
        // The model id is the match key for requests; changing it in place
        // would route one model's traffic to another model's backend.
        let desired = vec![home("https://a", "new-model", 100)];
        let current = vec![registered("https://a", "old-model", 100)];
        let plan = plan_sync(&desired, &current);
        assert_eq!(plan.add, desired);
        assert!(plan.reweight.is_empty());
    }

    #[test]
    fn a_model_change_takes_precedence_over_a_simultaneous_weight_change() {
        let desired = vec![home("https://a", "new-model", 42)];
        let current = vec![registered("https://a", "old-model", 100)];
        let plan = plan_sync(&desired, &current);
        assert_eq!(plan.add.len(), 1);
        assert!(plan.reweight.is_empty(), "must not do both to one backend");
    }

    #[test]
    fn losing_every_home_removes_every_backend() {
        let current = vec![
            registered("https://a", "m", 70),
            registered("https://b", "m", 30),
        ];
        let plan = plan_sync(&[], &current);
        assert_eq!(
            plan.remove,
            vec!["https://a".to_string(), "https://b".to_string()]
        );
        assert!(plan.add.is_empty());
    }

    #[test]
    fn a_mixed_change_is_split_into_its_three_kinds() {
        let desired = vec![
            home("https://keep", "m", 10),  // unchanged
            home("https://shift", "m", 55), // reweight
            home("https://fresh", "m", 5),  // add
        ];
        let current = vec![
            registered("https://keep", "m", 10),
            registered("https://shift", "m", 40),
            registered("https://gone", "m", 1),
        ];
        let plan = plan_sync(&desired, &current);
        assert_eq!(plan.add.len(), 1);
        assert_eq!(plan.add[0].url, "https://fresh");
        assert_eq!(plan.reweight.len(), 1);
        assert_eq!(plan.reweight[0].url, "https://shift");
        assert_eq!(plan.remove, vec!["https://gone".to_string()]);
    }

    #[test]
    fn removals_are_ordered_so_the_applied_change_is_reproducible() {
        let current = vec![
            registered("https://c", "m", 1),
            registered("https://a", "m", 1),
            registered("https://b", "m", 1),
        ];
        let plan = plan_sync(&[], &current);
        assert_eq!(
            plan.remove,
            vec![
                "https://a".to_string(),
                "https://b".to_string(),
                "https://c".to_string()
            ]
        );
    }

    #[test]
    fn applying_a_plan_reaches_a_fixed_point() {
        // The second pass over the same input must find nothing left to do,
        // or the gateway would churn its backends on every watch event.
        let desired = vec![home("https://a", "m", 70), home("https://b", "m", 30)];
        let first = plan_sync(&desired, &[]);
        let after: Vec<HomeSync> = first
            .add
            .iter()
            .map(|h| registered(&h.url, &h.model_id, u64::from(h.weight)))
            .collect();
        let second = plan_sync(&desired, &after);
        assert!(
            second.is_empty(),
            "second pass should be a no-op: {second:?}"
        );
    }
}
