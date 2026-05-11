use std::time::Instant;

use metrics_exporter_prometheus::PrometheusBuilder;

/// Install the Prometheus metrics recorder.
///
/// After calling this, all `metrics::counter!`, `metrics::gauge!`, `metrics::histogram!`
/// calls will be captured and exposed via the returned handle.
///
/// Returns a `PrometheusHandle` that can render the `/metrics` output.
pub fn init_metrics() -> metrics_exporter_prometheus::PrometheusHandle {
    let builder = PrometheusBuilder::new();
    builder
        .install_recorder()
        .expect("failed to install Prometheus recorder")
}

// Metric names as constants for consistency.
pub const METRIC_NODES_EXECUTED: &str = "tine_nodes_executed_total";
pub const METRIC_NODES_CACHE_HIT: &str = "tine_nodes_cache_hit_total";
pub const METRIC_NODES_FAILED: &str = "tine_nodes_failed_total";
pub const METRIC_PIPELINES_EXECUTED: &str = "tine_pipelines_executed_total";
pub const METRIC_KERNEL_STARTUPS: &str = "tine_kernel_startups_total";
pub const METRIC_KERNEL_ACTIVE: &str = "tine_kernels_active";
pub const METRIC_EXECUTION_DURATION: &str = "tine_execution_duration_seconds";
pub const METRIC_ARTIFACT_SIZE: &str = "tine_artifact_size_bytes";

// --- Kernel runtime performance histograms (WI-1) ------------------------
//
// All values are recorded in seconds (f64). Labels are enums with bounded
// cardinality — do NOT label by tree_id, owner_id, or any unbounded string.

// Environment acquisition
pub const METRIC_ENV_ENSURE_TOTAL: &str = "tine_ensure_tree_environment_total_seconds";
pub const METRIC_ENV_ENSURE_LOCK_WAIT: &str = "tine_ensure_tree_environment_lock_wait_seconds";
pub const METRIC_ENV_ENSURE_PIP_CHECK: &str = "tine_ensure_tree_environment_pip_check_seconds";
pub const METRIC_ENV_ENSURE_SYNC: &str = "tine_ensure_tree_environment_sync_seconds";
pub const METRIC_ENV_ENSURE_PREFLIGHT: &str = "tine_ensure_tree_environment_preflight_seconds";

// Kernel startup
pub const METRIC_KERNEL_START_TOTAL: &str = "tine_kernel_start_total_seconds";
pub const METRIC_KERNEL_START_SPAWN: &str = "tine_kernel_start_spawn_seconds";
pub const METRIC_KERNEL_START_HEARTBEAT_CONNECT: &str =
    "tine_kernel_start_heartbeat_connect_seconds";
pub const METRIC_KERNEL_START_HEARTBEAT_READY: &str = "tine_kernel_start_heartbeat_ready_seconds";
pub const METRIC_KERNEL_START_CHANNEL_CONNECT: &str = "tine_kernel_start_channel_connect_seconds";
pub const METRIC_KERNEL_START_SETUP_CODE: &str = "tine_kernel_start_setup_code_seconds";

// Context preparation
pub const METRIC_PREPARE_CONTEXT_TOTAL: &str = "tine_prepare_context_total_seconds";
pub const METRIC_PREPARE_CONTEXT_REPLAY_CELLS: &str = "tine_prepare_context_replay_cells";
pub const METRIC_PREPARE_CONTEXT_REPLAY: &str = "tine_prepare_context_replay_seconds";

// Per-cell execution
pub const METRIC_KERNEL_EXECUTE_TOTAL: &str = "tine_kernel_execute_total_seconds";
pub const METRIC_KERNEL_EXECUTE_IOPUB_WAIT: &str = "tine_kernel_execute_iopub_wait_seconds";
pub const METRIC_KERNEL_EXECUTE_SHELL_REPLY: &str = "tine_kernel_execute_shell_reply_seconds";

// Artifact persistence
pub const METRIC_ARTIFACT_PERSIST_TOTAL: &str = "tine_artifact_persist_total_seconds";
pub const METRIC_ARTIFACT_PERSIST_SLOT_COUNT: &str = "tine_artifact_persist_slot_count";

/// Records elapsed time on drop with an `outcome` label, so failure paths
/// (early `?` returns, panics during unwind, etc.) still emit a sample.
///
/// Default outcome is `"error"`; call `set_outcome("success")` (or any other
/// bounded-cardinality string) before the guard goes out of scope on the happy
/// path.
pub struct OutcomeTimer {
    name: &'static str,
    start: Instant,
    outcome: &'static str,
}

impl OutcomeTimer {
    pub fn start(name: &'static str) -> Self {
        Self {
            name,
            start: Instant::now(),
            outcome: "error",
        }
    }

    pub fn set_outcome(&mut self, outcome: &'static str) {
        self.outcome = outcome;
    }
}

impl Drop for OutcomeTimer {
    fn drop(&mut self) {
        metrics::histogram!(self.name, "outcome" => self.outcome)
            .record(self.start.elapsed().as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics_exporter_prometheus::PrometheusHandle;
    use std::sync::OnceLock;

    fn handle() -> &'static PrometheusHandle {
        static H: OnceLock<PrometheusHandle> = OnceLock::new();
        H.get_or_init(init_metrics)
    }

    #[test]
    fn outcome_timer_records_success_when_set() {
        let h = handle();
        {
            let mut t = OutcomeTimer::start("test_outcome_timer_success_seconds");
            t.set_outcome("success");
        }
        let r = h.render();
        assert!(
            r.contains("test_outcome_timer_success_seconds_count")
                && r.contains("outcome=\"success\""),
            "expected success sample, got:\n{}",
            r
        );
    }

    #[test]
    fn outcome_timer_records_error_when_dropped_without_set() {
        let h = handle();
        {
            let _t = OutcomeTimer::start("test_outcome_timer_error_seconds");
            // dropped without calling set_outcome
        }
        let r = h.render();
        assert!(
            r.contains("test_outcome_timer_error_seconds_count") && r.contains("outcome=\"error\""),
            "expected error sample, got:\n{}",
            r
        );
    }

    #[test]
    fn outcome_timer_records_error_on_early_return() {
        let h = handle();
        fn run(fail: bool) -> Result<(), &'static str> {
            let mut t = OutcomeTimer::start("test_outcome_timer_early_return_seconds");
            if fail {
                return Err("boom");
            }
            t.set_outcome("success");
            Ok(())
        }
        assert!(run(true).is_err());
        assert!(run(false).is_ok());
        let r = h.render();
        assert!(
            r.contains("outcome=\"error\"") && r.contains("outcome=\"success\""),
            "expected both outcomes, got:\n{}",
            r
        );
    }
}
