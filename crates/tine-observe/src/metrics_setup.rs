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
