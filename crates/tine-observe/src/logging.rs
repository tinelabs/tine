use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Initialize the tracing subscriber with env-filter and JSON or pretty formatting.
///
/// Set `RUST_LOG` env var to control log levels (default: `info`).
/// Set `TINE_LOG_JSON=1` for JSON output (suitable for production).
pub fn init_logging() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let json_logging = std::env::var("TINE_LOG_JSON")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);

    if json_logging {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::layer()
                    .json()
                    .with_target(true)
                    .with_span_list(true)
                    .with_writer(std::io::stderr),
            )
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(
                fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_target(true)
                    .with_thread_ids(false)
                    .with_file(false)
                    .with_line_number(false),
            )
            .init();
    }
}

/// Create a tracing span for a tree execution context.
#[macro_export]
macro_rules! tree_span {
    ($tree_id:expr) => {
        tracing::info_span!("tree", tree_id = %$tree_id)
    };
}

/// Create a tracing span for a cell execution context.
#[macro_export]
macro_rules! cell_span {
    ($tree_id:expr, $cell_id:expr) => {
        tracing::info_span!("cell", tree_id = %$tree_id, cell_id = %$cell_id)
    };
}
