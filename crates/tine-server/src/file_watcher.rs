use std::path::Path;

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::broadcast;
use tracing::{debug, error, info};

use tine_core::ExecutionEvent;

/// Start a file-system watcher on `root` that broadcasts `FileChanged` events.
/// Returns the watcher handle (must be kept alive for the duration of the app).
pub fn start_file_watcher(
    root: &Path,
    event_tx: broadcast::Sender<ExecutionEvent>,
) -> Result<RecommendedWatcher, notify::Error> {
    let root_owned = root.to_path_buf();

    let mut watcher = notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
        match res {
            Ok(event) => {
                let kind_str = match event.kind {
                    EventKind::Create(_) => "create",
                    EventKind::Modify(_) => "modify",
                    EventKind::Remove(_) => "remove",
                    _ => return, // ignore access / other events
                };

                for path in &event.paths {
                    // Make path relative to workspace root
                    let rel = path
                        .strip_prefix(&root_owned)
                        .unwrap_or(path)
                        .to_string_lossy()
                        .to_string();

                    // Skip hidden files and common noise
                    if rel.starts_with('.')
                        || rel.contains("__pycache__")
                        || rel.contains("node_modules")
                        || rel.contains(".tine")
                    {
                        continue;
                    }

                    debug!(path = %rel, kind = %kind_str, "file changed");
                    let _ = event_tx.send(ExecutionEvent::FileChanged {
                        path: rel,
                        kind: kind_str.to_string(),
                    });
                }
            }
            Err(e) => {
                error!(error = %e, "file watcher error");
            }
        }
    })?;

    watcher.watch(root, RecursiveMode::Recursive)?;
    info!(path = %root.display(), "file watcher started");
    Ok(watcher)
}
