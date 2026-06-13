use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use tokio::sync::broadcast::error::RecvError;
use tracing::{debug, error, info, warn};

use crate::rest::AppState;
use tine_core::ExecutionEvent;

/// WebSocket handler for streaming execution events.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

/// Map one broadcast receive result to the outgoing socket message.
///
/// `Ok(Some(text))` — forward to the client; `Ok(None)` — nothing to send
/// (serialization failure, already logged); `Err(())` — channel closed,
/// disconnect.
///
/// A lagged receiver is the load-bearing case: the broadcast channel drops the
/// oldest events for slow consumers, and a silently skipped `NodeCompleted` or
/// `ExecutionCompleted` leaves the client's execution tracking dangling. The
/// client is told explicitly so it can resync tracked executions via the
/// status API.
fn outgoing_message(received: Result<ExecutionEvent, RecvError>) -> Result<Option<String>, ()> {
    match received {
        Ok(event) => match serde_json::to_string(&event) {
            Ok(json) => Ok(Some(json)),
            Err(e) => {
                error!(error = %e, "failed to serialize event");
                Ok(None)
            }
        },
        Err(RecvError::Lagged(skipped)) => {
            warn!(
                skipped,
                "WebSocket event stream lagged; telling client to resync"
            );
            Ok(Some(
                serde_json::json!({
                    "type": "execution_events_lagged",
                    "skipped": skipped,
                })
                .to_string(),
            ))
        }
        Err(RecvError::Closed) => Err(()),
    }
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    info!("WebSocket client connected");

    let mut rx = state.workspace.subscribe_events();

    loop {
        tokio::select! {
            // Forward execution events to the WebSocket client
            received = rx.recv() => {
                match outgoing_message(received) {
                    Ok(Some(json)) => {
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            debug!("WebSocket client disconnected");
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(()) => {
                        debug!("event channel closed; disconnecting WebSocket client");
                        break;
                    }
                }
            }
            // Handle incoming messages from client (e.g., ping/pong, close)
            Some(msg) = async { socket.recv().await } => {
                match msg {
                    Ok(Message::Close(_)) => {
                        debug!("WebSocket client sent close");
                        break;
                    }
                    Ok(Message::Ping(data)) => {
                        let _ = socket.send(Message::Pong(data)).await;
                    }
                    Err(_) => {
                        debug!("WebSocket error, disconnecting");
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::broadcast;

    use super::outgoing_message;
    use tine_core::{ExecutionEvent, ExecutionId, NodeId};

    fn sample_event(label: &str) -> ExecutionEvent {
        ExecutionEvent::NodeStarted {
            execution_id: ExecutionId::new(label),
            node_id: NodeId::new("cell"),
            tree_id: None,
            branch_id: None,
            target_kind: None,
            target: None,
        }
    }

    #[tokio::test]
    async fn lagged_receiver_produces_resync_hint() {
        let (tx, mut rx) = broadcast::channel(1);
        tx.send(sample_event("one")).unwrap();
        tx.send(sample_event("two")).unwrap();
        tx.send(sample_event("three")).unwrap();

        let message = outgoing_message(rx.recv().await)
            .expect("lag must not disconnect the client")
            .expect("lag must produce a message");
        let parsed: serde_json::Value = serde_json::from_str(&message).unwrap();
        assert_eq!(parsed["type"], "execution_events_lagged");
        assert!(parsed["skipped"].as_u64().unwrap() >= 1);

        // The receiver recovers and serves the newest events afterwards.
        let next = outgoing_message(rx.recv().await)
            .expect("recovered receiver must not disconnect")
            .expect("recovered receiver must forward events");
        assert!(next.contains("node_started") || next.contains("NodeStarted"));
    }

    #[tokio::test]
    async fn closed_channel_disconnects_client() {
        let (tx, mut rx) = broadcast::channel::<ExecutionEvent>(1);
        drop(tx);
        assert!(outgoing_message(rx.recv().await).is_err());
    }
}
