use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use tracing::{debug, error, info};

use crate::rest::AppState;

/// WebSocket handler for streaming execution events.
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    info!("WebSocket client connected");

    let mut rx = state.workspace.subscribe_events();

    loop {
        tokio::select! {
            // Forward execution events to the WebSocket client
            Ok(event) = rx.recv() => {
                match serde_json::to_string(&event) {
                    Ok(json) => {
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            debug!("WebSocket client disconnected");
                            break;
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "failed to serialize event");
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
