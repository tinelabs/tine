// End-to-end integration tests for the Tine workspace API.
//
// These tests exercise the full WorkspaceApi lifecycle:
//   save_experiment_tree → create_branch → execute_branch → logs
//
// Tests that require a live Jupyter kernel (ipykernel + Python) are gated
// behind `#[ignore]` — run them with `cargo test -- --ignored`.

use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use serial_test::serial;
use tempfile::TempDir;
use tokio::time::{timeout, Duration};

use tine_api::Workspace;
use tine_core::{
    ArtifactKey, ArtifactMetadata, ArtifactStore, BranchDef, BranchId, BranchIsolationMode,
    CellDef, CellId, CellRuntimeState, ExecutionEvent, ExecutionMode, ExperimentTreeDef,
    ExperimentTreeId, NodeCode, NodeId, NodeStatus, ProjectDef, ProjectId, SlotName, TineError,
    TineResult, TreeKernelState, WorkspaceApi,
};

// ---------------------------------------------------------------------------
// In-memory ArtifactStore for tests
// ---------------------------------------------------------------------------

struct MemoryArtifactStore {
    data: DashMap<String, Vec<u8>>,
}

impl MemoryArtifactStore {
    fn new() -> Self {
        Self {
            data: DashMap::new(),
        }
    }
}

#[async_trait]
impl ArtifactStore for MemoryArtifactStore {
    async fn put(&self, key: &ArtifactKey, data: &[u8]) -> TineResult<[u8; 32]> {
        let hash = *blake3::hash(data).as_bytes();
        self.data.insert(key.as_str().to_string(), data.to_vec());
        Ok(hash)
    }

    async fn get(&self, key: &ArtifactKey) -> TineResult<Vec<u8>> {
        self.data
            .get(key.as_str())
            .map(|v| v.value().clone())
            .ok_or_else(|| tine_core::TineError::ArtifactNotFound(key.clone()))
    }

    async fn delete(&self, key: &ArtifactKey) -> TineResult<()> {
        self.data.remove(key.as_str());
        Ok(())
    }

    async fn exists(&self, key: &ArtifactKey) -> TineResult<bool> {
        Ok(self.data.contains_key(key.as_str()))
    }

    async fn metadata(&self, key: &ArtifactKey) -> TineResult<ArtifactMetadata> {
        let data = self.get(key).await?;
        Ok(ArtifactMetadata {
            key: key.clone(),
            size_bytes: data.len() as u64,
            schema: None,
            created_at: Utc::now(),
            content_hash: *blake3::hash(&data).as_bytes(),
        })
    }

    async fn list(&self) -> TineResult<Vec<ArtifactKey>> {
        Ok(self
            .data
            .iter()
            .map(|entry| ArtifactKey::new(entry.key().clone()))
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_save_and_load_experiment_tree() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree = trivial_tree();

    let id = ws.save_experiment_tree(&tree).await.unwrap().id;
    let loaded = ws.get_experiment_tree(&id).await.unwrap();

    assert_eq!(loaded.id, tree.id);
    assert_eq!(loaded.name, tree.name);
    assert_eq!(loaded.root_branch_id, tree.root_branch_id);
    assert_eq!(loaded.branches.len(), 1);
    assert_eq!(loaded.cells.len(), tree.cells.len());
}

#[tokio::test]
#[serial]
async fn test_list_experiment_trees_returns_saved_entries() {
    let (_tmp, ws) = open_temp_workspace().await;

    let tree1 = trivial_tree();
    ws.save_experiment_tree(&tree1).await.unwrap();

    let mut tree2 = trivial_tree();
    tree2.id = ExperimentTreeId::new("trivial-2");
    tree2.name = "trivial-2".to_string();
    ws.save_experiment_tree(&tree2).await.unwrap();

    let trees = ws.list_experiment_trees().await.unwrap();

    assert!(trees.iter().any(|tree| tree.id == tree1.id));
    assert!(trees.iter().any(|tree| tree.id == tree2.id));
}

#[tokio::test]
#[serial]
async fn test_runtime_branch_materializations_are_hidden_from_experiment_lists() {
    let (_tmp, ws) = open_temp_workspace().await;
    let project = ProjectDef {
        id: ProjectId::new("project-runtime-hidden"),
        name: "Runtime hidden".to_string(),
        description: None,
        workspace_dir: ".".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let project_id = ws.create_project(project).await.unwrap();

    let mut tree = two_cell_tree();
    tree.id = ExperimentTreeId::new("runtime-hidden");
    tree.name = "runtime-hidden".to_string();
    tree.project_id = Some(project_id.clone());
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &tine_core::BranchId::new("main"),
            "runtime-branch".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_step"),
                tree_id: tree_id.clone(),
                branch_id: tine_core::BranchId::new("ignored"),
                name: "Branch Step".to_string(),
                code: NodeCode {
                    source: "branch_value = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_value")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let exec_id = ws
        .execute_branch_in_experiment_tree(&tree_id, &branch_id)
        .await
        .unwrap();
    wait_for_execution_finished(&ws, &exec_id).await;

    let experiments = ws.list_experiments(&project_id).await.unwrap();
    assert_eq!(experiments.len(), 1);
    assert_eq!(experiments[0].id.as_str(), tree_id.as_str());

    let trees = ws.list_experiment_trees().await.unwrap();
    let matching: Vec<_> = trees
        .into_iter()
        .filter(|tree| {
            tree.id == tree_id
                || tree
                    .id
                    .as_str()
                    .starts_with(&format!("{}__", tree_id.as_str()))
        })
        .collect();
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].id, tree_id);
}

#[tokio::test]
#[serial]
async fn test_create_branch_in_experiment_tree() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree = two_cell_tree();
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &tree.root_branch_id,
            "alt-path".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_step"),
                tree_id: tree_id.clone(),
                branch_id: tree.root_branch_id.clone(),
                name: "Branch step".to_string(),
                code: NodeCode {
                    source: "branch_step = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_step")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    assert_eq!(loaded.branches.len(), 2);
    let branch = loaded
        .branches
        .iter()
        .find(|branch| branch.id == branch_id)
        .unwrap();
    assert_eq!(branch.name, "alt-path");
    assert_eq!(
        branch.parent_branch_id.as_ref().unwrap(),
        &tree.root_branch_id
    );
    assert_eq!(
        branch.branch_point_cell_id.as_ref().unwrap().as_str(),
        "step2"
    );
    assert_eq!(branch.cell_order.len(), 1);
    assert_eq!(branch.cell_order[0].as_str(), "branch_step");
    assert!(loaded
        .cells
        .iter()
        .any(|cell| cell.id.as_str() == "branch_step"));
}

#[test]
fn test_descendant_cell_ids_compat() {
    let mut tree = two_cell_tree();
    let root_branch = tree.root_branch_id.clone();

    tree.branches.push(tine_core::BranchDef {
        id: tine_core::BranchId::new("alt"),
        name: "alt".to_string(),
        parent_branch_id: Some(root_branch.clone()),
        branch_point_cell_id: Some(CellId::new("step2")),
        cell_order: vec![CellId::new("branch_step")],
        display: HashMap::new(),
    });
    tree.cells.push(CellDef {
        id: CellId::new("branch_step"),
        tree_id: tree.id.clone(),
        branch_id: tine_core::BranchId::new("alt"),
        name: "Branch step".to_string(),
        code: NodeCode {
            source: "branch_step = step2 + 1".to_string(),
            language: "python".to_string(),
        },
        upstream_cell_ids: vec![CellId::new("step2")],
        declared_outputs: vec![SlotName::new("branch_step")],
        cache: false,
        map_over: None,
        map_concurrency: None,
        timeout_secs: None,
        tags: HashMap::new(),
        revision_id: None,
        state: CellRuntimeState::Clean,
    });

    let descendants = Workspace::descendant_cell_ids_compat(&tree, &CellId::new("step1"));
    let descendant_names: std::collections::HashSet<_> =
        descendants.iter().map(|id| id.as_str()).collect();
    assert!(descendant_names.contains("step2"));
    assert!(descendant_names.contains("branch_step"));
}

#[tokio::test]
#[serial]
async fn test_mark_stale_descendants_compat() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree = two_cell_tree();
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;

    ws.create_branch_in_experiment_tree(
        &tree_id,
        &tree.root_branch_id,
        "alt-path".to_string(),
        &CellId::new("step2"),
        CellDef {
            id: CellId::new("branch_step"),
            tree_id: tree_id.clone(),
            branch_id: tree.root_branch_id.clone(),
            name: "Branch step".to_string(),
            code: NodeCode {
                source: "branch_step = step2 + 1".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![CellId::new("step2")],
            declared_outputs: vec![SlotName::new("branch_step")],
            cache: false,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        },
    )
    .await
    .unwrap();

    let stale = ws
        .mark_stale_descendants_compat(&tree_id, &CellId::new("step1"))
        .await
        .unwrap();
    let stale_names: std::collections::HashSet<_> = stale.iter().map(|id| id.as_str()).collect();
    assert!(stale_names.contains("step2"));
    assert!(stale_names.contains("branch_step"));

    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    let states: HashMap<_, _> = loaded
        .cells
        .iter()
        .map(|cell| (cell.id.as_str().to_string(), cell.state.clone()))
        .collect();
    assert_eq!(states.get("step1"), Some(&CellRuntimeState::Clean));
    assert_eq!(states.get("step2"), Some(&CellRuntimeState::Stale));
    assert_eq!(states.get("branch_step"), Some(&CellRuntimeState::Stale));
}

/// Open a fresh workspace backed by a temp directory.
async fn open_temp_workspace() -> (TempDir, Workspace) {
    let tmp = TempDir::new().expect("failed to create temp dir");
    eprintln!("[e2e] opening temp workspace at {}", tmp.path().display());
    let store: Arc<dyn ArtifactStore> = Arc::new(MemoryArtifactStore::new());
    let ws = Workspace::open(tmp.path().to_path_buf(), store, 4)
        .await
        .expect("failed to open workspace");
    (tmp, ws)
}

async fn wait_for_execution_finished(
    ws: &Workspace,
    exec_id: &tine_core::ExecutionId,
) -> tine_core::ExecutionStatus {
    for attempt in 0..480 {
        let status = ws.status(exec_id).await.unwrap();
        if status.finished_at.is_some() {
            eprintln!(
                "[e2e] execution {} finished on poll {} tree={:?} branch={:?} states={:?}",
                exec_id.as_str(),
                attempt,
                status.tree_id,
                status.branch_id,
                status.node_statuses
            );
            return status;
        }
        if attempt == 0 || attempt % 10 == 0 {
            eprintln!(
                "[e2e] waiting for execution {} poll={} tree={:?} branch={:?} states={:?}",
                exec_id.as_str(),
                attempt,
                status.tree_id,
                status.branch_id,
                status.node_statuses
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    let final_status = ws.status(exec_id).await.unwrap();
    panic!(
        "execution {} did not finish in time; tree={:?} branch={:?} states={:?}",
        exec_id.as_str(),
        final_status.tree_id,
        final_status.branch_id,
        final_status.node_statuses
    );
}

async fn wait_for_node_running(
    ws: &Workspace,
    exec_id: &tine_core::ExecutionId,
    node_id: &NodeId,
) -> tine_core::ExecutionStatus {
    for attempt in 0..240 {
        let status = ws.status(exec_id).await.unwrap();
        if matches!(status.node_statuses.get(node_id), Some(NodeStatus::Running)) {
            return status;
        }
        if status.finished_at.is_some() {
            panic!(
                "execution {} finished before node {} reached running; states={:?}",
                exec_id.as_str(),
                node_id.as_str(),
                status.node_statuses
            );
        }
        if attempt == 0 || attempt % 10 == 0 {
            eprintln!(
                "[e2e] waiting for node {} in execution {} to enter running poll={} states={:?}",
                node_id.as_str(),
                exec_id.as_str(),
                attempt,
                status.node_statuses
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    panic!(
        "node {} in execution {} did not reach running state in time",
        node_id.as_str(),
        exec_id.as_str()
    );
}
fn trivial_tree() -> ExperimentTreeDef {
    let tree_id = ExperimentTreeId::new("trivial");
    let branch_id = BranchId::new("main");
    let cell_id = CellId::new("step1");
    ExperimentTreeDef {
        id: tree_id.clone(),
        name: "trivial".to_string(),
        project_id: None,
        root_branch_id: branch_id.clone(),
        branches: vec![BranchDef {
            id: branch_id.clone(),
            name: "main".to_string(),
            parent_branch_id: None,
            branch_point_cell_id: None,
            cell_order: vec![cell_id.clone()],
            display: HashMap::new(),
        }],
        cells: vec![CellDef {
            id: cell_id.clone(),
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            name: "step1".to_string(),
            code: NodeCode {
                source: "step1 = 42\n".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![],
            declared_outputs: vec![SlotName::new("step1")],
            cache: false,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        }],
        environment: Default::default(),
        execution_mode: ExecutionMode::Parallel,
        budget: None,
        created_at: chrono::Utc::now(),
    }
}

fn two_cell_tree() -> ExperimentTreeDef {
    let tree_id = ExperimentTreeId::new("trivial");
    let branch_id = BranchId::new("main");
    let cell_id1 = CellId::new("step1");
    let cell_id2 = CellId::new("step2");
    ExperimentTreeDef {
        id: tree_id.clone(),
        name: "trivial".to_string(),
        project_id: None,
        root_branch_id: branch_id.clone(),
        branches: vec![BranchDef {
            id: branch_id.clone(),
            name: "main".to_string(),
            parent_branch_id: None,
            branch_point_cell_id: None,
            cell_order: vec![cell_id1.clone(), cell_id2.clone()],
            display: HashMap::new(),
        }],
        cells: vec![
            CellDef {
                id: cell_id1.clone(),
                tree_id: tree_id.clone(),
                branch_id: branch_id.clone(),
                name: "step1".to_string(),
                code: NodeCode {
                    source: "step1 = 42\n".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![],
                declared_outputs: vec![SlotName::new("step1")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
            CellDef {
                id: cell_id2.clone(),
                tree_id: tree_id.clone(),
                branch_id: branch_id.clone(),
                name: "step2".to_string(),
                code: NodeCode {
                    source: "step2 = step1 * 2\n".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![cell_id1.clone()],
                declared_outputs: vec![SlotName::new("step2")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        ],
        environment: Default::default(),
        execution_mode: ExecutionMode::Parallel,
        budget: None,
        created_at: chrono::Utc::now(),
    }
}

// ===========================================================================
// T E S T S
// ===========================================================================

// ---------------------------------------------------------------------------
// 7. Status + logs for non-existent execution
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_status_nonexistent_execution() {
    let (_tmp, ws) = open_temp_workspace().await;
    let fake_id = tine_core::ExecutionId::new("nonexistent-exec");
    let result = ws.status(&fake_id).await;
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// 8. Full execution with real kernel (requires ipykernel)
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
#[ignore]
async fn test_execute_branch_path_persists_target_metadata_and_tree_logs() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree = two_cell_tree();
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &tree.root_branch_id,
            "branch-run".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_cell_1"),
                tree_id: tree_id.clone(),
                branch_id: tree.root_branch_id.clone(),
                name: "Branch cell 1".to_string(),
                code: NodeCode {
                    source: "print(step2 + 1)\nbranch_value = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_value")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let exec_id = ws
        .execute_branch_in_experiment_tree(&tree_id, &branch_id)
        .await
        .unwrap();
    let status = wait_for_execution_finished(&ws, &exec_id).await;

    assert_eq!(status.tree_id.as_ref(), Some(&tree_id));
    assert_eq!(status.branch_id.as_ref(), Some(&branch_id));
    assert_eq!(
        status.target_kind,
        Some(tine_core::ExecutionTargetKind::ExperimentTreeBranch)
    );
    assert_eq!(
        status.node_statuses.get(&NodeId::new("branch_cell_1")),
        Some(&NodeStatus::Completed)
    );

    let logs = ws
        .logs_for_tree_cell(&tree_id, &branch_id, &CellId::new("branch_cell_1"))
        .await
        .unwrap();
    assert!(
        logs.stdout.contains("85"),
        "expected branch logs fetched by tree id to include branch stdout, got {:?}",
        logs.stdout
    );
}

// ---------------------------------------------------------------------------
// 9. Event subscription
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_event_subscription_does_not_panic() {
    let (_tmp, ws) = open_temp_workspace().await;
    // Just verify we can subscribe without panicking
    let _rx = ws.subscribe_events();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_namespace_guarded_run_all_emits_success_events() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &root_branch_id,
            "branch-events".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_events_cell"),
                tree_id: tree_id.clone(),
                branch_id: tine_core::BranchId::new("ignored"),
                name: "Branch events cell".to_string(),
                code: NodeCode {
                    source: "branch_events = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_events")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let mut state = ws
        .get_tree_runtime_state(&tree_id)
        .await
        .unwrap_or_else(|| tine_core::TreeRuntimeState {
            tree_id: tree_id.clone(),
            active_branch_id: root_branch_id.clone(),
            materialized_path_cell_ids: Vec::new(),
            runtime_epoch: 0,
            kernel_state: TreeKernelState::NeedsReplay,
            last_prepared_cell_id: None,
            isolation_mode: BranchIsolationMode::Disabled,
            last_isolation_result: None,
        });
    state.isolation_mode = BranchIsolationMode::NamespaceGuarded;
    ws.set_tree_runtime_state(state).await.unwrap();

    let mut rx = ws.subscribe_events();
    let event_tree_id = tree_id.clone();
    let expected_branches: HashSet<_> = [root_branch_id.clone(), branch_id.clone()]
        .into_iter()
        .collect();
    let expected_branches_for_task = expected_branches.clone();
    let event_task = tokio::spawn(async move {
        let mut attempted = HashSet::new();
        let mut resolved = HashSet::new();
        loop {
            let event = rx.recv().await.expect("event channel closed");
            match event {
                ExecutionEvent::IsolationAttempted {
                    tree_id: evt_tree,
                    branch_id,
                    ..
                } if evt_tree == event_tree_id => {
                    attempted.insert(branch_id);
                }
                ExecutionEvent::IsolationSucceeded {
                    tree_id: evt_tree,
                    branch_id,
                    ..
                } if evt_tree == event_tree_id => {
                    resolved.insert(branch_id);
                }
                ExecutionEvent::FallbackRestartTriggered {
                    tree_id: evt_tree,
                    branch_id,
                    ..
                } if evt_tree == event_tree_id => {
                    resolved.insert(branch_id);
                }
                _ => {}
            }
            if attempted == expected_branches_for_task && resolved == expected_branches_for_task {
                break;
            }
        }
        (attempted, resolved)
    });
    ws.execute_all_branches_in_experiment_tree(&tree_id)
        .await
        .unwrap();

    let (attempted, resolved) = timeout(Duration::from_secs(30), event_task)
        .await
        .expect("timed out waiting for event listener task")
        .expect("event listener task panicked");

    assert_eq!(attempted, expected_branches);
    assert_eq!(resolved, expected_branches);

    let runtime_state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    let isolation_result = runtime_state
        .last_isolation_result
        .as_ref()
        .expect("expected isolation result to be recorded");
    assert_eq!(runtime_state.kernel_state, TreeKernelState::NeedsReplay);
    assert!(runtime_state.materialized_path_cell_ids.is_empty());
    assert_eq!(runtime_state.last_prepared_cell_id, None);
    assert!(!isolation_result.succeeded);
    assert!(
        isolation_result
            .contamination_signals
            .iter()
            .any(|signal| signal == "session_overlap"),
        "expected guarded overlap to be recorded as contamination, got {:?}",
        isolation_result.contamination_signals
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_prepare_context_reuses_guarded_baseline_without_bumping_epoch() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    let mut state = ws
        .get_tree_runtime_state(&tree_id)
        .await
        .unwrap_or_else(|| tine_core::TreeRuntimeState {
            tree_id: tree_id.clone(),
            active_branch_id: root_branch_id.clone(),
            materialized_path_cell_ids: Vec::new(),
            runtime_epoch: 0,
            kernel_state: TreeKernelState::NeedsReplay,
            last_prepared_cell_id: None,
            isolation_mode: BranchIsolationMode::Disabled,
            last_isolation_result: None,
        });
    state.isolation_mode = BranchIsolationMode::NamespaceGuarded;
    ws.set_tree_runtime_state(state).await.unwrap();

    let executions = ws
        .execute_all_branches_in_experiment_tree(&tree_id)
        .await
        .unwrap();
    for (_, exec_id) in &executions {
        wait_for_execution_finished(&ws, exec_id).await;
    }

    let guarded_state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(guarded_state.kernel_state, TreeKernelState::Ready);
    assert_eq!(guarded_state.runtime_epoch, 0);
    assert!(guarded_state.materialized_path_cell_ids.is_empty());

    let (_execution_id, logs) = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &root_branch_id, &CellId::new("step2"))
        .await
        .unwrap();
    assert!(logs.error.is_none());

    let reused_state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(reused_state.runtime_epoch, guarded_state.runtime_epoch);
    assert_eq!(reused_state.kernel_state, TreeKernelState::Ready);
    assert_eq!(
        reused_state.last_prepared_cell_id,
        Some(CellId::new("step2"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_namespace_guarded_contamination_marks_replay_and_emits_fallback() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    let _branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &root_branch_id,
            "branch-contaminated".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_contaminated_cell"),
                tree_id: tree_id.clone(),
                branch_id: tine_core::BranchId::new("ignored"),
                name: "Branch contaminated cell".to_string(),
                code: NodeCode {
                    source: "import time\ntime.sleep(0.5)\nbranch_contaminated = step2 + 1"
                        .to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_contaminated")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let mut state = ws
        .get_tree_runtime_state(&tree_id)
        .await
        .unwrap_or_else(|| tine_core::TreeRuntimeState {
            tree_id: tree_id.clone(),
            active_branch_id: root_branch_id.clone(),
            materialized_path_cell_ids: Vec::new(),
            runtime_epoch: 0,
            kernel_state: TreeKernelState::NeedsReplay,
            last_prepared_cell_id: None,
            isolation_mode: BranchIsolationMode::Disabled,
            last_isolation_result: None,
        });
    state.isolation_mode = BranchIsolationMode::NamespaceGuarded;
    ws.set_tree_runtime_state(state).await.unwrap();

    let mut rx = ws.subscribe_events();

    let executions = ws
        .execute_all_branches_in_experiment_tree(&tree_id)
        .await
        .unwrap();
    for (_, exec_id) in &executions {
        wait_for_execution_finished(&ws, exec_id).await;
    }
    for _ in 0..120 {
        if ws
            .get_tree_runtime_state(&tree_id)
            .await
            .is_some_and(|state| state.kernel_state == TreeKernelState::NeedsReplay)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let (saw_contamination, saw_fallback) = timeout(Duration::from_secs(30), async {
        let mut saw_contamination = false;
        let mut saw_fallback = false;
        loop {
            match rx.recv().await {
                Ok(ExecutionEvent::ContaminationDetected {
                    tree_id: evt_tree,
                    signals,
                    ..
                }) if evt_tree == tree_id => {
                    saw_contamination |= !signals.is_empty();
                }
                Ok(ExecutionEvent::FallbackRestartTriggered {
                    tree_id: evt_tree,
                    reason,
                    ..
                }) if evt_tree == tree_id => {
                    saw_fallback |= reason == "contamination_detected";
                }
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    panic!(
                        "lagged while waiting for guarded contamination events, skipped {skipped}"
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    panic!("event channel closed before guarded contamination signals arrived");
                }
            }

            if saw_contamination && saw_fallback {
                break (saw_contamination, saw_fallback);
            }
        }
    })
    .await
    .expect("timed out waiting for guarded contamination/fallback events");

    assert!(
        saw_contamination,
        "expected contamination event for guarded branch"
    );
    assert!(
        saw_fallback,
        "expected fallback restart for contaminated guarded branch"
    );

    let runtime_state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    let isolation_result = runtime_state
        .last_isolation_result
        .as_ref()
        .expect("expected contamination result to be recorded");
    assert!(!isolation_result.succeeded);
    assert!(
        isolation_result
            .contamination_signals
            .iter()
            .any(|signal| signal == "session_overlap" || signal == "session_end_failed"),
        "expected guarded contamination signal, got {:?}",
        isolation_result.contamination_signals
    );
    assert_eq!(runtime_state.kernel_state, TreeKernelState::NeedsReplay);
    assert!(runtime_state.materialized_path_cell_ids.is_empty());
    assert_eq!(runtime_state.last_prepared_cell_id, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_namespace_guarded_end_session_failure_marks_replay_and_records_signal() {
    let (_tmp, ws) = open_temp_workspace().await;
    let root_branch_id = tine_core::BranchId::new("main");
    let mut tree = two_cell_tree();
    let tree_id = tree.id.clone();
    tree.cells
        .iter_mut()
        .find(|cell| cell.id == CellId::new("step2"))
        .expect("expected step2 cell in test tree")
        .code
        .source = "_pf_end_branch_session = None\nstep2 = step1 * 2".to_string();
    ws.save_experiment_tree(&tree).await.unwrap();

    let mut state = ws
        .get_tree_runtime_state(&tree_id)
        .await
        .unwrap_or_else(|| tine_core::TreeRuntimeState {
            tree_id: tree_id.clone(),
            active_branch_id: root_branch_id.clone(),
            materialized_path_cell_ids: Vec::new(),
            runtime_epoch: 0,
            kernel_state: TreeKernelState::NeedsReplay,
            last_prepared_cell_id: None,
            isolation_mode: BranchIsolationMode::Disabled,
            last_isolation_result: None,
        });
    state.isolation_mode = BranchIsolationMode::NamespaceGuarded;
    ws.set_tree_runtime_state(state).await.unwrap();

    let mut rx = ws.subscribe_events();

    let executions = ws
        .execute_all_branches_in_experiment_tree(&tree_id)
        .await
        .unwrap();
    for (_, exec_id) in &executions {
        wait_for_execution_finished(&ws, exec_id).await;
    }
    for _ in 0..120 {
        if ws
            .get_tree_runtime_state(&tree_id)
            .await
            .and_then(|state| state.last_isolation_result)
            .is_some_and(|result| result.branch_id == root_branch_id)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let mut fallback_reason = None;
    loop {
        match rx.try_recv() {
            Ok(ExecutionEvent::FallbackRestartTriggered {
                tree_id: evt_tree,
                branch_id: evt_branch_id,
                reason,
            }) if evt_tree == tree_id && evt_branch_id == root_branch_id => {
                fallback_reason = Some(reason);
                break;
            }
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(skipped)) => {
                panic!("lagged while draining events, skipped {skipped}");
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
        }
    }
    let fallback_reason =
        fallback_reason.expect("expected fallback restart event for end-session failure");

    assert!(
        fallback_reason.contains("failed_to_end_branch_session"),
        "unexpected fallback reason: {fallback_reason}"
    );

    let runtime_state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    let isolation_result = runtime_state
        .last_isolation_result
        .as_ref()
        .expect("expected end-session failure result to be recorded");
    assert_eq!(isolation_result.branch_id, root_branch_id);
    assert!(!isolation_result.succeeded);
    assert_eq!(
        isolation_result.contamination_signals,
        vec!["session_end_failed".to_string()]
    );
    assert_eq!(runtime_state.kernel_state, TreeKernelState::NeedsReplay);
    assert!(runtime_state.materialized_path_cell_ids.is_empty());
    assert_eq!(runtime_state.last_prepared_cell_id, None);
}

// ---------------------------------------------------------------------------
// 10. Shutdown
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial]
async fn test_shutdown_clean() {
    let (_tmp, ws) = open_temp_workspace().await;
    ws.save_experiment_tree(&trivial_tree()).await.unwrap();
    ws.shutdown().await.unwrap();
}

#[tokio::test]
#[serial]
async fn test_new_project_experiment_is_listed_and_has_tree() {
    let (_tmp, ws) = open_temp_workspace().await;
    let project = ProjectDef {
        id: ProjectId::new("project-ui"),
        name: "UI project".to_string(),
        description: None,
        workspace_dir: ".".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let project_id = ws.create_project(project).await.unwrap();

    let mut tree = two_cell_tree();
    tree.project_id = Some(project_id.clone());
    tree.name = "experiment_from_ui".to_string();
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;

    let experiments = ws.list_experiments(&project_id).await.unwrap();
    assert!(experiments
        .iter()
        .any(|exp| exp.id.as_str() == tree_id.as_str()));

    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    assert_eq!(loaded.id, tree_id);
    assert_eq!(loaded.project_id.unwrap().as_str(), project_id.as_str());
}

#[tokio::test]
#[serial]
async fn test_add_cell_to_experiment_tree_branch_updates_branch_order() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree = two_cell_tree();
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &tine_core::BranchId::new("main"),
            "branch-a".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_cell_1"),
                tree_id: tree_id.clone(),
                branch_id: tine_core::BranchId::new("ignored"),
                name: "Branch cell 1".to_string(),
                code: NodeCode {
                    source: "branch_value = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_value")],
                cache: true,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    ws.add_cell_to_experiment_tree_branch(
        &tree_id,
        &branch_id,
        CellDef {
            id: CellId::new("branch_cell_2"),
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            name: "Branch cell 2".to_string(),
            code: NodeCode {
                source: "branch_value_2 = branch_value + 1".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![CellId::new("branch_cell_1")],
            declared_outputs: vec![SlotName::new("branch_value_2")],
            cache: true,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        },
        Some(&CellId::new("branch_cell_1")),
    )
    .await
    .unwrap();

    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    let branch = loaded
        .branches
        .iter()
        .find(|branch| branch.id == branch_id)
        .unwrap();

    assert_eq!(
        branch
            .cell_order
            .iter()
            .map(|cell_id| cell_id.as_str())
            .collect::<Vec<_>>(),
        vec!["branch_cell_1", "branch_cell_2"]
    );
    assert!(loaded
        .cells
        .iter()
        .any(|cell| cell.id.as_str() == "branch_cell_2"));
}

#[tokio::test]
#[serial]
async fn test_branch_cell_mutations_persist() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree = two_cell_tree();
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &tine_core::BranchId::new("main"),
            "branch-b".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_cell_1"),
                tree_id: tree_id.clone(),
                branch_id: tine_core::BranchId::new("ignored"),
                name: "Branch cell 1".to_string(),
                code: NodeCode {
                    source: "branch_value = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_value")],
                cache: true,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    ws.add_cell_to_experiment_tree_branch(
        &tree_id,
        &branch_id,
        CellDef {
            id: CellId::new("branch_cell_2"),
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            name: "Branch cell 2".to_string(),
            code: NodeCode {
                source: "branch_value_2 = branch_value + 1".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![CellId::new("branch_cell_1")],
            declared_outputs: vec![SlotName::new("branch_value_2")],
            cache: true,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        },
        Some(&CellId::new("branch_cell_1")),
    )
    .await
    .unwrap();

    ws.update_cell_code_in_experiment_tree(
        &tree_id,
        &CellId::new("branch_cell_2"),
        "branch_value_2 = 99",
    )
    .await
    .unwrap();
    ws.move_cell_in_experiment_tree_branch(
        &tree_id,
        &branch_id,
        &CellId::new("branch_cell_2"),
        "up",
    )
    .await
    .unwrap();
    ws.delete_cell_from_experiment_tree_branch(&tree_id, &branch_id, &CellId::new("branch_cell_1"))
        .await
        .unwrap();

    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    let branch = loaded
        .branches
        .iter()
        .find(|branch| branch.id == branch_id)
        .unwrap();
    assert_eq!(
        branch
            .cell_order
            .iter()
            .map(|cell_id| cell_id.as_str())
            .collect::<Vec<_>>(),
        vec!["branch_cell_2"]
    );
    let cell = loaded
        .cells
        .iter()
        .find(|cell| cell.id.as_str() == "branch_cell_2")
        .unwrap();
    assert_eq!(cell.code.source, "branch_value_2 = 99");
    assert!(loaded
        .cells
        .iter()
        .all(|cell| cell.id.as_str() != "branch_cell_1"));
}

#[tokio::test]
#[serial]
async fn test_branch_scoped_cell_routes_reject_membership_mismatch() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &tine_core::BranchId::new("main"),
            "branch-b".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_cell_1"),
                tree_id: tree_id.clone(),
                branch_id: tine_core::BranchId::new("ignored"),
                name: "Branch cell 1".to_string(),
                code: NodeCode {
                    source: "branch_value = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_value")],
                cache: true,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let err = ws
        .update_cell_code_in_experiment_tree_branch(
            &tree_id,
            &branch_id,
            &CellId::new("step1"),
            "value = 0",
        )
        .await
        .unwrap_err();
    match err {
        TineError::Internal(msg) => assert!(msg.contains("not found in branch")),
        other => panic!("unexpected error: {other:?}"),
    }

    let err = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &branch_id, &CellId::new("step1"))
        .await
        .unwrap_err();
    match err {
        TineError::Internal(msg) => assert!(msg.contains("not found in branch")),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn test_tree_runtime_state_helpers_track_materialization() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let mut rx = ws.subscribe_events();

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &tine_core::BranchId::new("main"),
            "branch-runtime".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_cell_runtime"),
                tree_id: tree_id.clone(),
                branch_id: tine_core::BranchId::new("ignored"),
                name: "Branch runtime cell".to_string(),
                code: NodeCode {
                    source: "branch_value = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_value")],
                cache: true,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let prepared = ws
        .prepare_context(&tree_id, &branch_id, &CellId::new("branch_cell_runtime"))
        .await
        .unwrap();
    assert_eq!(prepared.branch_id, branch_id);
    assert_eq!(
        prepared
            .runtime_state
            .materialized_path_cell_ids
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        vec!["step1", "step2", "branch_cell_runtime"]
    );
    assert_eq!(
        prepared.runtime_state.last_prepared_cell_id,
        Some(CellId::new("step2"))
    );
    assert_eq!(prepared.runtime_state.kernel_state, TreeKernelState::Ready);
    assert_eq!(prepared.runtime_state.runtime_epoch, 1);

    let stored = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(stored.active_branch_id, branch_id);
    assert_eq!(stored.runtime_epoch, 1);

    let replay_state = ws.mark_tree_needs_replay(&tree_id).await.unwrap();
    assert_eq!(replay_state.kernel_state, TreeKernelState::NeedsReplay);
    assert!(replay_state.materialized_path_cell_ids.is_empty());
    assert_eq!(replay_state.runtime_epoch, 2);

    let mut saw_switching = false;
    let mut saw_ready = false;
    let mut saw_needs_replay = false;
    timeout(Duration::from_secs(5), async {
        loop {
            match rx.recv().await.expect("event channel closed") {
                ExecutionEvent::TreeRuntimeStateChanged {
                    tree_id: event_tree_id,
                    branch_id: event_branch_id,
                    kernel_state,
                    ..
                } if event_tree_id == tree_id && event_branch_id == branch_id => match kernel_state
                {
                    TreeKernelState::Switching => saw_switching = true,
                    TreeKernelState::Ready => saw_ready = true,
                    TreeKernelState::NeedsReplay => saw_needs_replay = true,
                    TreeKernelState::KernelLost => {}
                },
                _ => {}
            }
            if saw_switching && saw_ready && saw_needs_replay {
                break;
            }
        }
    })
    .await
    .expect("timed out waiting for tree runtime state events");
    assert!(saw_switching, "expected switching runtime event");
    assert!(saw_ready, "expected ready runtime event");
    assert!(saw_needs_replay, "expected needs_replay runtime event");
}

#[tokio::test]
#[serial]
async fn test_tree_runtime_state_hydrates_after_workspace_reopen() {
    let tmp = TempDir::new().expect("failed to create temp dir");
    let store: Arc<dyn ArtifactStore> = Arc::new(MemoryArtifactStore::new());
    let ws = Workspace::open(tmp.path().to_path_buf(), Arc::clone(&store), 4)
        .await
        .expect("failed to open workspace");
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;

    let state = tine_core::TreeRuntimeState {
        tree_id: tree_id.clone(),
        active_branch_id: tine_core::BranchId::new("main"),
        materialized_path_cell_ids: vec![CellId::new("step1"), CellId::new("step2")],
        runtime_epoch: 7,
        kernel_state: TreeKernelState::Ready,
        last_prepared_cell_id: Some(CellId::new("step2")),
        isolation_mode: tine_core::BranchIsolationMode::NamespaceGuarded,
        last_isolation_result: Some(tine_core::IsolationResult {
            branch_id: tine_core::BranchId::new("main"),
            succeeded: true,
            contamination_signals: Vec::new(),
            namespace_delta: Some(tine_core::NamespaceDelta {
                added: vec!["df".to_string()],
                removed: Vec::new(),
                changed: vec!["model".to_string()],
                module_drift: Vec::new(),
            }),
        }),
    };
    ws.set_tree_runtime_state(state.clone()).await.unwrap();
    drop(ws);

    let reopened = Workspace::open(tmp.path().to_path_buf(), store, 4)
        .await
        .expect("failed to reopen workspace");
    let hydrated = reopened.get_tree_runtime_state(&tree_id).await.unwrap();

    assert_eq!(hydrated, state);
}

#[tokio::test]
#[serial]
async fn test_tree_mutations_force_runtime_replay() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    ws.execute_cell_in_experiment_tree_branch(&tree_id, &root_branch_id, &CellId::new("step2"))
        .await
        .unwrap();

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &root_branch_id,
            "branch-runtime".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_cell_runtime"),
                tree_id: tree_id.clone(),
                branch_id: tine_core::BranchId::new("ignored"),
                name: "Branch runtime cell".to_string(),
                code: NodeCode {
                    source: "branch_value = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_value")],
                cache: true,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let mut state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(state.kernel_state, TreeKernelState::NeedsReplay);
    assert!(state.materialized_path_cell_ids.is_empty());

    ws.execute_cell_in_experiment_tree_branch(
        &tree_id,
        &branch_id,
        &CellId::new("branch_cell_runtime"),
    )
    .await
    .unwrap();
    ws.add_cell_to_experiment_tree_branch(
        &tree_id,
        &branch_id,
        CellDef {
            id: CellId::new("branch_cell_runtime_2"),
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            name: "Branch runtime cell 2".to_string(),
            code: NodeCode {
                source: "branch_value_2 = branch_value + 1".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![CellId::new("branch_cell_runtime")],
            declared_outputs: vec![SlotName::new("branch_value_2")],
            cache: true,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        },
        Some(&CellId::new("branch_cell_runtime")),
    )
    .await
    .unwrap();
    state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(state.kernel_state, TreeKernelState::NeedsReplay);
    assert!(state.materialized_path_cell_ids.is_empty());

    ws.execute_cell_in_experiment_tree_branch(
        &tree_id,
        &branch_id,
        &CellId::new("branch_cell_runtime"),
    )
    .await
    .unwrap();
    ws.move_cell_in_experiment_tree_branch(
        &tree_id,
        &branch_id,
        &CellId::new("branch_cell_runtime_2"),
        "up",
    )
    .await
    .unwrap();
    state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(state.kernel_state, TreeKernelState::NeedsReplay);

    ws.execute_cell_in_experiment_tree_branch(
        &tree_id,
        &branch_id,
        &CellId::new("branch_cell_runtime"),
    )
    .await
    .unwrap();
    ws.delete_cell_from_experiment_tree_branch(
        &tree_id,
        &branch_id,
        &CellId::new("branch_cell_runtime_2"),
    )
    .await
    .unwrap();
    state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(state.kernel_state, TreeKernelState::NeedsReplay);

    ws.execute_cell_in_experiment_tree_branch(&tree_id, &root_branch_id, &CellId::new("step2"))
        .await
        .unwrap();
    ws.update_cell_code_in_experiment_tree(&tree_id, &CellId::new("step1"), "step1 = 99")
        .await
        .unwrap();
    state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(state.kernel_state, TreeKernelState::NeedsReplay);
    assert!(state.materialized_path_cell_ids.is_empty());
}

#[tokio::test]
#[serial]
async fn test_mark_tree_kernel_lost_clears_materialization() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    ws.execute_cell_in_experiment_tree_branch(&tree_id, &root_branch_id, &CellId::new("step2"))
        .await
        .unwrap();

    let lost_state = ws.mark_tree_kernel_lost(&tree_id).await.unwrap().unwrap();

    assert_eq!(lost_state.kernel_state, TreeKernelState::KernelLost);
    assert!(lost_state.materialized_path_cell_ids.is_empty());
    assert_eq!(lost_state.last_prepared_cell_id, None);

    let stored = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(stored.kernel_state, TreeKernelState::KernelLost);
    assert!(stored.materialized_path_cell_ids.is_empty());
}

#[tokio::test]
#[serial]
async fn test_root_branch_single_cell_execute_uses_tree_runtime() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    let (execution_id, logs) = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &root_branch_id, &CellId::new("step2"))
        .await
        .unwrap();

    assert!(!execution_id.as_str().is_empty());
    assert!(logs.error.is_none());

    let stored = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(stored.active_branch_id, root_branch_id);
    assert_eq!(
        stored
            .materialized_path_cell_ids
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        vec!["step1", "step2"]
    );
    assert_eq!(stored.last_prepared_cell_id, Some(CellId::new("step2")));
    assert_eq!(stored.kernel_state, TreeKernelState::Ready);

    let tree_logs = ws
        .logs_for_tree_cell(&tree_id, &root_branch_id, &CellId::new("step2"))
        .await
        .unwrap();
    assert!(tree_logs.error.is_none());
}

#[tokio::test]
#[serial]
async fn test_first_root_cell_execute_bootstraps_tree_kernel() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    let (execution_id, logs) = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &root_branch_id, &CellId::new("step1"))
        .await
        .unwrap();

    assert!(!execution_id.as_str().is_empty());
    assert!(logs.error.is_none());

    let stored = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(stored.active_branch_id, root_branch_id);
    assert_eq!(
        stored
            .materialized_path_cell_ids
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        vec!["step1", "step2"]
    );
    assert_eq!(stored.last_prepared_cell_id, Some(CellId::new("step1")));
    assert_eq!(stored.kernel_state, TreeKernelState::Ready);

    let tree_logs = ws
        .logs_for_tree_cell(&tree_id, &root_branch_id, &CellId::new("step1"))
        .await
        .unwrap();
    assert!(tree_logs.error.is_none());
}

#[tokio::test]
#[serial]
async fn test_root_branch_add_edit_and_execute_through_tree_api() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");
    let new_cell_id = CellId::new("step3");

    ws.add_cell_to_experiment_tree_branch(
        &tree_id,
        &root_branch_id,
        CellDef {
            id: new_cell_id.clone(),
            tree_id: tree_id.clone(),
            branch_id: root_branch_id.clone(),
            name: "Step 3".to_string(),
            code: NodeCode {
                source: "print(step2 + 5)\nstep3 = step2 + 5".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![CellId::new("step2")],
            declared_outputs: vec![SlotName::new("step3")],
            cache: false,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        },
        Some(&CellId::new("step2")),
    )
    .await
    .unwrap();

    ws.update_cell_code_in_experiment_tree_branch(
        &tree_id,
        &root_branch_id,
        &new_cell_id,
        "print(step2 + 7)\nstep3 = step2 + 7",
    )
    .await
    .unwrap();

    let (_execution_id, logs) = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &root_branch_id, &new_cell_id)
        .await
        .unwrap();

    assert!(logs.error.is_none());
    assert!(
        logs.stdout.contains("91"),
        "expected updated root cell stdout to include 91, got {:?}",
        logs.stdout
    );

    let persisted = ws
        .logs_for_tree_cell(&tree_id, &root_branch_id, &new_cell_id)
        .await
        .unwrap();
    assert!(persisted.stdout.contains("91"));

    let state = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(state.active_branch_id, root_branch_id);
    assert_eq!(state.last_prepared_cell_id, Some(new_cell_id));
    assert_eq!(
        state
            .materialized_path_cell_ids
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        vec!["step1", "step2", "step3"]
    );
}

#[tokio::test]
#[serial]
async fn test_child_branch_single_cell_execute_replays_and_branch_switch_advances_runtime() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    let branch_a = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &root_branch_id,
            "branch-a".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_a_cell"),
                tree_id: tree_id.clone(),
                branch_id: root_branch_id.clone(),
                name: "Branch A".to_string(),
                code: NodeCode {
                    source: "print(step2 + 1)\nbranch_a = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_a")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let branch_b = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &root_branch_id,
            "branch-b".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_b_cell"),
                tree_id: tree_id.clone(),
                branch_id: root_branch_id.clone(),
                name: "Branch B".to_string(),
                code: NodeCode {
                    source: "print(step2 + 2)\nbranch_b = step2 + 2".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_b")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let (_exec_a, logs_a) = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &branch_a, &CellId::new("branch_a_cell"))
        .await
        .unwrap();
    assert!(logs_a.stdout.contains("85"));

    let state_a = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(state_a.active_branch_id, branch_a);
    assert_eq!(
        state_a.last_prepared_cell_id,
        Some(CellId::new("branch_a_cell"))
    );
    assert_eq!(state_a.runtime_epoch, 1);

    let (_exec_b, logs_b) = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &branch_b, &CellId::new("branch_b_cell"))
        .await
        .unwrap();
    assert!(logs_b.stdout.contains("86"));

    let state_b = ws.get_tree_runtime_state(&tree_id).await.unwrap();
    assert_eq!(state_b.active_branch_id, branch_b);
    assert_eq!(
        state_b.last_prepared_cell_id,
        Some(CellId::new("branch_b_cell"))
    );
    assert_eq!(state_b.kernel_state, TreeKernelState::Ready);
    assert!(state_b.runtime_epoch > state_a.runtime_epoch);
    assert_eq!(
        state_b
            .materialized_path_cell_ids
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
        vec!["step1", "step2", "branch_b_cell"]
    );

    let persisted_b = ws
        .logs_for_tree_cell(&tree_id, &branch_b, &CellId::new("branch_b_cell"))
        .await
        .unwrap();
    assert!(persisted_b.stdout.contains("86"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_run_all_branches_replays_root_variables_for_branch_cells() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ws.save_experiment_tree(&two_cell_tree()).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    let branch_id = ws
        .create_branch_in_experiment_tree(
            &tree_id,
            &root_branch_id,
            "branch-vars".to_string(),
            &CellId::new("step2"),
            CellDef {
                id: CellId::new("branch_step"),
                tree_id: tree_id.clone(),
                branch_id: tine_core::BranchId::new("ignored"),
                name: "Branch Step".to_string(),
                code: NodeCode {
                    source: "print(step2 + 1)\nbranch_val = step2 + 1".to_string(),
                    language: "python".to_string(),
                },
                upstream_cell_ids: vec![CellId::new("step2")],
                declared_outputs: vec![SlotName::new("branch_val")],
                cache: false,
                map_over: None,
                map_concurrency: None,
                timeout_secs: None,
                tags: HashMap::new(),
                revision_id: None,
                state: CellRuntimeState::Clean,
            },
        )
        .await
        .unwrap();

    let (_root_exec, root_first_logs) = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &root_branch_id, &CellId::new("step2"))
        .await
        .unwrap();
    assert!(root_first_logs.error.is_none());

    let (_branch_exec, branch_first_logs) = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &branch_id, &CellId::new("branch_step"))
        .await
        .unwrap();
    assert!(branch_first_logs.error.is_none());

    let executions = ws
        .execute_all_branches_in_experiment_tree(&tree_id)
        .await
        .unwrap();
    for (_, exec_id) in &executions {
        wait_for_execution_finished(&ws, exec_id).await;
    }

    let root_logs = ws
        .logs_for_tree_cell(&tree_id, &root_branch_id, &CellId::new("step2"))
        .await
        .unwrap();
    assert!(
        root_logs.error.is_none(),
        "root cell should run without errors"
    );

    let branch_logs = ws
        .logs_for_tree_cell(&tree_id, &branch_id, &CellId::new("branch_step"))
        .await
        .unwrap();
    assert!(
        branch_logs.error.is_none(),
        "branch cell should see step2 from replayed root path, got {:?}",
        branch_logs.error
    );
    assert!(
        branch_logs.stdout.contains("85"),
        "expected branch stdout to include step2+1=85, got {:?}",
        branch_logs.stdout
    );
}

#[tokio::test]
#[serial]
async fn test_project_scoped_file_access_uses_workspace_dir() {
    let (tmp, ws) = open_temp_workspace().await;
    let project_root = tmp.path().join("project-files");
    fs::create_dir_all(project_root.join("nested")).unwrap();
    fs::write(project_root.join("notes.txt"), "hello from project").unwrap();

    let project = ProjectDef {
        id: ProjectId::new("project-files"),
        name: "Files project".to_string(),
        description: None,
        workspace_dir: "project-files".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let project_id = ws.create_project(project).await.unwrap();

    let entries = ws.list_project_files(Some(&project_id), "").await.unwrap();
    assert!(entries.iter().any(|entry| entry.name == "notes.txt"));
    assert!(entries.iter().all(|entry| entry.name != "tine.db"));

    let content = ws
        .read_project_file(Some(&project_id), "notes.txt")
        .await
        .unwrap();
    assert_eq!(content, "hello from project");

    ws.write_project_file(Some(&project_id), "nested/output.txt", "written")
        .await
        .unwrap();
    let written = fs::read_to_string(project_root.join("nested").join("output.txt")).unwrap();
    assert_eq!(written, "written");
}

#[tokio::test]
#[serial]
async fn test_project_scoped_execution_uses_project_workspace_dir() {
    let (tmp, ws) = open_temp_workspace().await;
    let project_root = tmp.path().join("project-exec");
    fs::create_dir_all(&project_root).unwrap();
    fs::write(project_root.join("notes.txt"), "hello from kernel cwd").unwrap();

    let project = ProjectDef {
        id: ProjectId::new("project-exec"),
        name: "Exec project".to_string(),
        description: None,
        workspace_dir: "project-exec".to_string(),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let project_id = ws.create_project(project).await.unwrap();

    let mut tree = two_cell_tree();
    tree.id = ExperimentTreeId::new("project-relative-cwd");
    tree.name = "project-relative-cwd".to_string();
    tree.project_id = Some(project_id.clone());
    tree.cells[0].code.source = r#"
with open("notes.txt", "r", encoding="utf-8") as fh:
    step1 = fh.read().strip()
print(step1)
"#
    .to_string();
    tree.cells[1].code.source = "step2 = step1".to_string();
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;
    let root_branch_id = tine_core::BranchId::new("main");

    let (_exec_id, logs) = ws
        .execute_cell_in_experiment_tree_branch(&tree_id, &root_branch_id, &CellId::new("step1"))
        .await
        .unwrap();
    assert!(
        logs.stdout.contains("hello from kernel cwd"),
        "expected relative file read from project workspace dir, got {:?}",
        logs.stdout
    );
}

#[tokio::test]
#[serial]
async fn test_move_cell_in_experiment_tree_branch() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree = two_cell_tree();
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;
    let branch_id = BranchId::new("main");

    // Add a second cell after the last root cell so we have two cells to swap
    ws.add_cell_to_experiment_tree_branch(
        &tree_id,
        &branch_id,
        CellDef {
            id: CellId::new("move_cell_b"),
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            name: "Move cell B".to_string(),
            code: NodeCode {
                source: "b = 1".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![],
            declared_outputs: vec![SlotName::new("b")],
            cache: true,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        },
        None,
    )
    .await
    .unwrap();

    // Add a third cell so we have [last_root_cell, move_cell_b, move_cell_c]
    ws.add_cell_to_experiment_tree_branch(
        &tree_id,
        &branch_id,
        CellDef {
            id: CellId::new("move_cell_c"),
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            name: "Move cell C".to_string(),
            code: NodeCode {
                source: "c = 2".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![],
            declared_outputs: vec![SlotName::new("c")],
            cache: true,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        },
        None,
    )
    .await
    .unwrap();

    // Verify initial order ends with [..., move_cell_b, move_cell_c]
    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    let branch = loaded.branches.iter().find(|b| b.id == branch_id).unwrap();
    let order_before: Vec<&str> = branch.cell_order.iter().map(|id| id.as_str()).collect();
    let len = order_before.len();
    assert_eq!(order_before[len - 2], "move_cell_b");
    assert_eq!(order_before[len - 1], "move_cell_c");

    // Move move_cell_c up — it should swap with move_cell_b
    ws.move_cell_in_experiment_tree_branch(&tree_id, &branch_id, &CellId::new("move_cell_c"), "up")
        .await
        .unwrap();

    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    let branch = loaded.branches.iter().find(|b| b.id == branch_id).unwrap();
    let order_after: Vec<&str> = branch.cell_order.iter().map(|id| id.as_str()).collect();
    assert_eq!(
        order_after[len - 2],
        "move_cell_c",
        "move_cell_c should be one position earlier after moving up"
    );
    assert_eq!(order_after[len - 1], "move_cell_b");

    // Move move_cell_c back down — order should be restored
    ws.move_cell_in_experiment_tree_branch(
        &tree_id,
        &branch_id,
        &CellId::new("move_cell_c"),
        "down",
    )
    .await
    .unwrap();

    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    let branch = loaded.branches.iter().find(|b| b.id == branch_id).unwrap();
    let order_restored: Vec<&str> = branch.cell_order.iter().map(|id| id.as_str()).collect();
    assert_eq!(order_restored[len - 2], "move_cell_b");
    assert_eq!(order_restored[len - 1], "move_cell_c");
}

#[tokio::test]
#[serial]
async fn test_delete_cell_from_experiment_tree_branch() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree = two_cell_tree();
    let tree_id = ws.save_experiment_tree(&tree).await.unwrap().id;
    let branch_id = BranchId::new("main");

    // Add a cell we will delete
    ws.add_cell_to_experiment_tree_branch(
        &tree_id,
        &branch_id,
        CellDef {
            id: CellId::new("to_delete"),
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            name: "Cell to delete".to_string(),
            code: NodeCode {
                source: "x = 42".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![],
            declared_outputs: vec![SlotName::new("x")],
            cache: true,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        },
        None,
    )
    .await
    .unwrap();

    // Confirm the cell is present
    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    let branch = loaded.branches.iter().find(|b| b.id == branch_id).unwrap();
    assert!(branch
        .cell_order
        .iter()
        .any(|id| id.as_str() == "to_delete"));
    assert!(loaded.cells.iter().any(|c| c.id.as_str() == "to_delete"));
    let count_before = branch.cell_order.len();

    // Delete the cell
    ws.delete_cell_from_experiment_tree_branch(&tree_id, &branch_id, &CellId::new("to_delete"))
        .await
        .unwrap();

    // Confirm it is gone from both order and cell list
    let loaded = ws.get_experiment_tree(&tree_id).await.unwrap();
    let branch = loaded.branches.iter().find(|b| b.id == branch_id).unwrap();
    assert_eq!(branch.cell_order.len(), count_before - 1);
    assert!(
        !branch
            .cell_order
            .iter()
            .any(|id| id.as_str() == "to_delete"),
        "cell should be removed from order"
    );
    assert!(
        !loaded.cells.iter().any(|c| c.id.as_str() == "to_delete"),
        "cell should be removed from cells list"
    );

    // Attempt to delete non-existent cell returns an error
    let err = ws
        .delete_cell_from_experiment_tree_branch(&tree_id, &branch_id, &CellId::new("to_delete"))
        .await;
    assert!(
        err.is_err(),
        "deleting a non-existent cell should return an error"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_workspace_cancel_interrupts_running_branch_and_preserves_partial_logs() {
    let (_tmp, ws) = open_temp_workspace().await;
    let tree_id = ExperimentTreeId::new("cancel-api-tree");
    let branch_id = BranchId::new("main");
    let cell_id = CellId::new("step1");
    let tree = ExperimentTreeDef {
        id: tree_id.clone(),
        name: "cancel-api-tree".to_string(),
        project_id: None,
        root_branch_id: branch_id.clone(),
        branches: vec![BranchDef {
            id: branch_id.clone(),
            name: "main".to_string(),
            parent_branch_id: None,
            branch_point_cell_id: None,
            cell_order: vec![cell_id.clone()],
            display: HashMap::new(),
        }],
        cells: vec![CellDef {
            id: cell_id.clone(),
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            name: "step1".to_string(),
            code: NodeCode {
                source: "import time\nprint('starting cancel api test', flush=True)\ntime.sleep(20)\nprint('should not reach cancel api end', flush=True)\nstep1 = 42\n".to_string(),
                language: "python".to_string(),
            },
            upstream_cell_ids: vec![],
            declared_outputs: vec![SlotName::new("step1")],
            cache: false,
            map_over: None,
            map_concurrency: None,
            timeout_secs: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        }],
        environment: Default::default(),
        execution_mode: ExecutionMode::Parallel,
        budget: None,
        created_at: chrono::Utc::now(),
    };
    ws.save_experiment_tree(&tree).await.unwrap();

    let execution_id = ws
        .execute_branch_in_experiment_tree(&tree_id, &branch_id)
        .await
        .unwrap();

    let running_status = wait_for_node_running(&ws, &execution_id, &NodeId::new("step1")).await;
    assert_eq!(running_status.tree_id.as_ref(), Some(&tree_id));
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    ws.cancel(&execution_id).await.unwrap();

    let final_status = wait_for_execution_finished(&ws, &execution_id).await;
    assert_eq!(final_status.tree_id.as_ref(), Some(&tree_id));
    assert_eq!(
        final_status.node_statuses.get(&NodeId::new("step1")),
        Some(&NodeStatus::Interrupted)
    );

    let logs = ws
        .logs_for_tree_cell(&tree_id, &branch_id, &cell_id)
        .await
        .unwrap();
    assert!(
        logs.stdout.contains("starting cancel api test"),
        "expected partial stdout to persist after cancel, got {:?}",
        logs.stdout
    );
    assert!(
        !logs.stdout.contains("should not reach cancel api end"),
        "unexpected post-cancel stdout in {:?}",
        logs.stdout
    );
}
