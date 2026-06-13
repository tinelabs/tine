use std::collections::{HashMap, HashSet};

use petgraph::algo::is_cyclic_directed;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{Bfs, EdgeRef, IntoNodeReferences, Topo};
use petgraph::Direction;

use tine_core::{
    ExecutableTreeBranch, ExecutableTreeCell, NodeCacheKey, NodeId, SlotName, TineError, TineResult,
};

/// A DAG representation of an executable tree branch, backed by petgraph.
#[derive(Debug, Clone)]
pub struct ExecutableTreeGraph {
    graph: DiGraph<NodeId, SlotName>,
    node_indices: HashMap<NodeId, NodeIndex>,
}

fn runtime_id(branch: &ExecutableTreeBranch) -> String {
    format!("{}::{}", branch.tree_id.as_str(), branch.branch_id.as_str())
}

fn node_id_for_cell(cell: &ExecutableTreeCell) -> NodeId {
    NodeId::new(cell.cell_id.as_str())
}

fn cell_by_node_id<'a>(
    branch: &'a ExecutableTreeBranch,
    node_id: &NodeId,
) -> &'a ExecutableTreeCell {
    branch
        .cells
        .iter()
        .find(|cell| cell.cell_id.as_str() == node_id.as_str())
        .unwrap()
}

/// Compute the cache-key input hashes for `cell` from upstream artifact keys.
///
/// Each input edge is fingerprinted from the upstream cell's *complete*
/// artifact set (sorted slot/key pairs), because the projected edge does not
/// record which upstream variable the cell's code actually reads — hashing a
/// single slot would let entries collide when only another slot changed.
///
/// Inputs whose upstream declares no outputs are ordering-only edges: they
/// carry no data the cache model can fingerprint, so they are excluded from
/// the key (upstream code changes still invalidate downstream at plan time
/// via dirty propagation).
///
/// Returns `None` when an upstream's declared artifacts are not all available
/// — such a cell cannot be cached safely. Both the execution planner and the
/// cache writer must use this function so their keys never diverge.
pub fn cache_input_hashes(
    branch: &ExecutableTreeBranch,
    cell: &ExecutableTreeCell,
    node_artifacts: &HashMap<NodeId, HashMap<SlotName, tine_core::ArtifactKey>>,
) -> Option<HashMap<SlotName, [u8; 32]>> {
    let mut input_hashes = HashMap::new();
    for (slot, input) in &cell.inputs {
        let upstream = branch
            .cells
            .iter()
            .find(|c| c.cell_id == input.source_cell_id)?;
        if upstream.outputs.is_empty() {
            continue;
        }

        let artifacts = node_artifacts.get(&NodeId::new(input.source_cell_id.as_str()))?;
        let mut entries = Vec::with_capacity(upstream.outputs.len());
        for out_slot in &upstream.outputs {
            entries.push((out_slot.as_str(), artifacts.get(out_slot)?.as_str()));
        }
        entries.sort_by(|a, b| a.0.cmp(b.0));

        let mut fingerprint = String::new();
        for (out_slot, artifact_key) in entries {
            fingerprint.push_str(out_slot);
            fingerprint.push('\0');
            fingerprint.push_str(artifact_key);
            fingerprint.push('\0');
        }
        input_hashes.insert(slot.clone(), NodeCacheKey::hash_code(&fingerprint));
    }
    Some(input_hashes)
}

impl ExecutableTreeGraph {
    /// Build an `ExecutableTreeGraph` from an executable branch.
    pub fn from_branch(branch: &ExecutableTreeBranch) -> TineResult<Self> {
        let mut graph = DiGraph::new();
        let mut node_indices = HashMap::new();
        let runtime_id = runtime_id(branch);

        for cell in &branch.cells {
            let node_id = node_id_for_cell(cell);
            if node_indices.contains_key(&node_id) {
                return Err(TineError::DuplicateNode {
                    runtime_id,
                    node_id,
                });
            }
            let idx = graph.add_node(node_id.clone());
            node_indices.insert(node_id, idx);
        }

        for cell in &branch.cells {
            let to_id = node_id_for_cell(cell);
            let to_idx = node_indices[&to_id];
            for (slot_name, input) in &cell.inputs {
                let from_id = NodeId::new(input.source_cell_id.as_str());
                let from_idx =
                    node_indices
                        .get(&from_id)
                        .ok_or_else(|| TineError::InvalidEdge {
                            from: from_id.clone(),
                            to: to_id.clone(),
                            slot: slot_name.clone(),
                        })?;
                graph.add_edge(*from_idx, to_idx, slot_name.clone());
            }
        }

        Ok(Self {
            graph,
            node_indices,
        })
    }

    pub fn validate(&self, runtime_id: &str) -> TineResult<()> {
        if is_cyclic_directed(&self.graph) {
            return Err(TineError::CycleDetected {
                runtime_id: runtime_id.to_string(),
            });
        }
        Ok(())
    }

    pub fn topo_sort(&self) -> Vec<NodeId> {
        let mut topo = Topo::new(&self.graph);
        let mut sorted = Vec::new();
        while let Some(idx) = topo.next(&self.graph) {
            sorted.push(self.graph[idx].clone());
        }
        sorted
    }

    pub fn downstream_of(&self, changed: &HashSet<NodeId>) -> HashSet<NodeId> {
        let mut downstream = HashSet::new();
        for node_id in changed {
            if let Some(&start_idx) = self.node_indices.get(node_id) {
                let mut bfs = Bfs::new(&self.graph, start_idx);
                while let Some(idx) = bfs.next(&self.graph) {
                    let nid = &self.graph[idx];
                    if nid != node_id {
                        downstream.insert(nid.clone());
                    }
                }
            }
        }
        downstream
    }

    pub fn ready_nodes(&self, completed: &HashSet<NodeId>) -> Vec<NodeId> {
        let mut ready = Vec::new();
        for (idx, node_id) in self.graph.node_references() {
            if completed.contains(node_id) {
                continue;
            }
            let all_inputs_done = self
                .graph
                .edges_directed(idx, Direction::Incoming)
                .all(|edge| completed.contains(&self.graph[edge.source()]));
            if all_inputs_done {
                ready.push(node_id.clone());
            }
        }
        ready
    }

    /// Plan which nodes must execute and which can be skipped via the cache.
    ///
    /// Skipped nodes are returned together with the artifacts of the cache
    /// entry that matched their full key, so the executor injects exactly
    /// what was matched instead of re-resolving entries by code hash alone.
    pub fn plan_execution(
        &self,
        branch: &ExecutableTreeBranch,
        cache: &HashMap<NodeCacheKey, HashMap<SlotName, tine_core::ArtifactKey>>,
        lockfile_hash: [u8; 32],
    ) -> (
        Vec<NodeId>,
        Vec<(NodeId, HashMap<SlotName, tine_core::ArtifactKey>)>,
    ) {
        let topo = self.topo_sort();
        let mut to_execute = Vec::new();
        let mut to_skip = Vec::new();
        let mut must_execute: HashSet<NodeId> = HashSet::new();
        let mut node_artifacts: HashMap<NodeId, HashMap<SlotName, tine_core::ArtifactKey>> =
            HashMap::new();

        for node_id in &topo {
            let cell = cell_by_node_id(branch, node_id);

            let upstream_dirty = cell
                .inputs
                .values()
                .any(|input| must_execute.contains(&NodeId::new(input.source_cell_id.as_str())));

            if upstream_dirty || !cell.cache {
                must_execute.insert(node_id.clone());
                to_execute.push(node_id.clone());
                continue;
            }

            let Some(input_hashes) = cache_input_hashes(branch, cell, &node_artifacts) else {
                must_execute.insert(node_id.clone());
                to_execute.push(node_id.clone());
                continue;
            };

            let code_hash = NodeCacheKey::hash_code(&cell.code.source);
            let cache_key = NodeCacheKey {
                code_hash,
                input_hashes,
                lockfile_hash,
                scope_hash: NodeCacheKey::scope_for(branch.tree_id.as_str(), cell.cell_id.as_str()),
            };

            if let Some(artifacts) = cache.get(&cache_key) {
                node_artifacts.insert(node_id.clone(), artifacts.clone());
                to_skip.push((node_id.clone(), artifacts.clone()));
            } else {
                must_execute.insert(node_id.clone());
                to_execute.push(node_id.clone());
            }
        }

        (to_execute, to_skip)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Utc;
    use tine_core::{
        ExecutableTreeBranch, ExecutableTreeCell, ExecutableTreeInput, ExecutionMode, NodeCacheKey,
        NodeCode, SlotName,
    };

    use super::{cache_input_hashes, ExecutableTreeGraph};

    fn make_cell(id: &str, inputs: &[(&str, &str, &str)], outputs: &[&str]) -> ExecutableTreeCell {
        let mut input_map = HashMap::new();
        for (slot, source_cell_id, source_output) in inputs {
            input_map.insert(
                SlotName::new(*slot),
                ExecutableTreeInput {
                    source_cell_id: tine_core::CellId::new(*source_cell_id),
                    source_output: SlotName::new(*source_output),
                },
            );
        }

        ExecutableTreeCell {
            tree_id: tine_core::ExperimentTreeId::new("tree-1"),
            branch_id: tine_core::BranchId::new("main"),
            cell_id: tine_core::CellId::new(id),
            name: id.to_string(),
            code: NodeCode {
                source: format!("# {}", id),
                language: "python".to_string(),
            },
            inputs: input_map,
            outputs: outputs.iter().map(|slot| SlotName::new(*slot)).collect(),
            cache: true,
            map_over: None,
            map_concurrency: None,
            tags: HashMap::new(),
            revision_id: None,
        }
    }

    fn make_linear_branch() -> ExecutableTreeBranch {
        ExecutableTreeBranch {
            tree_id: tine_core::ExperimentTreeId::new("tree-1"),
            branch_id: tine_core::BranchId::new("main"),
            name: "main".to_string(),
            lineage: vec![tine_core::BranchId::new("main")],
            path_cell_order: vec![
                tine_core::CellId::new("A"),
                tine_core::CellId::new("B"),
                tine_core::CellId::new("C"),
            ],
            topo_order: vec![
                tine_core::CellId::new("A"),
                tine_core::CellId::new("B"),
                tine_core::CellId::new("C"),
            ],
            cells: vec![
                make_cell("A", &[], &["out"]),
                make_cell("B", &[("in", "A", "out")], &["out"]),
                make_cell("C", &[("in", "B", "out")], &["out"]),
            ],
            environment: tine_core::EnvironmentSpec::default(),
            execution_mode: ExecutionMode::Parallel,
            budget: None,
            project_id: None,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn linear_branch_orders_cells_topologically() {
        let branch = make_linear_branch();
        let graph = ExecutableTreeGraph::from_branch(&branch).unwrap();
        graph.validate("tree-1::main").unwrap();

        let sorted = graph.topo_sort();
        let pos_a = sorted.iter().position(|n| n.as_str() == "A").unwrap();
        let pos_b = sorted.iter().position(|n| n.as_str() == "B").unwrap();
        let pos_c = sorted.iter().position(|n| n.as_str() == "C").unwrap();
        assert!(pos_a < pos_b);
        assert!(pos_b < pos_c);
    }

    #[test]
    fn plan_execution_skips_cached_runtime_cells() {
        let branch = make_linear_branch();
        let graph = ExecutableTreeGraph::from_branch(&branch).unwrap();

        let cache_key = NodeCacheKey {
            code_hash: NodeCacheKey::hash_code("# A"),
            input_hashes: HashMap::new(),
            lockfile_hash: [1; 32],
            scope_hash: NodeCacheKey::scope_for("tree-1", "A"),
        };
        let mut cache = HashMap::new();
        cache.insert(
            cache_key,
            HashMap::from([(
                SlotName::new("out"),
                tine_core::ArtifactKey::new("artifact-a"),
            )]),
        );

        let (to_execute, to_skip) = graph.plan_execution(&branch, &cache, [1; 32]);
        assert!(to_skip.iter().any(|(node, _)| node.as_str() == "A"));
        assert!(to_execute.iter().any(|node| node.as_str() == "B"));
    }

    fn artifacts(pairs: &[(&str, &str)]) -> HashMap<SlotName, tine_core::ArtifactKey> {
        pairs
            .iter()
            .map(|(slot, key)| (SlotName::new(*slot), tine_core::ArtifactKey::new(*key)))
            .collect()
    }

    /// A two-cell branch where the upstream produces two output slots.
    fn make_multi_output_branch() -> ExecutableTreeBranch {
        ExecutableTreeBranch {
            tree_id: tine_core::ExperimentTreeId::new("tree-1"),
            branch_id: tine_core::BranchId::new("main"),
            name: "main".to_string(),
            lineage: vec![tine_core::BranchId::new("main")],
            path_cell_order: vec![tine_core::CellId::new("A"), tine_core::CellId::new("B")],
            topo_order: vec![tine_core::CellId::new("A"), tine_core::CellId::new("B")],
            cells: vec![
                make_cell("A", &[], &["left", "right"]),
                make_cell("B", &[("in", "A", "left")], &["out"]),
            ],
            environment: tine_core::EnvironmentSpec::default(),
            execution_mode: ExecutionMode::Parallel,
            budget: None,
            project_id: None,
            created_at: Utc::now(),
        }
    }

    fn cache_key_for(
        branch: &ExecutableTreeBranch,
        cell_id: &str,
        upstream_artifacts: &HashMap<tine_core::NodeId, HashMap<SlotName, tine_core::ArtifactKey>>,
        lockfile_hash: [u8; 32],
    ) -> NodeCacheKey {
        let cell = branch
            .cells
            .iter()
            .find(|cell| cell.cell_id.as_str() == cell_id)
            .unwrap();
        NodeCacheKey {
            code_hash: NodeCacheKey::hash_code(&cell.code.source),
            input_hashes: cache_input_hashes(branch, cell, upstream_artifacts).unwrap(),
            lockfile_hash,
            scope_hash: NodeCacheKey::scope_for(branch.tree_id.as_str(), cell_id),
        }
    }

    #[test]
    fn plan_execution_hit_is_deterministic_with_multi_output_upstream() {
        let branch = make_multi_output_branch();
        let graph = ExecutableTreeGraph::from_branch(&branch).unwrap();
        let lockfile_hash = [1; 32];

        let a_artifacts = artifacts(&[("left", "artifact-left"), ("right", "artifact-right")]);
        let mut upstream = HashMap::new();
        upstream.insert(tine_core::NodeId::new("A"), a_artifacts.clone());

        let mut cache = HashMap::new();
        cache.insert(
            cache_key_for(&branch, "A", &HashMap::new(), lockfile_hash),
            a_artifacts,
        );
        cache.insert(
            cache_key_for(&branch, "B", &upstream, lockfile_hash),
            artifacts(&[("out", "artifact-b")]),
        );

        // The old key computation iterated upstream artifacts in HashMap
        // order, so multi-output upstreams produced flaky keys. Plan
        // repeatedly to guard against any order-dependence sneaking back in.
        for _ in 0..16 {
            let (to_execute, to_skip) = graph.plan_execution(&branch, &cache, lockfile_hash);
            assert!(to_execute.is_empty(), "expected full cache hit");
            let b_artifacts = &to_skip
                .iter()
                .find(|(node, _)| node.as_str() == "B")
                .expect("B should be skipped")
                .1;
            assert_eq!(
                b_artifacts.get(&SlotName::new("out")),
                Some(&tine_core::ArtifactKey::new("artifact-b")),
                "skipped node must carry the artifacts of the matched entry"
            );
        }
    }

    #[test]
    fn plan_execution_misses_when_any_upstream_output_changes() {
        let branch = make_multi_output_branch();
        let graph = ExecutableTreeGraph::from_branch(&branch).unwrap();
        let lockfile_hash = [1; 32];

        // B's entry was written when A produced (left, right-v1).
        let old_a_artifacts = artifacts(&[("left", "artifact-left"), ("right", "right-v1")]);
        let mut old_upstream = HashMap::new();
        old_upstream.insert(tine_core::NodeId::new("A"), old_a_artifacts);

        // A's current entry produces the same "left" but a different "right".
        // Even though B's projected input edge points at "left", a change in
        // any upstream output must invalidate B.
        let new_a_artifacts = artifacts(&[("left", "artifact-left"), ("right", "right-v2")]);

        let mut cache = HashMap::new();
        cache.insert(
            cache_key_for(&branch, "A", &HashMap::new(), lockfile_hash),
            new_a_artifacts,
        );
        cache.insert(
            cache_key_for(&branch, "B", &old_upstream, lockfile_hash),
            artifacts(&[("out", "artifact-b-stale")]),
        );

        let (to_execute, to_skip) = graph.plan_execution(&branch, &cache, lockfile_hash);
        assert!(to_skip.iter().any(|(node, _)| node.as_str() == "A"));
        assert!(
            to_execute.iter().any(|node| node.as_str() == "B"),
            "stale B entry must not be reused when an upstream output changed"
        );
    }

    #[test]
    fn plan_execution_executes_downstream_when_upstream_artifacts_incomplete() {
        let branch = make_multi_output_branch();
        let graph = ExecutableTreeGraph::from_branch(&branch).unwrap();
        let lockfile_hash = [1; 32];

        // A's cache entry is missing its declared "right" output, so B's
        // inputs cannot be fingerprinted and B must execute.
        let incomplete_a_artifacts = artifacts(&[("left", "artifact-left")]);
        let mut cache = HashMap::new();
        cache.insert(
            cache_key_for(&branch, "A", &HashMap::new(), lockfile_hash),
            incomplete_a_artifacts,
        );

        let (to_execute, _to_skip) = graph.plan_execution(&branch, &cache, lockfile_hash);
        assert!(to_execute.iter().any(|node| node.as_str() == "B"));
    }

    #[test]
    fn plan_execution_excludes_ordering_only_inputs_from_key() {
        // A declares no outputs; the edge A -> B is ordering-only and must
        // not block B's caching (A's code changes still dirty B at plan time).
        let branch = ExecutableTreeBranch {
            cells: vec![
                make_cell("A", &[], &[]),
                make_cell("B", &[("in", "A", "A")], &["out"]),
            ],
            ..make_multi_output_branch()
        };
        let graph = ExecutableTreeGraph::from_branch(&branch).unwrap();
        let lockfile_hash = [1; 32];

        let mut cache = HashMap::new();
        cache.insert(
            cache_key_for(&branch, "A", &HashMap::new(), lockfile_hash),
            HashMap::new(),
        );
        cache.insert(
            NodeCacheKey {
                code_hash: NodeCacheKey::hash_code("# B"),
                input_hashes: HashMap::new(),
                lockfile_hash,
                scope_hash: NodeCacheKey::scope_for("tree-1", "B"),
            },
            artifacts(&[("out", "artifact-b")]),
        );

        let (to_execute, to_skip) = graph.plan_execution(&branch, &cache, lockfile_hash);
        assert!(to_execute.is_empty());
        assert!(to_skip.iter().any(|(node, _)| node.as_str() == "B"));
    }

    #[test]
    fn plan_execution_never_hits_sideways_entries_from_other_cells() {
        // Two independent cells with byte-identical code: an entry produced
        // by one cell must never satisfy the other. Cache reuse is scoped
        // top-to-bottom (same cell across runs), not sideways across
        // logically distinct cells/branches that happen to share code.
        let identical = make_cell("A", &[], &["out"]);
        let mut sibling = make_cell("B", &[], &["out"]);
        sibling.code = identical.code.clone();
        let branch = ExecutableTreeBranch {
            path_cell_order: vec![tine_core::CellId::new("A"), tine_core::CellId::new("B")],
            topo_order: vec![tine_core::CellId::new("A"), tine_core::CellId::new("B")],
            cells: vec![identical, sibling],
            ..make_multi_output_branch()
        };
        let graph = ExecutableTreeGraph::from_branch(&branch).unwrap();
        let lockfile_hash = [1; 32];

        // Only cell A has a cache entry.
        let mut cache = HashMap::new();
        cache.insert(
            cache_key_for(&branch, "A", &HashMap::new(), lockfile_hash),
            artifacts(&[("out", "artifact-a")]),
        );

        let (to_execute, to_skip) = graph.plan_execution(&branch, &cache, lockfile_hash);
        assert!(to_skip.iter().any(|(node, _)| node.as_str() == "A"));
        assert!(
            to_execute.iter().any(|node| node.as_str() == "B"),
            "cell B shares A's code and inputs but must not reuse A's entry"
        );
    }
}
