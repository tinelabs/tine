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

    pub fn plan_execution(
        &self,
        branch: &ExecutableTreeBranch,
        cache: &HashMap<NodeCacheKey, HashMap<SlotName, tine_core::ArtifactKey>>,
        lockfile_hash: [u8; 32],
    ) -> (Vec<NodeId>, Vec<NodeId>) {
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

            let mut input_hashes: HashMap<SlotName, [u8; 32]> = HashMap::new();
            for (slot, input) in &cell.inputs {
                let source_node_id = NodeId::new(input.source_cell_id.as_str());
                if let Some(src_arts) = node_artifacts.get(&source_node_id) {
                    for artifact_key in src_arts.values() {
                        input_hashes
                            .insert(slot.clone(), NodeCacheKey::hash_code(artifact_key.as_str()));
                    }
                }
            }

            let code_hash = NodeCacheKey::hash_code(&cell.code.source);
            let cache_key = NodeCacheKey {
                code_hash,
                input_hashes,
                lockfile_hash,
            };

            if let Some(artifacts) = cache.get(&cache_key) {
                node_artifacts.insert(node_id.clone(), artifacts.clone());
                to_skip.push(node_id.clone());
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

    use super::ExecutableTreeGraph;

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
        assert!(to_skip.iter().any(|node| node.as_str() == "A"));
        assert!(to_execute.iter().any(|node| node.as_str() == "B"));
    }
}
