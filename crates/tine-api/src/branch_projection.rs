use std::collections::{HashMap, HashSet, VecDeque};

use tine_core::{
    BranchDef, BranchId, CellDef, CellId, ExecutableTreeBranch, ExecutableTreeCell,
    ExecutableTreeInput, ExperimentTreeDef, SlotName, TineError, TineResult, TreeKernelState,
    TreeRuntimeState,
};

#[derive(Debug, Clone)]
pub struct BranchProjection {
    pub tree_id: tine_core::ExperimentTreeId,
    pub branch_id: BranchId,
    pub lineage: Vec<BranchId>,
    pub path_cell_order: Vec<CellId>,
    pub topo_order: Vec<CellId>,
}

impl BranchProjection {
    pub fn from_tree(tree: &ExperimentTreeDef, branch_id: &BranchId) -> TineResult<Self> {
        let lineage = branch_lineage(tree, branch_id)?;
        let path_cell_order = path_cell_order(tree, &lineage)?;
        let topo_order = projected_topo_order(tree, branch_id, &path_cell_order)?;

        Ok(Self {
            tree_id: tree.id.clone(),
            branch_id: branch_id.clone(),
            lineage,
            path_cell_order,
            topo_order,
        })
    }

    pub fn to_executable_tree_branch(
        &self,
        tree: &ExperimentTreeDef,
    ) -> TineResult<ExecutableTreeBranch> {
        projected_tree_branch(
            tree,
            &self.branch_id,
            &self.lineage,
            &self.path_cell_order,
            &self.topo_order,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BranchTransitionPlan {
    pub target_branch_id: BranchId,
    pub target_cell_id: CellId,
    pub target_path_cell_ids: Vec<CellId>,
    pub shared_prefix_cell_ids: Vec<CellId>,
    pub divergence_cell_id: Option<CellId>,
    pub replay_from_idx: usize,
    pub replay_cell_ids: Vec<CellId>,
}

impl BranchTransitionPlan {
    pub fn replay_prefix_before_target(&self) -> TineResult<Vec<CellId>> {
        let target_idx = self
            .target_path_cell_ids
            .iter()
            .position(|cell_id| cell_id == &self.target_cell_id)
            .ok_or_else(|| {
                TineError::Internal(format!(
                    "target cell '{}' missing from transition path for branch '{}'",
                    self.target_cell_id, self.target_branch_id
                ))
            })?;
        if self.replay_from_idx > target_idx {
            return Err(TineError::Internal(format!(
                "transition replay index {} is beyond target cell '{}' for branch '{}'",
                self.replay_from_idx, self.target_cell_id, self.target_branch_id
            )));
        }
        Ok(self.replay_cell_ids.clone())
    }
}

pub fn plan_branch_transition(
    current_state: Option<&TreeRuntimeState>,
    target_branch_id: &BranchId,
    target_cell_id: &CellId,
    target_path_cell_ids: &[CellId],
) -> TineResult<BranchTransitionPlan> {
    let target_idx = target_path_cell_ids
        .iter()
        .position(|cell_id| cell_id == target_cell_id)
        .ok_or_else(|| {
            TineError::Internal(format!(
                "target cell '{}' missing from computed path for branch '{}'",
                target_cell_id, target_branch_id
            ))
        })?;

    let trusted_current_path = current_state
        .filter(|state| state.kernel_state == TreeKernelState::Ready)
        .map(|state| state.materialized_path_cell_ids.as_slice())
        .unwrap_or(&[]);

    let shared_prefix_len = trusted_current_path
        .iter()
        .zip(target_path_cell_ids.iter())
        .take_while(|(left, right)| left == right)
        .count();

    let can_resume_from_current =
        shared_prefix_len == trusted_current_path.len() && shared_prefix_len <= target_idx;
    let replay_from_idx = if can_resume_from_current {
        shared_prefix_len
    } else {
        0
    };
    let divergence_cell_id = target_path_cell_ids.get(replay_from_idx).cloned();
    let replay_cell_ids = target_path_cell_ids[replay_from_idx..target_idx].to_vec();

    Ok(BranchTransitionPlan {
        target_branch_id: target_branch_id.clone(),
        target_cell_id: target_cell_id.clone(),
        target_path_cell_ids: target_path_cell_ids.to_vec(),
        shared_prefix_cell_ids: target_path_cell_ids[..shared_prefix_len].to_vec(),
        divergence_cell_id,
        replay_from_idx,
        replay_cell_ids,
    })
}

pub fn branch_lineage(
    tree: &ExperimentTreeDef,
    target_branch_id: &BranchId,
) -> TineResult<Vec<BranchId>> {
    let branches_by_id = branch_map(tree);
    let mut lineage = Vec::new();
    let mut current_branch_id = target_branch_id.clone();

    loop {
        let branch = branches_by_id.get(&current_branch_id).ok_or_else(|| {
            TineError::Internal(format!(
                "branch '{}' not found in tree '{}'",
                current_branch_id, tree.id
            ))
        })?;

        lineage.push(branch.id.clone());
        match &branch.parent_branch_id {
            Some(parent_branch_id) => current_branch_id = parent_branch_id.clone(),
            None => break,
        }
    }

    lineage.reverse();
    Ok(lineage)
}

pub fn path_cell_order(tree: &ExperimentTreeDef, lineage: &[BranchId]) -> TineResult<Vec<CellId>> {
    if lineage.is_empty() {
        return Ok(Vec::new());
    }

    let branches_by_id = branch_map(tree);
    let mut ordered = Vec::new();

    for (index, branch_id) in lineage.iter().enumerate() {
        let branch = branches_by_id.get(branch_id).ok_or_else(|| {
            TineError::Internal(format!(
                "branch '{}' not found in tree '{}'",
                branch_id, tree.id
            ))
        })?;

        let next_branch = lineage
            .get(index + 1)
            .map(|next_branch_id| {
                branches_by_id.get(next_branch_id).ok_or_else(|| {
                    TineError::Internal(format!(
                        "branch '{}' not found in tree '{}'",
                        next_branch_id, tree.id
                    ))
                })
            })
            .transpose()?;

        let inclusive_end = next_branch
            .and_then(|branch| branch.branch_point_cell_id.as_ref())
            .map(|branch_point_cell_id| {
                branch
                    .cell_order
                    .iter()
                    .position(|cell_id| cell_id == branch_point_cell_id)
                    .map(|idx| idx + 1)
                    .ok_or_else(|| {
                        TineError::Internal(format!(
                            "branch point cell '{}' is not in parent branch '{}'",
                            branch_point_cell_id, branch.id
                        ))
                    })
            })
            .transpose()?;

        let cells = match inclusive_end {
            Some(end) => &branch.cell_order[..end],
            None => branch.cell_order.as_slice(),
        };

        ordered.extend(cells.iter().cloned());
    }

    Ok(ordered)
}

fn branch_runtime_id(tree: &ExperimentTreeDef, branch_id: &BranchId) -> String {
    format!("{}::{}", tree.id.as_str(), branch_id.as_str())
}

fn projected_topo_order(
    tree: &ExperimentTreeDef,
    branch_id: &BranchId,
    path_cell_order: &[CellId],
) -> TineResult<Vec<CellId>> {
    let cell_lookup = cell_map(tree);
    let runtime_id = branch_runtime_id(tree, branch_id);
    let mut path_cell_set = HashSet::with_capacity(path_cell_order.len());
    let mut indegree: HashMap<CellId, usize> = HashMap::with_capacity(path_cell_order.len());
    let mut adjacency: HashMap<CellId, Vec<CellId>> = HashMap::with_capacity(path_cell_order.len());

    for cell_id in path_cell_order {
        if !path_cell_set.insert(cell_id.clone()) {
            return Err(TineError::DuplicateNode {
                runtime_id: runtime_id.clone(),
                node_id: tine_core::NodeId::new(cell_id.as_str()),
            });
        }
        indegree.insert(cell_id.clone(), 0);
        adjacency.insert(cell_id.clone(), Vec::new());
    }

    for (index, cell_id) in path_cell_order.iter().enumerate() {
        let cell = cell_lookup.get(cell_id).ok_or_else(|| {
            TineError::Internal(format!(
                "cell '{}' not found in tree '{}'",
                cell_id, tree.id
            ))
        })?;
        for upstream_cell_id in &cell.upstream_cell_ids {
            if !path_cell_set.contains(upstream_cell_id) {
                continue;
            }

            adjacency
                .get_mut(upstream_cell_id)
                .ok_or_else(|| TineError::InvalidEdge {
                    from: tine_core::NodeId::new(upstream_cell_id.as_str()),
                    to: tine_core::NodeId::new(cell.id.as_str()),
                    slot: upstream_slot_name(cell),
                })?
                .push(cell.id.clone());
            *indegree
                .get_mut(&cell.id)
                .ok_or_else(|| TineError::InvalidEdge {
                    from: tine_core::NodeId::new(upstream_cell_id.as_str()),
                    to: tine_core::NodeId::new(cell.id.as_str()),
                    slot: upstream_slot_name(cell),
                })? += 1;
        }

        if let Some(previous_cell_id) = index
            .checked_sub(1)
            .and_then(|previous_index| path_cell_order.get(previous_index))
            .filter(|id| *id != &cell.id)
        {
            adjacency
                .get_mut(previous_cell_id)
                .ok_or_else(|| TineError::InvalidEdge {
                    from: tine_core::NodeId::new(previous_cell_id.as_str()),
                    to: tine_core::NodeId::new(cell.id.as_str()),
                    slot: upstream_slot_name(cell),
                })?
                .push(cell.id.clone());
            *indegree
                .get_mut(&cell.id)
                .ok_or_else(|| TineError::InvalidEdge {
                    from: tine_core::NodeId::new(previous_cell_id.as_str()),
                    to: tine_core::NodeId::new(cell.id.as_str()),
                    slot: upstream_slot_name(cell),
                })? += 1;
        }
    }

    let mut ready = VecDeque::new();
    for cell_id in path_cell_order {
        if indegree.get(cell_id).copied().unwrap_or_default() == 0 {
            ready.push_back(cell_id.clone());
        }
    }

    let mut topo_order = Vec::with_capacity(path_cell_order.len());
    while let Some(cell_id) = ready.pop_front() {
        topo_order.push(cell_id.clone());
        if let Some(successors) = adjacency.get(&cell_id) {
            for successor in successors {
                if let Some(degree) = indegree.get_mut(successor) {
                    *degree -= 1;
                    if *degree == 0 {
                        ready.push_back(successor.clone());
                    }
                }
            }
        }
    }

    if topo_order.len() != path_cell_order.len() {
        return Err(TineError::CycleDetected { runtime_id });
    }

    Ok(topo_order)
}

fn projected_tree_branch(
    tree: &ExperimentTreeDef,
    branch_id: &BranchId,
    lineage: &[BranchId],
    path_cell_order: &[CellId],
    topo_order: &[CellId],
) -> TineResult<ExecutableTreeBranch> {
    let cell_lookup = cell_map(tree);
    let path_cell_set: HashSet<CellId> = path_cell_order.iter().cloned().collect();
    let mut cells = Vec::with_capacity(path_cell_order.len());

    for (index, cell_id) in path_cell_order.iter().enumerate() {
        let cell = cell_lookup.get(cell_id).ok_or_else(|| {
            TineError::Internal(format!(
                "cell '{}' not found in tree '{}'",
                cell_id, tree.id
            ))
        })?;
        let previous_cell_id = index
            .checked_sub(1)
            .and_then(|previous_index| path_cell_order.get(previous_index));
        cells.push(executable_cell_from_cell(
            cell,
            &cell_lookup,
            branch_id,
            &path_cell_set,
            previous_cell_id,
        )?);
    }

    Ok(ExecutableTreeBranch {
        tree_id: tree.id.clone(),
        branch_id: branch_id.clone(),
        name: format!("{} [{}]", tree.name, branch_id),
        lineage: lineage.to_vec(),
        path_cell_order: path_cell_order.to_vec(),
        topo_order: topo_order.to_vec(),
        cells,
        environment: tree.environment.clone(),
        execution_mode: tree.execution_mode.clone(),
        budget: tree.budget.clone(),
        project_id: tree.project_id.clone(),
        created_at: tree.created_at,
    })
}

fn executable_cell_from_cell(
    cell: &CellDef,
    cell_lookup: &HashMap<CellId, &CellDef>,
    branch_id: &BranchId,
    path_cell_set: &HashSet<CellId>,
    previous_cell_id: Option<&CellId>,
) -> TineResult<ExecutableTreeCell> {
    let mut inputs = HashMap::new();
    for upstream_cell_id in &cell.upstream_cell_ids {
        if !path_cell_set.contains(upstream_cell_id) {
            continue;
        }

        let upstream_cell = cell_lookup.get(upstream_cell_id).ok_or_else(|| {
            TineError::Internal(format!(
                "upstream cell '{}' not found while projecting '{}'",
                upstream_cell_id, cell.id
            ))
        })?;
        let slot = upstream_slot_name(upstream_cell);
        inputs.insert(
            slot.clone(),
            ExecutableTreeInput {
                source_cell_id: upstream_cell_id.clone(),
                source_output: slot,
            },
        );
    }

    if let Some(previous_cell_id) = previous_cell_id.filter(|id| *id != &cell.id) {
        let previous_cell = cell_lookup.get(previous_cell_id).ok_or_else(|| {
            TineError::Internal(format!(
                "previous path cell '{}' not found while projecting '{}'",
                previous_cell_id, cell.id
            ))
        })?;
        let slot = upstream_slot_name(previous_cell);
        inputs
            .entry(slot.clone())
            .or_insert_with(|| ExecutableTreeInput {
                source_cell_id: previous_cell_id.clone(),
                source_output: slot,
            });
    }

    Ok(ExecutableTreeCell {
        tree_id: cell.tree_id.clone(),
        branch_id: branch_id.clone(),
        cell_id: cell.id.clone(),
        name: cell.name.clone(),
        code: cell.code.clone(),
        inputs,
        outputs: cell.declared_outputs.clone(),
        cache: cell.cache,
        map_over: cell.map_over.clone(),
        map_concurrency: cell.map_concurrency,
        tags: cell.tags.clone(),
        revision_id: cell.revision_id.clone(),
    })
}

fn upstream_slot_name(cell: &CellDef) -> SlotName {
    cell.declared_outputs
        .first()
        .cloned()
        .unwrap_or_else(|| SlotName::new(cell.id.as_str()))
}

fn branch_map(tree: &ExperimentTreeDef) -> HashMap<BranchId, &BranchDef> {
    tree.branches
        .iter()
        .map(|branch| (branch.id.clone(), branch))
        .collect()
}

fn cell_map(tree: &ExperimentTreeDef) -> HashMap<CellId, &CellDef> {
    tree.cells
        .iter()
        .map(|cell| (cell.id.clone(), cell))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tine_core::{
        BranchDef, BranchId, CellDef, CellId, CellRuntimeState, ExecutionMode, ExperimentTreeDef,
        ExperimentTreeId, NodeCode, SlotName, TineError, TreeKernelState, TreeRuntimeState,
    };

    use super::{branch_lineage, plan_branch_transition, BranchProjection};

    #[test]
    fn projects_root_branch_cells_in_order() {
        let tree = trivial_tree();
        let projection = BranchProjection::from_tree(&tree, &tree.root_branch_id).unwrap();

        assert_eq!(projection.lineage, vec![tree.root_branch_id.clone()]);
        assert_eq!(
            projection.path_cell_order,
            vec![
                CellId::new("step1"),
                CellId::new("step2"),
                CellId::new("step3")
            ]
        );
        assert_eq!(projection.topo_order, projection.path_cell_order);
    }

    #[test]
    fn projects_selected_branch_path_without_siblings() {
        let tree = nested_branch_tree();
        let projection = BranchProjection::from_tree(&tree, &BranchId::new("branch_b")).unwrap();

        assert_eq!(
            projection.lineage,
            vec![
                tree.root_branch_id.clone(),
                BranchId::new("branch_a"),
                BranchId::new("branch_b"),
            ]
        );
        assert_eq!(
            projection.path_cell_order,
            vec![
                CellId::new("step1"),
                CellId::new("step2"),
                CellId::new("branch_a_1"),
                CellId::new("branch_a_2"),
                CellId::new("branch_b_1"),
            ]
        );
        assert_eq!(projection.topo_order, projection.path_cell_order);
    }

    #[test]
    fn projects_executable_tree_branch_inputs_from_upstreams() {
        let tree = nested_branch_tree();
        let projection = BranchProjection::from_tree(&tree, &BranchId::new("branch_b")).unwrap();
        let branch = projection.to_executable_tree_branch(&tree).unwrap();

        assert_eq!(branch.tree_id, tree.id);
        assert_eq!(branch.branch_id, BranchId::new("branch_b"));
        assert_eq!(branch.path_cell_order, projection.path_cell_order);
        assert_eq!(branch.topo_order, projection.topo_order);

        let cell = branch
            .cells
            .iter()
            .find(|cell| cell.cell_id.as_str() == "branch_b_1")
            .unwrap();
        let input = cell.inputs.get(&SlotName::new("branch_a_2")).unwrap();
        assert_eq!(input.source_cell_id.as_str(), "branch_a_2");
        assert_eq!(input.source_output.as_str(), "branch_a_2");
        assert!(branch
            .cells
            .iter()
            .all(|cell| cell.cell_id.as_str() != "step3"));
    }

    #[test]
    fn returns_error_when_branch_point_is_not_in_parent_branch() {
        let mut tree = trivial_tree();
        tree.branches.push(BranchDef {
            id: BranchId::new("broken"),
            name: "broken".to_string(),
            parent_branch_id: Some(tree.root_branch_id.clone()),
            branch_point_cell_id: Some(CellId::new("missing")),
            cell_order: vec![CellId::new("branch_step")],
            display: HashMap::new(),
        });
        tree.cells.push(branch_cell(
            &tree.id,
            &BranchId::new("broken"),
            "branch_step",
            "branch_step",
            vec![CellId::new("step2")],
        ));

        let err = BranchProjection::from_tree(&tree, &BranchId::new("broken")).unwrap_err();
        assert!(format!("{err}").contains("branch point cell 'missing'"));
    }

    #[test]
    fn returns_cycle_detected_for_invalid_path_dependencies() {
        let mut tree = trivial_tree();
        tree.cells[0].upstream_cell_ids = vec![CellId::new("step2")];

        let err = BranchProjection::from_tree(&tree, &tree.root_branch_id).unwrap_err();
        assert!(matches!(err, TineError::CycleDetected { .. }));
    }

    #[test]
    fn finds_branch_lineage_from_root() {
        let tree = nested_branch_tree();
        let lineage = branch_lineage(&tree, &BranchId::new("branch_b")).unwrap();

        assert_eq!(
            lineage,
            vec![
                tree.root_branch_id.clone(),
                BranchId::new("branch_a"),
                BranchId::new("branch_b"),
            ]
        );
    }

    #[test]
    fn transition_plan_finds_shared_prefix_for_ready_runtime() {
        let tree = nested_branch_tree();
        let branch_id = BranchId::new("branch_b");
        let target_cell_id = CellId::new("branch_b_1");
        let projection = BranchProjection::from_tree(&tree, &branch_id).unwrap();
        let current_state = TreeRuntimeState {
            tree_id: tree.id.clone(),
            active_branch_id: BranchId::new("branch_a"),
            materialized_path_cell_ids: vec![
                CellId::new("step1"),
                CellId::new("step2"),
                CellId::new("branch_a_1"),
            ],
            runtime_epoch: 1,
            kernel_state: TreeKernelState::Ready,
            last_prepared_cell_id: Some(CellId::new("branch_a_1")),
            isolation_mode: tine_core::BranchIsolationMode::Disabled,
            last_isolation_result: None,
        };

        let plan = plan_branch_transition(
            Some(&current_state),
            &branch_id,
            &target_cell_id,
            &projection.path_cell_order,
        )
        .unwrap();

        assert_eq!(
            plan.shared_prefix_cell_ids,
            vec![
                CellId::new("step1"),
                CellId::new("step2"),
                CellId::new("branch_a_1"),
            ]
        );
        assert_eq!(plan.replay_from_idx, 3);
        assert_eq!(plan.divergence_cell_id, Some(CellId::new("branch_a_2")));
        assert_eq!(
            plan.replay_prefix_before_target().unwrap(),
            vec![CellId::new("branch_a_2")]
        );
    }

    #[test]
    fn transition_plan_ignores_untrusted_runtime_state() {
        let tree = nested_branch_tree();
        let branch_id = BranchId::new("branch_b");
        let target_cell_id = CellId::new("branch_b_1");
        let projection = BranchProjection::from_tree(&tree, &branch_id).unwrap();
        let current_state = TreeRuntimeState {
            tree_id: tree.id.clone(),
            active_branch_id: BranchId::new("branch_a"),
            materialized_path_cell_ids: vec![
                CellId::new("step1"),
                CellId::new("step2"),
                CellId::new("branch_a_1"),
            ],
            runtime_epoch: 2,
            kernel_state: TreeKernelState::NeedsReplay,
            last_prepared_cell_id: Some(CellId::new("branch_a_1")),
            isolation_mode: tine_core::BranchIsolationMode::Disabled,
            last_isolation_result: None,
        };

        let plan = plan_branch_transition(
            Some(&current_state),
            &branch_id,
            &target_cell_id,
            &projection.path_cell_order,
        )
        .unwrap();

        assert!(plan.shared_prefix_cell_ids.is_empty());
        assert_eq!(plan.divergence_cell_id, Some(CellId::new("step1")));
        assert_eq!(
            plan.replay_prefix_before_target().unwrap(),
            vec![
                CellId::new("step1"),
                CellId::new("step2"),
                CellId::new("branch_a_1"),
                CellId::new("branch_a_2"),
            ]
        );
    }

    #[test]
    fn transition_plan_restarts_when_ready_runtime_diverged_from_target_branch() {
        let tree = nested_branch_tree();
        let branch_id = BranchId::new("branch_a");
        let target_cell_id = CellId::new("branch_a_1");
        let projection = BranchProjection::from_tree(&tree, &branch_id).unwrap();
        let current_state = TreeRuntimeState {
            tree_id: tree.id.clone(),
            active_branch_id: tree.root_branch_id.clone(),
            materialized_path_cell_ids: vec![
                CellId::new("step1"),
                CellId::new("step2"),
                CellId::new("step3"),
            ],
            runtime_epoch: 3,
            kernel_state: TreeKernelState::Ready,
            last_prepared_cell_id: Some(CellId::new("step3")),
            isolation_mode: tine_core::BranchIsolationMode::Disabled,
            last_isolation_result: None,
        };

        let plan = plan_branch_transition(
            Some(&current_state),
            &branch_id,
            &target_cell_id,
            &projection.path_cell_order,
        )
        .unwrap();

        assert_eq!(
            plan.shared_prefix_cell_ids,
            vec![CellId::new("step1"), CellId::new("step2")]
        );
        assert_eq!(plan.replay_from_idx, 0);
        assert_eq!(plan.divergence_cell_id, Some(CellId::new("step1")));
        assert_eq!(
            plan.replay_prefix_before_target().unwrap(),
            vec![CellId::new("step1"), CellId::new("step2")]
        );
    }

    #[test]
    fn transition_plan_restarts_when_rerunning_earlier_cell_on_same_branch() {
        let tree = nested_branch_tree();
        let branch_id = BranchId::new("branch_a");
        let target_cell_id = CellId::new("branch_a_1");
        let projection = BranchProjection::from_tree(&tree, &branch_id).unwrap();
        let current_state = TreeRuntimeState {
            tree_id: tree.id.clone(),
            active_branch_id: branch_id.clone(),
            materialized_path_cell_ids: projection.path_cell_order.clone(),
            runtime_epoch: 4,
            kernel_state: TreeKernelState::Ready,
            last_prepared_cell_id: Some(CellId::new("branch_a_2")),
            isolation_mode: tine_core::BranchIsolationMode::Disabled,
            last_isolation_result: None,
        };

        let plan = plan_branch_transition(
            Some(&current_state),
            &branch_id,
            &target_cell_id,
            &projection.path_cell_order,
        )
        .unwrap();

        assert_eq!(
            plan.shared_prefix_cell_ids, projection.path_cell_order,
            "full-path match should be treated as stale when the target cell is earlier",
        );
        assert_eq!(plan.replay_from_idx, 0);
        assert_eq!(plan.divergence_cell_id, Some(CellId::new("step1")));
        assert_eq!(
            plan.replay_prefix_before_target().unwrap(),
            vec![CellId::new("step1"), CellId::new("step2")]
        );
    }

    fn nested_branch_tree() -> tine_core::ExperimentTreeDef {
        let mut tree = trivial_tree();
        let root_branch_id = tree.root_branch_id.clone();

        tree.branches.push(BranchDef {
            id: BranchId::new("branch_a"),
            name: "branch_a".to_string(),
            parent_branch_id: Some(root_branch_id.clone()),
            branch_point_cell_id: Some(CellId::new("step2")),
            cell_order: vec![CellId::new("branch_a_1"), CellId::new("branch_a_2")],
            display: HashMap::new(),
        });
        tree.cells.push(branch_cell(
            &tree.id,
            &BranchId::new("branch_a"),
            "branch_a_1",
            "branch_a_1",
            vec![CellId::new("step2")],
        ));
        tree.cells.push(branch_cell(
            &tree.id,
            &BranchId::new("branch_a"),
            "branch_a_2",
            "branch_a_2",
            vec![CellId::new("branch_a_1")],
        ));

        tree.branches.push(BranchDef {
            id: BranchId::new("branch_b"),
            name: "branch_b".to_string(),
            parent_branch_id: Some(BranchId::new("branch_a")),
            branch_point_cell_id: Some(CellId::new("branch_a_2")),
            cell_order: vec![CellId::new("branch_b_1")],
            display: HashMap::new(),
        });
        tree.cells.push(branch_cell(
            &tree.id,
            &BranchId::new("branch_b"),
            "branch_b_1",
            "branch_b_1",
            vec![CellId::new("branch_a_2")],
        ));

        tree
    }

    fn branch_cell(
        tree_id: &tine_core::ExperimentTreeId,
        branch_id: &BranchId,
        id: &str,
        output: &str,
        upstream_cell_ids: Vec<CellId>,
    ) -> CellDef {
        CellDef {
            id: CellId::new(id),
            tree_id: tree_id.clone(),
            branch_id: branch_id.clone(),
            name: id.to_string(),
            code: NodeCode {
                source: format!("{output} = 1"),
                language: "python".to_string(),
            },
            upstream_cell_ids,
            declared_outputs: vec![SlotName::new(output)],
            cache: false,
            map_over: None,
            map_concurrency: None,
            tags: HashMap::new(),
            revision_id: None,
            state: CellRuntimeState::Clean,
        }
    }

    fn trivial_tree() -> ExperimentTreeDef {
        let tree_id = ExperimentTreeId::new("trivial");
        let branch_id = BranchId::new("main");
        let cell_ids = ["step1", "step2", "step3"];
        let cells: Vec<CellDef> = cell_ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                let upstream = if i == 0 {
                    vec![]
                } else {
                    vec![CellId::new(cell_ids[i - 1])]
                };
                CellDef {
                    id: CellId::new(id),
                    tree_id: tree_id.clone(),
                    branch_id: branch_id.clone(),
                    name: id.to_string(),
                    code: NodeCode {
                        source: format!("{id} = 1"),
                        language: "python".to_string(),
                    },
                    upstream_cell_ids: upstream,
                    declared_outputs: vec![SlotName::new(id)],
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                }
            })
            .collect();
        ExperimentTreeDef {
            id: tree_id.clone(),
            name: "trivial-test".to_string(),
            project_id: None,
            root_branch_id: branch_id.clone(),
            branches: vec![BranchDef {
                id: branch_id.clone(),
                name: "main".to_string(),
                parent_branch_id: None,
                branch_point_cell_id: None,
                cell_order: cell_ids.iter().map(|&id| CellId::new(id)).collect(),
                display: HashMap::new(),
            }],
            cells,
            environment: Default::default(),
            execution_mode: ExecutionMode::Parallel,
            budget: None,
            created_at: chrono::Utc::now(),
        }
    }
}
