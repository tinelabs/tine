use crate::branch_projection::BranchProjection;
use tine_core::{BranchId, ExperimentTreeDef, TineError, TineResult};

fn branch_display_name(tree: &ExperimentTreeDef, branch_id: &BranchId) -> String {
    tree.branches
        .iter()
        .find(|branch| &branch.id == branch_id)
        .map(|branch| branch.name.clone())
        .unwrap_or_else(|| branch_id.as_str().to_string())
}

fn branch_cells_in_path<'a>(
    tree: &'a ExperimentTreeDef,
    branch_id: &BranchId,
) -> TineResult<(BranchProjection, Vec<&'a tine_core::CellDef>)> {
    let projection = BranchProjection::from_tree(tree, branch_id)?;
    let cells = projection
        .path_cell_order
        .iter()
        .map(|cell_id| {
            tree.cells
                .iter()
                .find(|cell| cell.id == *cell_id)
                .ok_or_else(|| {
                    TineError::Internal(format!(
                        "cell '{}' missing from tree '{}' during export",
                        cell_id, tree.id
                    ))
                })
        })
        .collect::<TineResult<Vec<_>>>()?;
    Ok((projection, cells))
}

pub fn export_branch_as_python(
    tree: &ExperimentTreeDef,
    branch_id: &BranchId,
) -> TineResult<String> {
    let (projection, cells) = branch_cells_in_path(tree, branch_id)?;
    let lineage = projection
        .lineage
        .iter()
        .map(|id| id.as_str())
        .collect::<Vec<_>>()
        .join(" -> ");

    let mut out = String::new();
    out.push_str(&format!(
        "# Exported from tine\n# tree: {}\n# branch: {}\n# lineage: {}\n\n",
        tree.name,
        branch_display_name(tree, branch_id),
        lineage
    ));

    for (index, cell) in cells.iter().enumerate() {
        if index > 0 {
            out.push('\n');
        }
        out.push_str(&format!("# %% [{}] {}\n", cell.id.as_str(), cell.name));
        out.push_str(&cell.code.source);
        if !cell.code.source.ends_with('\n') {
            out.push('\n');
        }
    }

    Ok(out)
}

fn json_string_lines(source: &str) -> Vec<String> {
    let mut lines = source
        .lines()
        .map(|line| format!("{line}\n"))
        .collect::<Vec<_>>();
    if lines.is_empty() {
        lines.push(String::new());
    }
    lines
}

pub fn export_branch_as_ipynb(
    tree: &ExperimentTreeDef,
    branch_id: &BranchId,
) -> TineResult<serde_json::Value> {
    let (projection, cells) = branch_cells_in_path(tree, branch_id)?;
    let lineage = projection
        .lineage
        .iter()
        .map(|id| id.as_str().to_string())
        .collect::<Vec<_>>();

    let notebook_cells = cells
        .into_iter()
        .map(|cell| {
            serde_json::json!({
                "cell_type": "code",
                "execution_count": serde_json::Value::Null,
                "metadata": {
                    "tine": {
                        "cell_id": cell.id.as_str(),
                        "branch_id": cell.branch_id.as_str(),
                        "name": cell.name,
                    }
                },
                "outputs": [],
                "source": json_string_lines(&cell.code.source),
            })
        })
        .collect::<Vec<_>>();

    Ok(serde_json::json!({
        "cells": notebook_cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "name": "python"
            },
            "tine": {
                "tree_id": tree.id.as_str(),
                "tree_name": tree.name.as_str(),
                "branch_id": branch_id.as_str(),
                "branch_name": branch_display_name(tree, branch_id),
                "lineage": lineage,
                "dependencies": &tree.environment.dependencies,
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tine_core::{
        BranchDef, CellDef, CellId, CellRuntimeState, EnvironmentSpec, ExecutionMode,
        ExperimentTreeDef, ExperimentTreeId, NodeCode, SlotName,
    };

    fn test_tree() -> ExperimentTreeDef {
        let tree_id = ExperimentTreeId::new("tree_export");
        ExperimentTreeDef {
            id: tree_id.clone(),
            name: "Export tree".to_string(),
            project_id: None,
            root_branch_id: BranchId::new("main"),
            branches: vec![
                BranchDef {
                    id: BranchId::new("main"),
                    name: "Main".to_string(),
                    parent_branch_id: None,
                    branch_point_cell_id: None,
                    cell_order: vec![CellId::new("cell_1"), CellId::new("cell_2")],
                    display: HashMap::new(),
                },
                BranchDef {
                    id: BranchId::new("branch_a"),
                    name: "Branch A".to_string(),
                    parent_branch_id: Some(BranchId::new("main")),
                    branch_point_cell_id: Some(CellId::new("cell_2")),
                    cell_order: vec![CellId::new("branch_cell_1")],
                    display: HashMap::new(),
                },
            ],
            cells: vec![
                CellDef {
                    id: CellId::new("cell_1"),
                    tree_id: tree_id.clone(),
                    branch_id: BranchId::new("main"),
                    name: "Cell 1".to_string(),
                    code: NodeCode {
                        source: "a = 1\n".to_string(),
                        language: "python".to_string(),
                    },
                    upstream_cell_ids: vec![],
                    declared_outputs: vec![SlotName::new("a")],
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                },
                CellDef {
                    id: CellId::new("cell_2"),
                    tree_id: tree_id.clone(),
                    branch_id: BranchId::new("main"),
                    name: "Cell 2".to_string(),
                    code: NodeCode {
                        source: "b = a + 1\n".to_string(),
                        language: "python".to_string(),
                    },
                    upstream_cell_ids: vec![CellId::new("cell_1")],
                    declared_outputs: vec![SlotName::new("b")],
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                },
                CellDef {
                    id: CellId::new("branch_cell_1"),
                    tree_id: tree_id.clone(),
                    branch_id: BranchId::new("branch_a"),
                    name: "Branch Cell".to_string(),
                    code: NodeCode {
                        source: "c = b + 1\n".to_string(),
                        language: "python".to_string(),
                    },
                    upstream_cell_ids: vec![CellId::new("cell_2")],
                    declared_outputs: vec![SlotName::new("c")],
                    cache: false,
                    map_over: None,
                    map_concurrency: None,
                    tags: HashMap::new(),
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                },
            ],
            environment: EnvironmentSpec {
                dependencies: vec!["httpx".to_string()],
            },
            execution_mode: ExecutionMode::Parallel,
            budget: None,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn python_export_uses_branch_path_order() {
        let tree = test_tree();
        let exported = export_branch_as_python(&tree, &BranchId::new("branch_a")).unwrap();
        assert!(exported.contains("# %% [cell_1] Cell 1"));
        assert!(exported.contains("# %% [cell_2] Cell 2"));
        assert!(exported.contains("# %% [branch_cell_1] Branch Cell"));
        assert!(exported.contains("lineage: main -> branch_a"));
    }

    #[test]
    fn ipynb_export_includes_branch_metadata() {
        let tree = test_tree();
        let exported = export_branch_as_ipynb(&tree, &BranchId::new("branch_a")).unwrap();
        assert_eq!(exported["nbformat"], 4);
        assert_eq!(exported["cells"].as_array().unwrap().len(), 3);
        assert_eq!(exported["metadata"]["tine"]["branch_id"], "branch_a");
        assert_eq!(exported["metadata"]["tine"]["dependencies"][0], "httpx");
    }
}
