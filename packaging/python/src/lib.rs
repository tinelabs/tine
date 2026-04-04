use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use tine_api::Workspace;
use tine_core::{
    ArtifactKey, ArtifactMetadata, ArtifactStore, BranchDef, BranchId, CellDef, CellId,
    CellRuntimeState, EnvironmentSpec, ExecutionId, ExecutionStatus, ExperimentTreeDef,
    ExperimentTreeId, NodeCode, NodeDef, NodeId, NodeLogs, SlotName, TineError, TineResult,
    WorkspaceApi,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn to_pyerr(e: TineError) -> PyErr {
    PyRuntimeError::new_err(format!("{e}"))
}

/// A shared tokio runtime for blocking on async calls from Python.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime")
    })
}

// ---------------------------------------------------------------------------
// Local artifact store (used as default when opening a workspace from Python)
// ---------------------------------------------------------------------------

struct LocalArtifactStore {
    root: PathBuf,
}

impl LocalArtifactStore {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn path_for(&self, key: &ArtifactKey) -> PathBuf {
        self.root.join(key.as_str())
    }
}

#[async_trait::async_trait]
impl ArtifactStore for LocalArtifactStore {
    async fn put(&self, key: &ArtifactKey, data: &[u8]) -> TineResult<[u8; 32]> {
        let path = self.path_for(key);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&path, data).await?;
        Ok(*blake3::hash(data).as_bytes())
    }

    async fn get(&self, key: &ArtifactKey) -> TineResult<Vec<u8>> {
        let path = self.path_for(key);
        Ok(tokio::fs::read(&path).await?)
    }

    async fn delete(&self, key: &ArtifactKey) -> TineResult<()> {
        let path = self.path_for(key);
        if path.exists() {
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }

    async fn exists(&self, key: &ArtifactKey) -> TineResult<bool> {
        Ok(self.path_for(key).exists())
    }

    async fn metadata(&self, key: &ArtifactKey) -> TineResult<ArtifactMetadata> {
        let path = self.path_for(key);
        let meta = tokio::fs::metadata(&path).await?;
        Ok(ArtifactMetadata {
            key: key.clone(),
            size_bytes: meta.len(),
            schema: None,
            content_hash: [0u8; 32],
            created_at: chrono::Utc::now(),
        })
    }

    async fn list(&self) -> TineResult<Vec<ArtifactKey>> {
        let mut keys = Vec::new();
        if self.root.exists() {
            let mut entries = tokio::fs::read_dir(&self.root).await?;
            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str() {
                    keys.push(ArtifactKey::new(name));
                }
            }
        }
        Ok(keys)
    }
}

// ---------------------------------------------------------------------------
// PyWorkspace — the main entry point
// ---------------------------------------------------------------------------

/// A tine workspace backed by SQLite.
///
/// Usage::
///
///     import tine
///     ws = tine.Workspace("/path/to/workspace")
///     eid = ws.create_experiment("train", nodes)
///     run = ws.execute_branch(eid)
///     print(ws.status(eid))
#[pyclass(name = "Workspace")]
pub struct PyWorkspace {
    inner: Arc<Workspace>,
}

impl PyWorkspace {
    fn main_branch_id() -> BranchId {
        BranchId::new("main")
    }
}

#[pymethods]
impl PyWorkspace {
    /// Open or create a workspace rooted at `path`.
    #[new]
    #[pyo3(signature = (path = ".", max_kernels = 4))]
    fn new(path: &str, max_kernels: usize) -> PyResult<Self> {
        let root = PathBuf::from(path);
        let artifact_dir = root.join(".tine").join("artifacts");
        let store: Arc<dyn ArtifactStore> = Arc::new(LocalArtifactStore::new(artifact_dir));
        let ws = runtime()
            .block_on(Workspace::open(root, store, max_kernels))
            .map_err(to_pyerr)?;
        Ok(Self {
            inner: Arc::new(ws),
        })
    }

    // -- Experiment management ----------------------------------------------

    /// Create a new experiment tree from a list of node dicts.
    ///
    /// Each node dict should have: id, name, code, inputs (optional),
    /// outputs (optional), map_over (optional), map_concurrency (optional).
    ///
    /// `env` is an optional dict with a `dependencies` list.
    ///
    /// Returns the ExperimentTreeId as a string.
    #[pyo3(signature = (name, nodes, env = None))]
    fn create_experiment(
        &self,
        name: &str,
        nodes: &Bound<'_, PyList>,
        env: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let node_defs = py_list_to_node_defs(nodes)?;
        let environment = match env {
            Some(d) => {
                let dependencies: Vec<String> = d
                    .get_item("dependencies")?
                    .map(|v| v.extract())
                    .transpose()?
                    .unwrap_or_default();
                EnvironmentSpec { dependencies }
            }
            None => Default::default(),
        };
        let tree_id = tine_core::ExperimentTreeId::generate();
        let root_branch_id = Self::main_branch_id();
        let cells: Vec<CellDef> = node_defs
            .into_iter()
            .map(|node| {
                let upstream_cell_ids = node
                    .inputs
                    .values()
                    .map(|(upstream, _)| CellId::new(upstream.as_str()))
                    .collect();
                CellDef {
                    id: CellId::new(node.id.as_str()),
                    tree_id: tree_id.clone(),
                    branch_id: root_branch_id.clone(),
                    name: node.name,
                    code: node.code,
                    upstream_cell_ids,
                    declared_outputs: node.outputs,
                    cache: node.cache,
                    map_over: node.map_over,
                    map_concurrency: node.map_concurrency,
                    timeout_secs: node.timeout_secs,
                    tags: node.tags,
                    revision_id: None,
                    state: CellRuntimeState::Clean,
                }
            })
            .collect();
        let tree = ExperimentTreeDef {
            id: tree_id.clone(),
            name: name.to_string(),
            project_id: None,
            root_branch_id: root_branch_id.clone(),
            branches: vec![BranchDef {
                id: root_branch_id.clone(),
                name: "main".to_string(),
                parent_branch_id: None,
                branch_point_cell_id: None,
                cell_order: cells.iter().map(|cell: &CellDef| cell.id.clone()).collect(),
                display: HashMap::new(),
            }],
            cells,
            environment,
            execution_mode: Default::default(),
            budget: None,
            created_at: chrono::Utc::now(),
        };
        let pid = tree.id.clone();
        runtime()
            .block_on(self.inner.save_experiment_tree(&tree))
            .map_err(to_pyerr)?;
        Ok(pid.to_string())
    }

    /// Clone an existing experiment tree, optionally replacing cell source code.
    ///
    /// `replacements` is a dict mapping cell_id -> new source code.
    #[pyo3(signature = (source_id, new_name, replacements = None))]
    fn clone_experiment(
        &self,
        source_id: &str,
        new_name: &str,
        replacements: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<String> {
        let pid = tine_core::ExperimentTreeId::new(source_id);

        let mut tree = runtime()
            .block_on(self.inner.get_experiment_tree(&pid))
            .map_err(to_pyerr)?;
        tree.id = tine_core::ExperimentTreeId::generate();
        tree.name = new_name.to_string();
        let new_tree_id = tree.id.clone();

        if let Some(rep) = replacements {
            for (key, val) in rep.iter() {
                let cell_id: String = key.extract()?;
                let new_code: String = val.extract()?;
                let cell = tree
                    .cells
                    .iter_mut()
                    .find(|cell| cell.id.as_str() == cell_id)
                    .ok_or_else(|| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "cell '{}' not found in source experiment",
                            cell_id
                        ))
                    })?;
                cell.code = NodeCode {
                    language: "python".into(),
                    source: new_code,
                };
            }
        }
        for cell in &mut tree.cells {
            cell.tree_id = new_tree_id.clone();
        }
        let saved_tree = runtime()
            .block_on(self.inner.save_experiment_tree(&tree))
            .map_err(to_pyerr)?;
        Ok(saved_tree.id.to_string())
    }

    fn get_experiment(&self, experiment_id: &str) -> PyResult<String> {
        let tree = runtime()
            .block_on(
                self.inner
                    .get_experiment_tree(&ExperimentTreeId::new(experiment_id)),
            )
            .map_err(to_pyerr)?;
        serde_json::to_string(&tree).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    fn rename_experiment(&self, experiment_id: &str, name: &str) -> PyResult<()> {
        runtime()
            .block_on(
                self.inner
                    .rename_experiment_tree(&ExperimentTreeId::new(experiment_id), name),
            )
            .map_err(to_pyerr)
    }

    fn delete_experiment(&self, experiment_id: &str) -> PyResult<()> {
        runtime()
            .block_on(
                self.inner
                    .delete_experiment_tree(&ExperimentTreeId::new(experiment_id)),
            )
            .map_err(to_pyerr)
    }

    fn list_experiments(&self) -> PyResult<Vec<String>> {
        let defs = runtime()
            .block_on(self.inner.list_experiment_trees())
            .map_err(to_pyerr)?;
        Ok(defs.into_iter().map(|d| d.id.to_string()).collect())
    }

    // -- Execution -----------------------------------------------------------

    /// Execute a branch. Returns the ExecutionId.
    #[pyo3(signature = (experiment_id, branch_id = None))]
    fn execute_branch(&self, experiment_id: &str, branch_id: Option<&str>) -> PyResult<String> {
        let tree_id = tine_core::ExperimentTreeId::new(experiment_id);
        let branch_id = branch_id
            .map(BranchId::new)
            .unwrap_or_else(Self::main_branch_id);
        let eid = runtime()
            .block_on(
                self.inner
                    .execute_branch_in_experiment_tree(&tree_id, &branch_id),
            )
            .map_err(to_pyerr)?;
        Ok(eid.to_string())
    }

    /// Execute every branch in an experiment tree.
    fn execute_all_branches(&self, experiment_id: &str) -> PyResult<Vec<String>> {
        let execution_ids = runtime()
            .block_on(
                self.inner
                    .execute_all_branches_in_experiment_tree(&ExperimentTreeId::new(experiment_id)),
            )
            .map_err(to_pyerr)?;
        Ok(execution_ids
            .into_iter()
            .map(|(_, execution_id)| execution_id.to_string())
            .collect())
    }

    /// Cancel a running execution.
    fn cancel(&self, execution_id: &str) -> PyResult<()> {
        let eid = ExecutionId::new(execution_id);
        runtime()
            .block_on(self.inner.cancel(&eid))
            .map_err(to_pyerr)
    }

    // -- Observation ---------------------------------------------------------

    /// Get execution status as a JSON string.
    fn status(&self, execution_id: &str) -> PyResult<String> {
        let eid = ExecutionId::new(execution_id);
        let status: ExecutionStatus = runtime()
            .block_on(self.inner.status(&eid))
            .map_err(to_pyerr)?;
        serde_json::to_string(&status).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }

    /// Get cell logs as a JSON string.
    #[pyo3(signature = (experiment_id, cell_id, branch_id = None))]
    fn logs(
        &self,
        experiment_id: &str,
        cell_id: &str,
        branch_id: Option<&str>,
    ) -> PyResult<String> {
        let tree_id = tine_core::ExperimentTreeId::new(experiment_id);
        let branch_id = branch_id
            .map(BranchId::new)
            .unwrap_or_else(Self::main_branch_id);
        let cell_id = CellId::new(cell_id);
        let logs: NodeLogs = runtime()
            .block_on(
                self.inner
                    .logs_for_tree_cell(&tree_id, &branch_id, &cell_id),
            )
            .map_err(to_pyerr)?;
        serde_json::to_string(&logs).map_err(|e| PyRuntimeError::new_err(e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Helpers: Python dicts -> Rust types
// ---------------------------------------------------------------------------

fn py_list_to_node_defs(list: &Bound<'_, PyList>) -> PyResult<Vec<NodeDef>> {
    let mut defs = Vec::new();
    for item in list.iter() {
        let dict = item
            .downcast::<PyDict>()
            .map_err(|_| PyValueError::new_err("each node must be a dict"))?;

        let id: String = dict
            .get_item("id")?
            .ok_or_else(|| PyValueError::new_err("node missing 'id'"))?
            .extract()?;
        let name: String = dict
            .get_item("name")?
            .ok_or_else(|| PyValueError::new_err("node missing 'name'"))?
            .extract()?;
        let code: String = dict
            .get_item("code")?
            .ok_or_else(|| PyValueError::new_err("node missing 'code'"))?
            .extract()?;

        let language: String = dict
            .get_item("language")?
            .map(|v| v.extract())
            .transpose()?
            .unwrap_or_else(|| "python".to_string());

        // inputs: dict[str, (str, str)] -> HashMap<SlotName, (NodeId, SlotName)>
        let mut inputs = HashMap::new();
        if let Some(inp) = dict.get_item("inputs")? {
            let inp_dict = inp
                .downcast::<PyDict>()
                .map_err(|_| PyValueError::new_err("'inputs' must be a dict"))?;
            for (k, v) in inp_dict.iter() {
                let slot: String = k.extract()?;
                let (src_node, src_slot): (String, String) = v.extract()?;
                inputs.insert(
                    SlotName::new(&slot),
                    (NodeId::new(&src_node), SlotName::new(&src_slot)),
                );
            }
        }

        // outputs: list[str] -> Vec<SlotName>
        let mut outputs = Vec::new();
        if let Some(out) = dict.get_item("outputs")? {
            let out_list: Vec<String> = out.extract()?;
            outputs = out_list.into_iter().map(|s| SlotName::new(&s)).collect();
        }

        // map_over / map_concurrency
        let map_over: Option<SlotName> = dict
            .get_item("map_over")?
            .map(|v| -> PyResult<SlotName> {
                let s: String = v.extract()?;
                Ok(SlotName::new(&s))
            })
            .transpose()?;
        let map_concurrency: Option<usize> = dict
            .get_item("map_concurrency")?
            .map(|v| v.extract())
            .transpose()?;

        defs.push(NodeDef {
            id: NodeId::new(&id),
            name,
            code: NodeCode {
                language,
                source: code,
            },
            inputs,
            outputs,
            map_over,
            map_concurrency,
            cache: true,
            timeout_secs: None,
            tags: HashMap::new(),
        });
    }
    Ok(defs)
}

// ---------------------------------------------------------------------------
// Module
// ---------------------------------------------------------------------------

/// The tine native extension module.
#[pymodule]
fn _tine(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyWorkspace>()?;
    Ok(())
}
