use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, info};

use tine_core::{
    EnvironmentSpec, ExperimentTreeDef, ExperimentTreeId, ProjectId, TineError, TineResult,
};

// ---------------------------------------------------------------------------
// Default packages — the "conda defaults" equivalent
// ---------------------------------------------------------------------------
// These are always installed in every tine kernel environment.  They mirror
// what conda's `defaults` channel ships so that common data-science workflows
// work out of the box without the user declaring any deps.
//
// Packages are pinned to major-version ranges for reproducibility while still
// allowing patch updates.  Users can override any version by listing the same
// package in their pipeline's `deps`.

/// Packages essential for tine's own plumbing (Arrow IPC serialization, kernel
/// protocol, etc.).  These are non-negotiable.
pub const TINE_REQUIRED_PACKAGES: &[&str] = &["ipykernel", "cloudpickle"];

/// The data-science "batteries included" set — modeled after conda defaults.
pub const DEFAULT_PACKAGES: &[&str] = &[
    // Arrow (needed for zero-copy DataFrames in cache)
    "pyarrow>=14",
    // Data wrangling
    "numpy>=1.26",
    "pandas>=2.1",
    "polars>=0.20",
    // Math / stats
    "scipy>=1.12",
    // Machine learning
    "scikit-learn>=1.4",
    // Visualization
    "matplotlib>=3.8",
    "seaborn>=0.13",
    // Progress bars
    "tqdm>=4.66",
    // HTTP
    "requests>=2.31",
    // Image processing
    "pillow>=10",
];

/// Merges required + default + user packages, deduplicating by package name
/// (user deps take precedence over defaults).
pub fn resolve_packages(user_deps: &[String]) -> Vec<String> {
    use std::collections::HashMap;

    // Start with required packages
    let mut by_name: HashMap<String, String> = HashMap::new();
    for pkg in TINE_REQUIRED_PACKAGES {
        let name = package_name(pkg);
        by_name.insert(name, pkg.to_string());
    }

    // Layer defaults
    for pkg in DEFAULT_PACKAGES {
        let name = package_name(pkg);
        by_name.insert(name, pkg.to_string());
    }

    // Layer user deps — these win over defaults
    for pkg in user_deps {
        let name = package_name(pkg);
        by_name.insert(name, pkg.clone());
    }

    let mut packages: Vec<String> = by_name.into_values().collect();
    packages.sort();
    packages
}

/// Extract the bare package name from a specifier like "numpy>=1.26".
fn package_name(spec: &str) -> String {
    let name = spec
        .split(|c: char| c == '>' || c == '<' || c == '=' || c == '!' || c == '[' || c == ';')
        .next()
        .unwrap_or(spec)
        .trim();
    name.to_lowercase()
}

/// Manages Python environments via the `uv` binary.
pub struct EnvironmentManager {
    /// Path to the uv binary.
    uv_path: PathBuf,
    /// Root directory of the tine workspace.
    workspace_root: PathBuf,
    /// Serialize venv creation/sync so concurrent branch execution does not
    /// race on the shared workspace environment.
    env_lock: Mutex<()>,
}

fn global_env_lock() -> &'static Mutex<()> {
    static GLOBAL_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    GLOBAL_ENV_LOCK.get_or_init(|| Mutex::new(()))
}

#[derive(Debug, Clone)]
pub struct TreeEnvironmentDescriptor {
    pub tree_id: ExperimentTreeId,
    pub project_id: Option<ProjectId>,
    pub environment: EnvironmentSpec,
}

impl TreeEnvironmentDescriptor {
    pub fn new(
        tree_id: ExperimentTreeId,
        project_id: Option<ProjectId>,
        environment: EnvironmentSpec,
    ) -> Self {
        Self {
            tree_id,
            project_id,
            environment,
        }
    }

    pub fn from_tree(tree: &ExperimentTreeDef) -> Self {
        Self::new(
            tree.id.clone(),
            tree.project_id.clone(),
            tree.environment.clone(),
        )
    }
}

impl EnvironmentManager {
    pub fn new(workspace_root: PathBuf) -> Self {
        let workspace_root = normalize_workspace_root(workspace_root);
        // Default: look for uv on PATH
        Self {
            uv_path: PathBuf::from("uv"),
            workspace_root,
            env_lock: Mutex::new(()),
        }
    }

    pub fn with_uv_path(mut self, path: PathBuf) -> Self {
        self.uv_path = path;
        self
    }

    /// Verify uv is available.
    pub async fn ensure_uv(&self) -> TineResult<()> {
        let output = Command::new(&self.uv_path)
            .arg("--version")
            .output()
            .await
            .map_err(|e| TineError::UvNotFound {
                path: self.uv_path.display().to_string(),
                message: e.to_string(),
            })?;

        if !output.status.success() {
            return Err(TineError::UvNotFound {
                path: self.uv_path.display().to_string(),
                message: String::from_utf8_lossy(&output.stderr).to_string(),
            });
        }

        let version = String::from_utf8_lossy(&output.stdout);
        info!(version = %version.trim(), "uv available");
        Ok(())
    }

    fn venv_dir_for_project(&self, project_id: Option<&ProjectId>) -> PathBuf {
        match project_id {
            Some(project_id) => self
                .workspace_root
                .join(".tine")
                .join("projects")
                .join(project_id.as_str())
                .join("venv"),
            None => self.workspace_root.join(".tine").join("venv"),
        }
    }

    fn venv_dir_for_tree(&self, descriptor: &TreeEnvironmentDescriptor) -> PathBuf {
        self.venv_dir_for_project(descriptor.project_id.as_ref())
    }

    fn effective_requirements_path(&self, venv_dir: &Path) -> PathBuf {
        venv_dir.join("requirements.txt")
    }

    fn normalize_venv_dir(&self, venv_dir: &Path) -> PathBuf {
        if venv_dir.is_absolute() {
            venv_dir.to_path_buf()
        } else {
            self.workspace_root.join(venv_dir)
        }
    }

    /// Ensure the environment for an experiment tree exists and is up-to-date.
    /// Returns the path to the venv.
    pub async fn ensure_tree_environment(
        &self,
        tree: &TreeEnvironmentDescriptor,
    ) -> TineResult<PathBuf> {
        self.ensure_tree_environment_internal(tree)
            .await
            .map(|(venv_dir, _)| venv_dir)
    }

    async fn ensure_tree_environment_internal(
        &self,
        tree: &TreeEnvironmentDescriptor,
    ) -> TineResult<(PathBuf, String)> {
        self.ensure_environment_with_owner(
            tree.tree_id.as_str(),
            &tree.environment,
            &self.venv_dir_for_tree(tree),
        )
        .await
    }

    async fn ensure_environment_with_owner(
        &self,
        runtime_id: &str,
        spec: &EnvironmentSpec,
        venv_dir: &Path,
    ) -> TineResult<(PathBuf, String)> {
        let _global_env_guard = global_env_lock().lock().await;
        let _env_guard = self.env_lock.lock().await;
        let venv_dir = self.normalize_venv_dir(venv_dir);
        let mut logs = Vec::new();

        info!(
            owner = runtime_id,
            venv = %venv_dir.display(),
            deps = spec.dependencies.len(),
            "ensuring environment"
        );

        // Create venv if it doesn't exist
        if !venv_dir.exists() {
            eprintln!("[tine-env] creating venv at {}", venv_dir.display());
            debug!(venv = %venv_dir.display(), "creating venv");
            logs.push(format!("Creating venv at {}", venv_dir.display()));
            tokio::fs::create_dir_all(&venv_dir).await.map_err(|e| {
                TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: format!("failed to create venv dir: {}", e),
                }
            })?;
            let output = Command::new(&self.uv_path)
                .args(["venv", &venv_dir.display().to_string()])
                .args(["--python", DEFAULT_PYTHON_VERSION])
                .current_dir(&self.workspace_root)
                .output()
                .await
                .map_err(|e| TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: e.to_string(),
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !output.stdout.is_empty() {
                    logs.push(String::from_utf8_lossy(&output.stdout).trim().to_string());
                }
                if !stderr.trim().is_empty() {
                    logs.push(stderr.trim().to_string());
                }
                eprintln!("[tine-env] venv creation failed: {}", stderr);
                return Err(TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: stderr.to_string(),
                });
            }
            if !output.stdout.is_empty() {
                logs.push(String::from_utf8_lossy(&output.stdout).trim().to_string());
            }
            let python_path = self.python_path(&venv_dir);
            if !python_path.exists() {
                return Err(TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: format!(
                        "venv created at '{}' but python executable was not found at '{}'",
                        venv_dir.display(),
                        python_path.display()
                    ),
                });
            }
            eprintln!("[tine-env] venv created successfully");
            logs.push("Venv created successfully".to_string());
        } else {
            eprintln!("[tine-env] venv already exists at {}", venv_dir.display());
            logs.push(format!("Using existing venv at {}", venv_dir.display()));
        }

        // Sync all packages: required + defaults + user deps
        let all_packages = resolve_packages(&spec.dependencies);
        let requirements_path = self.effective_requirements_path(&venv_dir);
        let requirements_contents = if all_packages.is_empty() {
            String::new()
        } else {
            format!("{}\n", all_packages.join("\n"))
        };
        tokio::fs::write(&requirements_path, requirements_contents)
            .await
            .map_err(|e| TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!("failed to write effective requirements: {}", e),
            })?;
        if !all_packages.is_empty() {
            eprintln!(
                "[tine-env] syncing {} packages (required + defaults + user)",
                all_packages.len()
            );
            logs.push(format!(
                "Installing {} packages from {}",
                all_packages.len(),
                requirements_path.display()
            ));
            let python_path = self.python_path(&venv_dir);
            if !python_path.exists() {
                return Err(TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: format!(
                        "python executable was not found at '{}' before syncing packages",
                        python_path.display()
                    ),
                });
            }
            let mut cmd = Command::new(&self.uv_path);
            cmd.arg("pip")
                .arg("install")
                .arg("--python")
                .arg(&python_path)
                .arg("-r")
                .arg(&requirements_path)
                .env("VIRTUAL_ENV", &venv_dir)
                .current_dir(&self.workspace_root);

            let output = cmd
                .output()
                .await
                .map_err(|e| TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: e.to_string(),
                })?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !output.stdout.is_empty() {
                    logs.push(String::from_utf8_lossy(&output.stdout).trim().to_string());
                }
                if !stderr.trim().is_empty() {
                    logs.push(stderr.trim().to_string());
                }
                eprintln!("[tine-env] package sync failed: {}", stderr);
                return Err(TineError::DependencyResolution(stderr.to_string()));
            }
            if !output.stdout.is_empty() {
                logs.push(String::from_utf8_lossy(&output.stdout).trim().to_string());
            }
            if !output.stderr.is_empty() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                if !stderr.trim().is_empty() {
                    logs.push(stderr.trim().to_string());
                }
            }
            eprintln!("[tine-env] environment synced successfully");
            logs.push("Environment is ready".to_string());
        } else {
            logs.push("No additional packages required".to_string());
        }

        let python_path = self.python_path(&venv_dir);
        self.preflight_kernel_runtime(runtime_id, &python_path, &venv_dir, &mut logs)
            .await?;

        Ok((venv_dir, logs.join("\n\n")))
    }

    async fn preflight_kernel_runtime(
        &self,
        runtime_id: &str,
        python_path: &Path,
        venv_dir: &Path,
        logs: &mut Vec<String>,
    ) -> TineResult<()> {
        if !python_path.exists() {
            return Err(TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!(
                    "kernel runtime preflight failed: python executable was not found at '{}' after environment sync",
                    python_path.display()
                ),
            });
        }

        logs.push(format!(
            "Preflighting kernel runtime via {}",
            python_path.display()
        ));
        let output = Command::new(python_path)
            .arg("-c")
            .arg("import ipykernel_launcher")
            .env("VIRTUAL_ENV", venv_dir)
            .current_dir(&self.workspace_root)
            .output()
            .await
            .map_err(|e| TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!(
                    "kernel runtime preflight failed for '{}': {}. The environment was created at '{}', but the kernel entrypoint could not be started.",
                    python_path.display(),
                    e,
                    venv_dir.display()
                ),
            })?;

        if !output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
            if !stdout.is_empty() {
                logs.push(stdout.clone());
            }
            if !stderr.is_empty() {
                logs.push(stderr.clone());
            }
            let detail = if !stderr.is_empty() { stderr } else { stdout };
            return Err(TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!(
                    "kernel runtime preflight failed for '{}': {}. Ensure 'ipykernel' is installed and the venv is healthy before retrying.",
                    python_path.display(),
                    detail
                ),
            });
        }

        logs.push("Kernel runtime preflight passed".to_string());
        Ok(())
    }

    /// Compute blake3 hash of the lockfile for cache key computation.
    pub async fn lockfile_hash_for_tree(
        &self,
        tree: &TreeEnvironmentDescriptor,
    ) -> TineResult<[u8; 32]> {
        let venv_dir = self.venv_dir_for_tree(tree);
        let lockfile_path = venv_dir.join("requirements.lock");

        if lockfile_path.exists() {
            let data = tokio::fs::read(&lockfile_path).await?;
            Ok(*blake3::hash(&data).as_bytes())
        } else {
            // No lockfile yet — use hash of dependency list as fallback
            let deps_str = tree.environment.dependencies.join("\n");
            Ok(*blake3::hash(deps_str.as_bytes()).as_bytes())
        }
    }

    /// Get the Python executable path within a venv.
    pub fn python_path(&self, venv_dir: &Path) -> PathBuf {
        if cfg!(windows) {
            venv_dir.join("Scripts").join("python.exe")
        } else {
            venv_dir.join("bin").join("python")
        }
    }
}

fn normalize_workspace_root(workspace_root: PathBuf) -> PathBuf {
    if workspace_root.is_absolute() {
        workspace_root
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(&workspace_root))
            .unwrap_or(workspace_root)
    }
}

const DEFAULT_PYTHON_VERSION: &str = "3.11";

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;
    use tine_core::ExperimentTreeDef;

    #[test]
    fn tree_environment_descriptor_uses_workspace_root_for_non_project_trees() {
        let manager = EnvironmentManager::new(PathBuf::from("/tmp/tine-workspace"));
        let descriptor = TreeEnvironmentDescriptor::new(
            ExperimentTreeId::new("tree-1"),
            None,
            EnvironmentSpec::default(),
        );

        assert_eq!(
            manager.venv_dir_for_tree(&descriptor),
            PathBuf::from("/tmp/tine-workspace/.tine/venv")
        );
    }

    #[test]
    fn tree_environment_descriptor_uses_project_root_for_project_trees() {
        let manager = EnvironmentManager::new(PathBuf::from("/tmp/tine-workspace"));
        let descriptor = TreeEnvironmentDescriptor::new(
            ExperimentTreeId::new("tree-1"),
            Some(ProjectId::new("project-1")),
            EnvironmentSpec::default(),
        );

        assert_eq!(
            manager.venv_dir_for_tree(&descriptor),
            PathBuf::from("/tmp/tine-workspace/.tine/projects/project-1/venv")
        );
    }

    #[test]
    fn tree_environment_descriptor_can_be_built_from_tree() {
        let tree: ExperimentTreeDef = serde_json::from_value(serde_json::json!({
            "id": "tree-1",
            "name": "Tree",
            "project_id": "project-1",
            "root_branch_id": "main",
            "branches": [{
                "id": "main",
                "name": "main",
                "cell_order": [],
                "display": {}
            }],
            "cells": [],
            "environment": {
                "dependencies": ["pandas"]
            },
            "execution_mode": "parallel",
            "budget": null,
            "created_at": "2026-01-01T00:00:00Z"
        }))
        .unwrap();

        let descriptor = TreeEnvironmentDescriptor::from_tree(&tree);

        assert_eq!(descriptor.tree_id, tree.id);
        assert_eq!(descriptor.project_id, tree.project_id);
        assert_eq!(descriptor.environment.dependencies, vec!["pandas"]);
    }

    #[test]
    fn environment_manager_normalizes_relative_workspace_root() {
        let relative = PathBuf::from("target/test-workspace-root");
        let manager = EnvironmentManager::new(relative.clone());

        assert!(manager.workspace_root.is_absolute());
        assert!(manager.workspace_root.ends_with(relative));
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn kernel_runtime_preflight_reports_missing_ipykernel() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().unwrap();
        let workspace_root = temp.path().join("workspace");
        fs::create_dir_all(&workspace_root).unwrap();

        let fake_python = temp.path().join("python");
        fs::write(
            &fake_python,
            "#!/bin/sh\n>&2 echo \"No module named ipykernel_launcher\"\nexit 1\n",
        )
        .unwrap();
        let mut permissions = fs::metadata(&fake_python).unwrap().permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&fake_python, permissions).unwrap();

        let manager = EnvironmentManager::new(workspace_root);
        let mut logs = Vec::new();
        let error = manager
            .preflight_kernel_runtime("tree-1", &fake_python, temp.path(), &mut logs)
            .await
            .unwrap_err();

        match error {
            TineError::EnvironmentFailed {
                runtime_id,
                message,
            } => {
                assert_eq!(runtime_id, "tree-1");
                assert!(message.contains("kernel runtime preflight failed"));
                assert!(message.contains("ipykernel"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
