use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::Deserialize;
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
// Shipped desktop runtime packages are pinned exactly in the shared
// runtime_pins.json manifest. Users can still override any version by listing
// the same package in their pipeline's `deps`.

const CORE_RUNTIME_CATEGORY: &str = "Core Notebook / Runtime";

#[derive(Debug, Clone, Deserialize)]
struct RuntimePackagePin {
    category: String,
    package: String,
    version: String,
}

#[derive(Debug, Deserialize)]
struct DesktopRuntimePins {
    baseline_packages: Vec<RuntimePackagePin>,
}

#[derive(Debug, Deserialize)]
struct RuntimePinsManifest {
    desktop_runtime: DesktopRuntimePins,
}

fn runtime_package_pins() -> &'static [RuntimePackagePin] {
    static PINS: OnceLock<Vec<RuntimePackagePin>> = OnceLock::new();
    PINS.get_or_init(|| {
        let manifest: RuntimePinsManifest = serde_json::from_str(include_str!(
            "../../../scripts/release/runtime_pins.json"
        ))
        .expect("runtime_pins.json should be valid JSON");
        manifest.desktop_runtime.baseline_packages
    })
    .as_slice()
}

fn package_pin_spec(pin: &RuntimePackagePin) -> String {
    format!("{}=={}", pin.package, pin.version)
}

fn required_runtime_packages() -> &'static [String] {
    static REQUIRED: OnceLock<Vec<String>> = OnceLock::new();
    REQUIRED
        .get_or_init(|| {
            runtime_package_pins()
                .iter()
                .filter(|pin| pin.category == CORE_RUNTIME_CATEGORY)
                .map(package_pin_spec)
                .collect()
        })
        .as_slice()
}

fn default_runtime_packages() -> &'static [String] {
    static DEFAULTS: OnceLock<Vec<String>> = OnceLock::new();
    DEFAULTS
        .get_or_init(|| {
            runtime_package_pins()
                .iter()
                .filter(|pin| pin.category != CORE_RUNTIME_CATEGORY)
                .map(package_pin_spec)
                .collect()
        })
        .as_slice()
}

/// Merges required + default + user packages, deduplicating by package name
/// (user deps take precedence over defaults).
pub fn resolve_packages(user_deps: &[String]) -> Vec<String> {
    use std::collections::HashMap;

    // Start with required packages
    let mut by_name: HashMap<String, String> = HashMap::new();
    for pkg in required_runtime_packages() {
        let name = package_name(pkg);
        by_name.insert(name, pkg.clone());
    }

    // Layer defaults
    for pkg in default_runtime_packages() {
        let name = package_name(pkg);
        by_name.insert(name, pkg.clone());
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

#[derive(Debug, Clone)]
struct PythonCommand {
    program: PathBuf,
    args: Vec<String>,
    display: String,
}

impl PythonCommand {
    fn new(program: impl Into<PathBuf>, args: Vec<String>) -> Self {
        let program = program.into();
        let display = if args.is_empty() {
            program.display().to_string()
        } else {
            format!("{} {}", program.display(), args.join(" "))
        };
        Self {
            program,
            args,
            display,
        }
    }
}

fn bundled_python_path() -> Option<PathBuf> {
    std::env::var_os("TINE_BUNDLED_PYTHON").map(PathBuf::from)
}

fn supported_python_version(version: &str) -> bool {
    let mut parts = version.split('.');
    let major = parts.next().and_then(|part| part.parse::<u32>().ok());
    let minor = parts.next().and_then(|part| part.parse::<u32>().ok());

    matches!((major, minor), (Some(3), Some(minor)) if (10..=13).contains(&minor))
}

/// Manages Python environments for experiment kernels.
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

    /// Verify that a compatible Python interpreter is available.
    pub async fn ensure_python_version_available(
        &self,
        python_version: &str,
    ) -> TineResult<String> {
        self.resolve_python_command(python_version)
            .await
            .map(|command| command.display)
    }

    /// Build a temporary kernel runtime and verify the Jupyter entrypoint can start.
    pub async fn doctor_runtime_check(&self) -> TineResult<String> {
        let python_command = self.resolve_python_command(DEFAULT_PYTHON_VERSION).await?;

        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let venv_dir = self
            .workspace_root
            .join(".tine")
            .join("doctor")
            .join(format!("runtime-check-{unique}"));
        let runtime_id = format!("doctor-runtime-{unique}");
        let mut logs = Vec::new();
        let uses_bundled_python = bundled_python_path()
            .as_ref()
            .is_some_and(|path| path == &python_command.program);

        let result = async {
            self.create_venv(
                &runtime_id,
                &python_command,
                &venv_dir,
                uses_bundled_python,
                &mut logs,
            )
                .await?;
            let required_packages = required_runtime_packages().to_vec();
            self.sync_packages(&runtime_id, &venv_dir, &required_packages, &mut logs)
                .await?;
            let python_path = self.python_path(&venv_dir);
            self.preflight_kernel_runtime(&runtime_id, &python_path, &venv_dir, &mut logs)
                .await?;
            Ok::<(), TineError>(())
        }
        .await;

        match tokio::fs::remove_dir_all(&venv_dir).await {
            Ok(()) => logs.push(format!("Removed doctor runtime at {}", venv_dir.display())),
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => logs.push(format!(
                "Failed to remove doctor runtime at {}: {}",
                venv_dir.display(),
                error
            )),
        }

        result?;
        Ok(logs.join("\n\n"))
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

    fn venv_config_path(&self, venv_dir: &Path) -> PathBuf {
        venv_dir.join("pyvenv.cfg")
    }

    fn venv_looks_valid(&self, venv_dir: &Path) -> bool {
        self.venv_config_path(venv_dir).is_file() && self.python_path(venv_dir).is_file()
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
        let python_command = self.resolve_python_command(DEFAULT_PYTHON_VERSION).await?;
        let uses_bundled_python = bundled_python_path()
            .as_ref()
            .is_some_and(|path| path == &python_command.program);

        info!(
            owner = runtime_id,
            venv = %venv_dir.display(),
            deps = spec.dependencies.len(),
            "ensuring environment"
        );

        if venv_dir.exists() && !self.venv_looks_valid(&venv_dir) {
            eprintln!("[tine-env] removing broken venv at {}", venv_dir.display());
            logs.push(format!(
                "Removing broken venv at {} before recreation",
                venv_dir.display()
            ));
            tokio::fs::remove_dir_all(&venv_dir).await.map_err(|e| {
                TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: format!("failed to remove broken venv dir: {}", e),
                }
            })?;
        }

        // Create venv if it doesn't exist
        if !venv_dir.exists() {
            self.create_venv(
                runtime_id,
                &python_command,
                &venv_dir,
                uses_bundled_python,
                &mut logs,
            )
            .await?;
        } else {
            eprintln!("[tine-env] venv already exists at {}", venv_dir.display());
            logs.push(format!("Using existing venv at {}", venv_dir.display()));
        }

        self.ensure_pip_available(runtime_id, &venv_dir, &mut logs)
            .await?;

        let packages_to_sync = if uses_bundled_python {
            if spec.dependencies.is_empty() {
                logs.push(
                    "Bundled runtime already provides baseline packages; no package sync required"
                        .to_string(),
                );
            } else {
                logs.push(format!(
                    "Bundled runtime provides baseline packages; syncing {} user-declared package(s)",
                    spec.dependencies.len()
                ));
            }
            spec.dependencies.clone()
        } else {
            resolve_packages(&spec.dependencies)
        };
        self.sync_packages(runtime_id, &venv_dir, &packages_to_sync, &mut logs)
            .await?;

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
        let mut command = Command::new(python_path);
        command
            .arg("-c")
            .arg("import ipykernel_launcher")
            .current_dir(&self.workspace_root);
        self.apply_venv_env(&mut command, venv_dir);
        let output = command.output().await.map_err(|e| TineError::EnvironmentFailed {
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

    async fn create_venv(
        &self,
        runtime_id: &str,
        python_command: &PythonCommand,
        venv_dir: &Path,
        inherit_site_packages: bool,
        logs: &mut Vec<String>,
    ) -> TineResult<()> {
        eprintln!("[tine-env] creating venv at {}", venv_dir.display());
        debug!(venv = %venv_dir.display(), "creating venv");
        logs.push(format!("Creating venv at {}", venv_dir.display()));
        logs.push(format!(
            "Using Python interpreter {}",
            python_command.display
        ));
        let venv_parent = venv_dir.parent().unwrap_or(&self.workspace_root);
        tokio::fs::create_dir_all(venv_parent)
            .await
            .map_err(|e| TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!("failed to create venv parent dir: {}", e),
            })?;
        let mut command = Command::new(&python_command.program);
        command
            .args(&python_command.args)
            .arg("-m")
            .arg("venv");
        if inherit_site_packages {
            command.arg("--system-site-packages");
            logs.push(
                "Bundled runtime detected; creating venv with inherited site-packages"
                    .to_string(),
            );
        }
        command
            .arg(venv_dir)
            .current_dir(&self.workspace_root);
        let output = command
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
            if venv_dir.exists() && !self.venv_looks_valid(&venv_dir) {
                match tokio::fs::remove_dir_all(&venv_dir).await {
                    Ok(()) => logs.push(format!(
                        "Removed partial venv at {} after failed creation",
                        venv_dir.display()
                    )),
                    Err(remove_error) if remove_error.kind() == ErrorKind::NotFound => {}
                    Err(remove_error) => logs.push(format!(
                        "Failed to remove partial venv at {}: {}",
                        venv_dir.display(),
                        remove_error
                    )),
                }
            }
            return Err(TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: stderr.to_string(),
            });
        }
        if !output.stdout.is_empty() {
            logs.push(String::from_utf8_lossy(&output.stdout).trim().to_string());
        }
        let python_path = self.python_path(venv_dir);
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
        self.ensure_pip_available(runtime_id, venv_dir, logs)
            .await?;
        eprintln!("[tine-env] venv created successfully");
        logs.push("Venv created successfully".to_string());
        Ok(())
    }

    async fn ensure_pip_available(
        &self,
        runtime_id: &str,
        venv_dir: &Path,
        logs: &mut Vec<String>,
    ) -> TineResult<()> {
        let python_path = self.python_path(venv_dir);
        if !python_path.exists() {
            return Err(TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!(
                    "python executable was not found at '{}' before ensuring pip",
                    python_path.display()
                ),
            });
        }

        let mut pip_check = Command::new(&python_path);
        pip_check.arg("-m").arg("pip").arg("--version");
        self.apply_venv_env(&mut pip_check, venv_dir);
        let pip_ready = pip_check
            .output()
            .await
            .map(|output| output.status.success());
        if matches!(pip_ready, Ok(true)) {
            logs.push("pip is available in the environment".to_string());
            return Ok(());
        }

        logs.push("Bootstrapping pip with ensurepip".to_string());
        let mut ensurepip = Command::new(&python_path);
        ensurepip.arg("-m").arg("ensurepip").arg("--upgrade");
        self.apply_venv_env(&mut ensurepip, venv_dir);
        let output = ensurepip
            .output()
            .await
            .map_err(|e| TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!("failed to bootstrap pip with ensurepip: {}", e),
            })?;

        if !output.stdout.is_empty() {
            logs.push(String::from_utf8_lossy(&output.stdout).trim().to_string());
        }
        if !output.stderr.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if !stderr.trim().is_empty() {
                logs.push(stderr.trim().to_string());
            }
        }
        if !output.status.success() {
            return Err(TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!(
                    "failed to bootstrap pip in '{}': {}",
                    venv_dir.display(),
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            });
        }

        let mut verify_pip = Command::new(&python_path);
        verify_pip.arg("-m").arg("pip").arg("--version");
        self.apply_venv_env(&mut verify_pip, venv_dir);
        let verify_output =
            verify_pip
                .output()
                .await
                .map_err(|e| TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: format!("failed to verify pip after ensurepip: {}", e),
                })?;
        if !verify_output.status.success() {
            return Err(TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!(
                    "pip remained unavailable in '{}' after ensurepip",
                    venv_dir.display()
                ),
            });
        }

        logs.push("pip is available in the environment".to_string());
        Ok(())
    }

    async fn sync_packages(
        &self,
        runtime_id: &str,
        venv_dir: &Path,
        packages: &[String],
        logs: &mut Vec<String>,
    ) -> TineResult<()> {
        let requirements_path = self.effective_requirements_path(venv_dir);
        let requirements_contents = if packages.is_empty() {
            String::new()
        } else {
            format!("{}\n", packages.join("\n"))
        };
        let requirements_unchanged = match tokio::fs::read_to_string(&requirements_path).await {
            Ok(existing) => existing == requirements_contents,
            Err(err) if err.kind() == ErrorKind::NotFound => false,
            Err(err) => {
                return Err(TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: format!("failed to read effective requirements: {}", err),
                });
            }
        };
        if !requirements_unchanged {
            tokio::fs::write(&requirements_path, &requirements_contents)
                .await
                .map_err(|e| TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: format!("failed to write effective requirements: {}", e),
                })?;
        }
        if packages.is_empty() {
            logs.push("No additional packages required".to_string());
            return Ok(());
        }

        if requirements_unchanged {
            logs.push(format!(
                "Requirements already match {}; skipping package sync",
                requirements_path.display()
            ));
            return Ok(());
        }

        eprintln!(
            "[tine-env] syncing {} packages (required + defaults + user)",
            packages.len()
        );
        logs.push(format!(
            "Installing {} packages from {}",
            packages.len(),
            requirements_path.display()
        ));
        let python_path = self.python_path(venv_dir);
        if !python_path.exists() {
            return Err(TineError::EnvironmentFailed {
                runtime_id: runtime_id.to_string(),
                message: format!(
                    "python executable was not found at '{}' before syncing packages",
                    python_path.display()
                ),
            });
        }
        let mut cmd = Command::new(&python_path);
        cmd.arg("-m")
            .arg("pip")
            .arg("install")
            .arg("-r")
            .arg(&requirements_path)
            .current_dir(&self.workspace_root);
        self.apply_venv_env(&mut cmd, venv_dir);

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

    fn venv_bin_dir(&self, venv_dir: &Path) -> PathBuf {
        if cfg!(windows) {
            venv_dir.join("Scripts")
        } else {
            venv_dir.join("bin")
        }
    }

    fn apply_venv_env<'a>(&self, command: &'a mut Command, venv_dir: &Path) -> &'a mut Command {
        command
            .env("VIRTUAL_ENV", venv_dir)
            .env("PATH", self.path_with_venv_bin(venv_dir))
    }

    fn path_with_venv_bin(&self, venv_dir: &Path) -> OsString {
        let venv_bin = self.venv_bin_dir(venv_dir);
        let mut paths = vec![venv_bin];
        if let Some(existing) = std::env::var_os("PATH") {
            paths.extend(std::env::split_paths(&existing));
        }
        std::env::join_paths(paths).unwrap_or_else(|_| {
            let mut fallback = OsString::from(self.venv_bin_dir(venv_dir));
            if let Some(existing) = std::env::var_os("PATH") {
                fallback.push(if cfg!(windows) { ";" } else { ":" });
                fallback.push(existing);
            }
            fallback
        })
    }

    async fn resolve_python_command(&self, python_version: &str) -> TineResult<PythonCommand> {
        let mut attempted = Vec::new();

        for command in self.explicit_python_commands() {
            attempted.push(command.display.clone());
            match self
                .python_command_matches_version(&command, python_version)
                .await
            {
                Ok(true) => return Ok(command),
                Ok(false) => {}
                Err(_) => {}
            }
        }

        if let Some(command) = self.resolve_python_via_uv(python_version).await {
            return Ok(command);
        }

        for command in self.python_command_candidates(python_version) {
            attempted.push(command.display.clone());
            match self
                .python_command_matches_version(&command, python_version)
                .await
            {
                Ok(true) => return Ok(command),
                Ok(false) => {}
                Err(_) => {}
            }
        }

        Err(TineError::Config(format!(
            "Python {}+ is not available. Tried {}",
            python_version,
            attempted.join(", ")
        )))
    }

    fn explicit_python_commands(&self) -> Vec<PythonCommand> {
        let mut commands = Vec::new();
        let mut seen = std::collections::HashSet::new();

        for key in ["TINE_PYTHON", "TINE_BUNDLED_PYTHON", "TINE_WRAPPER_PYTHON"] {
            let Some(value) = std::env::var_os(key) else {
                continue;
            };
            let path = PathBuf::from(value);
            if !seen.insert(path.clone()) {
                continue;
            }
            commands.push(PythonCommand::new(path, Vec::new()));
        }

        commands
    }

    async fn resolve_python_via_uv(&self, python_version: &str) -> Option<PythonCommand> {
        let output = Command::new(&self.uv_path)
            .args(["python", "find", python_version])
            .current_dir(&self.workspace_root)
            .output()
            .await
            .ok()?;

        if !output.status.success() {
            return None;
        }

        let resolved = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if resolved.is_empty() {
            return None;
        }

        Some(PythonCommand::new(PathBuf::from(resolved), Vec::new()))
    }

    fn python_command_candidates(&self, python_version: &str) -> Vec<PythonCommand> {
        let major = python_version
            .split('.')
            .next()
            .unwrap_or(python_version)
            .to_string();

        let mut candidates = Vec::new();
        let mut seen = std::collections::HashSet::new();

        let mut push_candidate = |command: PythonCommand| {
            let key = command.display.clone();
            if seen.insert(key) {
                candidates.push(command);
            }
        };

        if cfg!(windows) {
            push_candidate(PythonCommand::new(
                PathBuf::from("py"),
                vec![format!("-{}", python_version)],
            ));
            push_candidate(PythonCommand::new(
                PathBuf::from("py"),
                vec![format!("-{}", major)],
            ));
            push_candidate(PythonCommand::new(
                PathBuf::from(format!("python{}", python_version)),
                Vec::new(),
            ));
            push_candidate(PythonCommand::new(PathBuf::from("python"), Vec::new()));
        } else {
            push_candidate(PythonCommand::new(
                PathBuf::from(format!("python{}", python_version)),
                Vec::new(),
            ));
            push_candidate(PythonCommand::new(
                PathBuf::from(format!("python{}", major)),
                Vec::new(),
            ));
            push_candidate(PythonCommand::new(PathBuf::from("python3"), Vec::new()));
            push_candidate(PythonCommand::new(PathBuf::from("python"), Vec::new()));
        }

        candidates
    }

    async fn python_command_matches_version(
        &self,
        command: &PythonCommand,
        _python_version: &str,
    ) -> TineResult<bool> {
        let mut probe = Command::new(&command.program);
        probe
            .args(&command.args)
            .arg("-c")
            .arg("import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')")
            .current_dir(&self.workspace_root);
        let output = probe
            .output()
            .await
            .map_err(|e| TineError::Config(format!("failed to run {}: {}", command.display, e)))?;
        if !output.status.success() {
            return Ok(false);
        }

        let actual = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(supported_python_version(&actual))
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

pub const DEFAULT_PYTHON_VERSION: &str = "3.11";

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

    #[test]
    fn supported_python_version_accepts_supported_range() {
        assert!(supported_python_version("3.10"));
        assert!(supported_python_version("3.11"));
        assert!(supported_python_version("3.12"));
        assert!(supported_python_version("3.13"));
        assert!(!supported_python_version("3.9"));
        assert!(!supported_python_version("3.14"));
        assert!(!supported_python_version("2.7"));
    }

    #[test]
    fn explicit_python_commands_prioritize_env_vars_without_duplicates() {
        let manager = EnvironmentManager::new(PathBuf::from("/tmp/tine-workspace"));
        let bundled = if cfg!(windows) {
            r"C:\runtime\python.exe"
        } else {
            "/tmp/runtime/python/bin/python3"
        };
        let override_python = if cfg!(windows) {
            r"C:\override\python.exe"
        } else {
            "/tmp/override/python3"
        };

        let previous_tine_python = std::env::var_os("TINE_PYTHON");
        let previous_bundled = std::env::var_os("TINE_BUNDLED_PYTHON");
        let previous_wrapper = std::env::var_os("TINE_WRAPPER_PYTHON");

        std::env::set_var("TINE_PYTHON", override_python);
        std::env::set_var("TINE_BUNDLED_PYTHON", bundled);
        std::env::set_var("TINE_WRAPPER_PYTHON", bundled);
        let commands = manager.explicit_python_commands();

        match previous_tine_python {
            Some(value) => std::env::set_var("TINE_PYTHON", value),
            None => std::env::remove_var("TINE_PYTHON"),
        }
        match previous_bundled {
            Some(value) => std::env::set_var("TINE_BUNDLED_PYTHON", value),
            None => std::env::remove_var("TINE_BUNDLED_PYTHON"),
        }
        match previous_wrapper {
            Some(value) => std::env::set_var("TINE_WRAPPER_PYTHON", value),
            None => std::env::remove_var("TINE_WRAPPER_PYTHON"),
        }

        let displays: Vec<_> = commands.into_iter().map(|command| command.display).collect();
        assert_eq!(displays.len(), 2);
        assert_eq!(displays[0], override_python);
        assert_eq!(displays[1], bundled);
    }

    #[test]
    fn bundled_python_path_reads_env_var() {
        let bundled = if cfg!(windows) {
            r"C:\runtime\python.exe"
        } else {
            "/tmp/runtime/python/bin/python3"
        };
        let previous_bundled = std::env::var_os("TINE_BUNDLED_PYTHON");
        std::env::set_var("TINE_BUNDLED_PYTHON", bundled);

        assert_eq!(bundled_python_path(), Some(PathBuf::from(bundled)));

        match previous_bundled {
            Some(value) => std::env::set_var("TINE_BUNDLED_PYTHON", value),
            None => std::env::remove_var("TINE_BUNDLED_PYTHON"),
        }
    }

    #[test]
    fn runtime_package_pins_load_exact_versions_from_manifest() {
        let required = required_runtime_packages();
        let defaults = default_runtime_packages();

        assert!(required.iter().any(|pkg| pkg == "ipykernel==7.2.0"));
        assert!(required.iter().any(|pkg| pkg == "cloudpickle==3.1.2"));
        assert!(defaults.iter().any(|pkg| pkg == "pandas==3.0.2"));
        assert!(defaults.iter().any(|pkg| pkg == "scikit-learn==1.8.0"));
    }

    #[test]
    fn venv_is_only_valid_when_config_and_python_exist() {
        let temp = tempdir().unwrap();
        let manager = EnvironmentManager::new(temp.path().join("workspace"));
        let venv_dir = temp.path().join("venv");
        fs::create_dir_all(venv_dir.join("bin")).unwrap();

        assert!(!manager.venv_looks_valid(&venv_dir));

        fs::write(venv_dir.join("pyvenv.cfg"), "home = /tmp/python\n").unwrap();
        assert!(!manager.venv_looks_valid(&venv_dir));

        fs::write(venv_dir.join("bin").join("python"), "#!/bin/sh\n").unwrap();
        assert!(manager.venv_looks_valid(&venv_dir));
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
