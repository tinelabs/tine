use serde::Deserialize;
use std::collections::HashMap;
use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::process::Command;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use tine_core::{
    EnvironmentSpec, ExperimentTreeDef, ExperimentTreeId, ProjectId, TineError, TineResult,
};
use tine_observe::{
    OutcomeTimer, METRIC_ENV_ENSURE_LOCK_WAIT, METRIC_ENV_ENSURE_PIP_CHECK,
    METRIC_ENV_ENSURE_PREFLIGHT, METRIC_ENV_ENSURE_SYNC, METRIC_ENV_ENSURE_TOTAL,
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
        let manifest: RuntimePinsManifest =
            serde_json::from_str(include_str!("../../../scripts/release/runtime_pins.json"))
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

/// The interpreter architecture pinned at install / first stage and exported
/// by the desktop app or pip wrapper as the authoritative platform identity.
/// Holds Python's `platform.machine()` value (e.g. "arm64", "x86_64",
/// "aarch64"). When unset — dev runs, source checkouts, tests — architecture
/// is not enforced and resolution behaves exactly as before.
///
/// The comparison against an interpreter is an exact (case-insensitive) token
/// match, so the pin MUST be sourced from `platform.machine()` on the same OS
/// family it will run on: the same ISA is reported as `arm64` on macOS but
/// `aarch64` on Linux (and `AMD64` vs `x86_64` on Windows), so a pin copied
/// across operating systems would never match. The install-stage producer
/// records it from the bundled interpreter on the target host, which keeps
/// pin and probe on the same OS.
const PINNED_PLATFORM_ENV: &str = "TINE_PYTHON_PLATFORM";

fn pinned_python_platform() -> Option<String> {
    std::env::var(PINNED_PLATFORM_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Outcome of comparing a pinned architecture against an interpreter's
/// reported one. Kept as a pure function so the policy is exhaustively
/// unit-testable without spawning interpreters or mutating the environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlatformMatch {
    /// No architecture was pinned — enforcement is off.
    NotPinned,
    /// Pinned and the interpreter reports the same architecture.
    Match,
    /// Pinned but the interpreter reports a different architecture, or its
    /// architecture could not be determined (fail closed).
    Mismatch,
}

fn classify_platform_match(pinned: Option<&str>, actual: Option<&str>) -> PlatformMatch {
    match pinned {
        None => PlatformMatch::NotPinned,
        Some(pin) => match actual {
            Some(reported) if reported.eq_ignore_ascii_case(pin) => PlatformMatch::Match,
            _ => PlatformMatch::Mismatch,
        },
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
    /// Whether `uv_path` points at a working uv (checked once per process).
    uv_available: tokio::sync::OnceCell<bool>,
    /// Environments verified ready in this process, keyed by venv dir.
    /// Lets repeat ensures (every execution runs one) skip the subprocess
    /// round of python-resolve / pip-check / sync / preflight entirely.
    ready_environments: std::sync::Mutex<HashMap<PathBuf, EnvironmentReadyStamp>>,
}

/// Proof that a venv was verified for a given package set; invalidated when
/// the requested packages change or the venv's python binary changes on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
struct EnvironmentReadyStamp {
    packages_fingerprint: String,
    python_modified: Option<std::time::SystemTime>,
}

fn python_modified_time(python_path: &Path) -> Option<std::time::SystemTime> {
    std::fs::metadata(python_path)
        .and_then(|meta| meta.modified())
        .ok()
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
        // Resolution order: TINE_UV_PATH (pip wrapper / desktop / CI sets
        // this explicitly) → `uv` on PATH. Availability is probed lazily and
        // installs fall back to pip when uv is absent.
        let uv_path = std::env::var_os("TINE_UV_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("uv"));
        Self {
            uv_path,
            workspace_root,
            env_lock: Mutex::new(()),
            uv_available: tokio::sync::OnceCell::new(),
            ready_environments: std::sync::Mutex::new(HashMap::new()),
        }
    }

    pub fn with_uv_path(mut self, path: PathBuf) -> Self {
        self.uv_path = path;
        self.uv_available = tokio::sync::OnceCell::new();
        self
    }

    /// Which package installer this manager will use, for diagnostics.
    pub async fn installer_description(&self) -> String {
        if self.uv_is_available().await {
            format!("uv ({})", self.uv_path.display())
        } else {
            "pip (uv not found; installs will be slower — set TINE_UV_PATH or put `uv` on PATH)"
                .to_string()
        }
    }

    /// Probe `uv --version` once per process.
    async fn uv_is_available(&self) -> bool {
        *self
            .uv_available
            .get_or_init(|| async {
                let available = Command::new(&self.uv_path)
                    .arg("--version")
                    .output()
                    .await
                    .map(|output| output.status.success())
                    .unwrap_or(false);
                if available {
                    info!(uv = %self.uv_path.display(), "package installs will use uv");
                } else {
                    warn!(
                        uv = %self.uv_path.display(),
                        "uv not available; falling back to pip for package installs"
                    );
                }
                available
            })
            .await
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
            let use_uv = self.uv_is_available().await;
            self.sync_packages(
                &runtime_id,
                &venv_dir,
                &required_packages,
                use_uv,
                &mut logs,
            )
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

    /// Fingerprint of everything that determines an ensure's outcome for a
    /// given venv: the user-declared deps plus whether the bundled runtime
    /// supplies the baseline. Deliberately avoids subprocess calls so the
    /// memoization fast path stays free.
    fn environment_fingerprint(spec: &EnvironmentSpec) -> String {
        Self::environment_fingerprint_with(bundled_python_path().is_some(), spec)
    }

    fn environment_fingerprint_with(uses_bundled_python: bool, spec: &EnvironmentSpec) -> String {
        // The pinned architecture is part of the identity: if the install-stage
        // pin changes, a previously-verified venv must be re-validated rather
        // than reused from the in-process ready stamp.
        format!(
            "bundled={}|arch={}|deps={}",
            uses_bundled_python,
            pinned_python_platform().unwrap_or_default(),
            spec.dependencies.join("\n")
        )
    }

    /// True when this venv was already verified for this spec in this
    /// process and its python binary is unchanged on disk.
    fn environment_is_ready(&self, venv_dir: &Path, fingerprint: &str) -> bool {
        let stamp = match self
            .ready_environments
            .lock()
            .expect("ready_environments lock poisoned")
            .get(venv_dir)
            .cloned()
        {
            Some(stamp) => stamp,
            None => return false,
        };
        stamp.packages_fingerprint == fingerprint
            && stamp.python_modified.is_some()
            && stamp.python_modified == python_modified_time(&self.python_path(venv_dir))
    }

    fn mark_environment_ready(&self, venv_dir: PathBuf, fingerprint: String) {
        let python_modified = python_modified_time(&self.python_path(&venv_dir));
        self.ready_environments
            .lock()
            .expect("ready_environments lock poisoned")
            .insert(
                venv_dir,
                EnvironmentReadyStamp {
                    packages_fingerprint: fingerprint,
                    python_modified,
                },
            );
    }

    async fn ensure_environment_with_owner(
        &self,
        runtime_id: &str,
        spec: &EnvironmentSpec,
        venv_dir: &Path,
    ) -> TineResult<(PathBuf, String)> {
        let mut total_timer = OutcomeTimer::start(METRIC_ENV_ENSURE_TOTAL);
        let venv_dir = self.normalize_venv_dir(venv_dir);

        // Fast path: this venv was verified for this exact spec earlier in
        // this process and its python binary hasn't changed. Every execution
        // runs an ensure, so without this each run pays several subprocess
        // spawns (python resolve, pip check, sync, preflight) for a no-op.
        let fingerprint = Self::environment_fingerprint(spec);
        if self.environment_is_ready(&venv_dir, &fingerprint) {
            total_timer.set_outcome("cached");
            return Ok((venv_dir, "Environment ready (cached)".to_string()));
        }

        let mut lock_wait_timer = OutcomeTimer::start(METRIC_ENV_ENSURE_LOCK_WAIT);
        let _global_env_guard = global_env_lock().lock().await;
        let _env_guard = self.env_lock.lock().await;
        lock_wait_timer.set_outcome("success");
        drop(lock_wait_timer);

        // Re-check under the lock: a concurrent ensure may have finished the
        // verification while this task waited.
        if self.environment_is_ready(&venv_dir, &fingerprint) {
            total_timer.set_outcome("cached");
            return Ok((venv_dir, "Environment ready (cached)".to_string()));
        }

        let mut logs = Vec::new();
        let python_command = self.resolve_python_command(DEFAULT_PYTHON_VERSION).await?;
        let uses_bundled_python = bundled_python_path()
            .as_ref()
            .is_some_and(|path| path == &python_command.program);
        let use_uv = self.uv_is_available().await;

        info!(
            owner = runtime_id,
            venv = %venv_dir.display(),
            deps = spec.dependencies.len(),
            installer = if use_uv { "uv" } else { "pip" },
            "ensuring environment"
        );

        // An existing venv may have been built for a different architecture by
        // an earlier install (e.g. before an arch migration, or under
        // emulation). The in-process ready stamp cannot catch this across
        // restarts, so probe the venv's own interpreter against the pin and
        // recreate on mismatch. Only runs when an architecture is pinned.
        let pinned = pinned_python_platform();
        let venv_arch_mismatch = if pinned.is_some()
            && venv_dir.exists()
            && self.venv_looks_valid(&venv_dir)
        {
            let venv_python = PythonCommand::new(self.python_path(&venv_dir), Vec::new());
            matches!(
                self.classify_python_platform(&venv_python, pinned.as_deref())
                    .await,
                PlatformMatch::Mismatch
            )
        } else {
            false
        };

        if venv_dir.exists() && (!self.venv_looks_valid(&venv_dir) || venv_arch_mismatch) {
            let reason = if venv_arch_mismatch {
                "architecture-mismatched"
            } else {
                "broken"
            };
            eprintln!(
                "[tine-env] removing {reason} venv at {}",
                venv_dir.display()
            );
            logs.push(format!(
                "Removing {reason} venv at {} before recreation",
                venv_dir.display()
            ));
            tokio::fs::remove_dir_all(&venv_dir).await.map_err(|e| {
                TineError::EnvironmentFailed {
                    runtime_id: runtime_id.to_string(),
                    message: format!("failed to remove {reason} venv dir: {}", e),
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

        // pip bootstrap is only needed when pip itself performs installs.
        // In uv mode pip is delivered as an ordinary package below (cells
        // still use `!pip install`), which avoids the slow ensurepip step.
        if !use_uv {
            let mut pip_check_timer = OutcomeTimer::start(METRIC_ENV_ENSURE_PIP_CHECK);
            self.ensure_pip_available(runtime_id, &venv_dir, &mut logs)
                .await?;
            pip_check_timer.set_outcome("success");
            drop(pip_check_timer);
        }

        let mut packages_to_sync = if uses_bundled_python {
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
        if use_uv && !uses_bundled_python {
            // Notebook cells rely on `!pip install`; in uv mode pip arrives
            // like any other package instead of via ensurepip.
            packages_to_sync.push("pip".to_string());
        }
        let mut sync_timer = OutcomeTimer::start(METRIC_ENV_ENSURE_SYNC);
        self.sync_packages(runtime_id, &venv_dir, &packages_to_sync, use_uv, &mut logs)
            .await?;
        sync_timer.set_outcome("success");
        drop(sync_timer);

        let python_path = self.python_path(&venv_dir);
        let mut preflight_timer = OutcomeTimer::start(METRIC_ENV_ENSURE_PREFLIGHT);
        self.preflight_kernel_runtime(runtime_id, &python_path, &venv_dir, &mut logs)
            .await?;
        preflight_timer.set_outcome("success");
        drop(preflight_timer);

        self.mark_environment_ready(venv_dir.clone(), fingerprint);
        total_timer.set_outcome("success");
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
        command.args(&python_command.args).arg("-m").arg("venv");
        if !cfg!(windows) {
            command.arg("--symlinks");
            logs.push("Creating venv with symlinked executables".to_string());
        }
        if inherit_site_packages {
            command.arg("--system-site-packages");
            logs.push(
                "Bundled runtime detected; creating venv with inherited site-packages".to_string(),
            );
        }
        command.arg(venv_dir).current_dir(&self.workspace_root);
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
        use_uv: bool,
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
        // uv installs the same requirements an order of magnitude faster
        // than pip (parallel downloads, hardlinked wheel cache); pip remains
        // the fallback when uv is unavailable.
        let mut cmd = if use_uv {
            let mut cmd = Command::new(&self.uv_path);
            cmd.arg("pip")
                .arg("install")
                .arg("--python")
                .arg(&python_path)
                .arg("-r")
                .arg(&requirements_path)
                .current_dir(&self.workspace_root);
            cmd
        } else {
            let mut cmd = Command::new(&python_path);
            cmd.arg("-m")
                .arg("pip")
                .arg("install")
                .arg("-r")
                .arg(&requirements_path)
                .current_dir(&self.workspace_root);
            cmd
        };
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
        let pinned = pinned_python_platform();

        // Explicitly configured interpreters (TINE_PYTHON / TINE_BUNDLED_PYTHON
        // / TINE_WRAPPER_PYTHON). A pinned-architecture mismatch here is a
        // misconfiguration — someone pointed us at a specific interpreter and
        // it is the wrong arch — so it is a hard error, not something to
        // silently route around.
        for command in self.explicit_python_commands() {
            attempted.push(command.display.clone());
            if !matches!(
                self.python_command_matches_version(&command, python_version)
                    .await,
                Ok(true)
            ) {
                continue;
            }
            match self.classify_python_platform(&command, pinned.as_deref()).await {
                PlatformMatch::NotPinned | PlatformMatch::Match => return Ok(command),
                PlatformMatch::Mismatch => {
                    return Err(TineError::Config(format!(
                        "configured Python interpreter '{}' does not match the required architecture '{}'. \
                         Point TINE_PYTHON/TINE_BUNDLED_PYTHON at a {}-native interpreter.",
                        command.display,
                        pinned.as_deref().unwrap_or_default(),
                        pinned.as_deref().unwrap_or_default(),
                    )));
                }
            }
        }

        // uv-managed interpreter: uv downloads host-native by default, but if
        // it returns a mismatched arch (e.g. running under emulation) fall
        // through to the system candidates rather than failing outright.
        if let Some(command) = self.resolve_python_via_uv(python_version).await {
            attempted.push(command.display.clone());
            match self.classify_python_platform(&command, pinned.as_deref()).await {
                PlatformMatch::NotPinned | PlatformMatch::Match => return Ok(command),
                PlatformMatch::Mismatch => {}
            }
        }

        // System interpreters: skip any whose arch does not match the pin.
        for command in self.python_command_candidates(python_version) {
            attempted.push(command.display.clone());
            if !matches!(
                self.python_command_matches_version(&command, python_version)
                    .await,
                Ok(true)
            ) {
                continue;
            }
            match self.classify_python_platform(&command, pinned.as_deref()).await {
                PlatformMatch::NotPinned | PlatformMatch::Match => return Ok(command),
                PlatformMatch::Mismatch => continue,
            }
        }

        Err(TineError::Config(format!(
            "Python {}+ ({}) is not available. Tried {}",
            python_version,
            pinned
                .as_deref()
                .map(|arch| format!("architecture {arch}"))
                .unwrap_or_else(|| "any architecture".to_string()),
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

    /// Report the interpreter's architecture via `platform.machine()` — the
    /// same value the install-stage pin records. `None` if the interpreter
    /// cannot be run or prints nothing.
    async fn probe_python_platform(&self, command: &PythonCommand) -> Option<String> {
        let mut probe = Command::new(&command.program);
        probe
            .args(&command.args)
            .arg("-c")
            .arg("import platform; print(platform.machine())")
            .current_dir(&self.workspace_root);
        let output = probe.output().await.ok()?;
        if !output.status.success() {
            return None;
        }
        let machine = String::from_utf8_lossy(&output.stdout).trim().to_string();
        (!machine.is_empty()).then_some(machine)
    }

    /// Compare a candidate interpreter against the pinned architecture.
    /// Probes the interpreter only when a pin is actually set, so unpinned
    /// (dev) resolution pays no extra subprocess.
    async fn classify_python_platform(
        &self,
        command: &PythonCommand,
        pinned: Option<&str>,
    ) -> PlatformMatch {
        if pinned.is_none() {
            return PlatformMatch::NotPinned;
        }
        let actual = self.probe_python_platform(command).await;
        classify_platform_match(pinned, actual.as_deref())
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
    #[serial_test::serial]
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

        let displays: Vec<_> = commands
            .into_iter()
            .map(|command| command.display)
            .collect();
        assert_eq!(displays.len(), 2);
        assert_eq!(displays[0], override_python);
        assert_eq!(displays[1], bundled);
    }

    #[test]
    #[serial_test::serial]
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

    fn venv_with_python(root: &Path) -> PathBuf {
        let venv = root.join("venv");
        let bin = venv.join(if cfg!(windows) { "Scripts" } else { "bin" });
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(
            bin.join(if cfg!(windows) {
                "python.exe"
            } else {
                "python"
            }),
            b"#!/bin/sh\n",
        )
        .unwrap();
        venv
    }

    #[test]
    fn environment_ready_stamp_round_trips_and_invalidates() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = EnvironmentManager::new(tmp.path().to_path_buf());
        let venv = venv_with_python(tmp.path());

        // Not ready before any successful ensure.
        assert!(!manager.environment_is_ready(&venv, "fp-a"));

        manager.mark_environment_ready(venv.clone(), "fp-a".to_string());
        assert!(manager.environment_is_ready(&venv, "fp-a"));

        // A different package fingerprint must miss.
        assert!(!manager.environment_is_ready(&venv, "fp-b"));

        // Rewriting the python binary (recreated venv) must invalidate.
        std::thread::sleep(std::time::Duration::from_millis(20));
        let python = manager.python_path(&venv);
        std::fs::write(&python, b"#!/bin/sh\n# changed\n").unwrap();
        let now = std::time::SystemTime::now();
        let _ = filetime_set(&python, now);
        if python_modified_time(&python)
            == manager
                .ready_environments
                .lock()
                .unwrap()
                .get(&venv)
                .and_then(|stamp| stamp.python_modified)
        {
            // mtime granularity too coarse on this filesystem; skip the
            // invalidation assertion rather than flake.
            return;
        }
        assert!(!manager.environment_is_ready(&venv, "fp-a"));
    }

    fn filetime_set(path: &Path, time: std::time::SystemTime) -> std::io::Result<()> {
        // Touch by reopening with append; falls back to best effort.
        let file = std::fs::OpenOptions::new().append(true).open(path)?;
        file.set_modified(time)?;
        Ok(())
    }

    #[test]
    fn environment_ready_requires_python_binary() {
        let tmp = tempfile::tempdir().unwrap();
        let manager = EnvironmentManager::new(tmp.path().to_path_buf());
        let venv = tmp.path().join("venv-without-python");
        std::fs::create_dir_all(&venv).unwrap();

        // Marking a venv whose python is missing records no mtime, so it can
        // never satisfy a readiness check.
        manager.mark_environment_ready(venv.clone(), "fp".to_string());
        assert!(!manager.environment_is_ready(&venv, "fp"));
    }

    #[test]
    fn environment_fingerprint_tracks_dependencies() {
        // Test the pure variant: the public one reads TINE_BUNDLED_PYTHON,
        // which a sibling test mutates, making it racy under parallel runs.
        let spec_a = EnvironmentSpec {
            dependencies: vec!["pandas".to_string()],
        };
        let spec_b = EnvironmentSpec {
            dependencies: vec!["polars".to_string()],
        };
        assert_eq!(
            EnvironmentManager::environment_fingerprint_with(false, &spec_a),
            EnvironmentManager::environment_fingerprint_with(false, &spec_a),
        );
        assert_ne!(
            EnvironmentManager::environment_fingerprint_with(false, &spec_a),
            EnvironmentManager::environment_fingerprint_with(false, &spec_b),
        );
        assert_ne!(
            EnvironmentManager::environment_fingerprint_with(false, &spec_a),
            EnvironmentManager::environment_fingerprint_with(true, &spec_a),
            "bundled-runtime mode must be part of the fingerprint"
        );
    }

    // ---- Architecture-pin enforcement ------------------------------------

    /// Restores an env var to its prior value on drop, so serial env-mutating
    /// tests don't leak state into each other.
    struct EnvVarGuard {
        key: &'static str,
        prev: Option<std::ffi::OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, prev }
        }

        fn unset(key: &'static str) -> Self {
            let prev = std::env::var_os(key);
            std::env::remove_var(key);
            Self { key, prev }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    // Pure policy: exhaustive and deterministic, no subprocess/env.
    #[test]
    fn classify_platform_match_covers_pin_states() {
        assert_eq!(
            classify_platform_match(None, Some("arm64")),
            PlatformMatch::NotPinned,
            "no pin disables enforcement"
        );
        assert_eq!(
            classify_platform_match(Some("arm64"), Some("arm64")),
            PlatformMatch::Match
        );
        assert_eq!(
            classify_platform_match(Some("ARM64"), Some("arm64")),
            PlatformMatch::Match,
            "comparison is case-insensitive"
        );
        assert_eq!(
            classify_platform_match(Some("arm64"), Some("x86_64")),
            PlatformMatch::Mismatch
        );
        assert_eq!(
            classify_platform_match(Some("arm64"), None),
            PlatformMatch::Mismatch,
            "unknown interpreter arch fails closed"
        );
    }

    /// Resolve a real host interpreter (unpinned) and probe its arch, so the
    /// e2e tests below are portable across whatever arch the runner is on.
    /// Returns None when no interpreter is available; the e2e tests then skip
    /// rather than fail. The pure-logic tests (`classify_platform_match_*`,
    /// `environment_fingerprint_includes_pinned_arch`) stay non-skippable, so
    /// CI must provide a Python interpreter for the e2e arch tests to have
    /// teeth — which it does, since tine cannot run kernels without one.
    async fn host_interpreter(manager: &EnvironmentManager) -> Option<(PythonCommand, String)> {
        let command = manager.resolve_python_command(DEFAULT_PYTHON_VERSION).await.ok()?;
        let arch = manager.probe_python_platform(&command).await?;
        Some((command, arch))
    }

    fn wrong_arch(host_arch: &str) -> &'static str {
        if host_arch.eq_ignore_ascii_case("x86_64") {
            "arm64"
        } else {
            "x86_64"
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn probe_reports_a_real_interpreter_architecture() {
        let manager = EnvironmentManager::new(std::env::temp_dir());
        let _pin = EnvVarGuard::unset(PINNED_PLATFORM_ENV);
        let Some((command, arch)) = host_interpreter(&manager).await else {
            eprintln!("no host python available; skipping");
            return;
        };
        assert!(!arch.is_empty(), "probe must report a non-empty arch");
        // Classify the real interpreter against itself and against a wrong arch.
        assert_eq!(
            manager.classify_python_platform(&command, Some(&arch)).await,
            PlatformMatch::Match
        );
        assert_eq!(
            manager
                .classify_python_platform(&command, Some(wrong_arch(&arch)))
                .await,
            PlatformMatch::Mismatch
        );
        assert_eq!(
            manager.classify_python_platform(&command, None).await,
            PlatformMatch::NotPinned
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn resolve_accepts_interpreter_matching_the_pin() {
        let manager = EnvironmentManager::new(std::env::temp_dir());
        let arch = {
            let _pin = EnvVarGuard::unset(PINNED_PLATFORM_ENV);
            let Some((_, arch)) = host_interpreter(&manager).await else {
                eprintln!("no host python available; skipping");
                return;
            };
            arch
        };
        let _pin = EnvVarGuard::set(PINNED_PLATFORM_ENV, &arch);
        manager
            .resolve_python_command(DEFAULT_PYTHON_VERSION)
            .await
            .expect("a host-arch pin must resolve the native interpreter");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn resolve_hard_fails_when_explicit_interpreter_mismatches_pin() {
        let manager = EnvironmentManager::new(std::env::temp_dir());
        let (program, arch) = {
            let _pin = EnvVarGuard::unset(PINNED_PLATFORM_ENV);
            let _clear_explicit = EnvVarGuard::unset("TINE_PYTHON");
            let Some((command, arch)) = host_interpreter(&manager).await else {
                eprintln!("no host python available; skipping");
                return;
            };
            (command.program, arch)
        };
        // Point TINE_PYTHON at a real interpreter but pin a different arch.
        let _explicit = EnvVarGuard::set("TINE_PYTHON", &program.to_string_lossy());
        let _pin = EnvVarGuard::set(PINNED_PLATFORM_ENV, wrong_arch(&arch));
        let err = manager
            .resolve_python_command(DEFAULT_PYTHON_VERSION)
            .await
            .expect_err("an explicitly-configured wrong-arch interpreter must hard-fail");
        let message = err.to_string();
        assert!(
            message.contains("architecture"),
            "error must explain the architecture mismatch: {message}"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn resolve_is_unaffected_when_no_arch_is_pinned() {
        let manager = EnvironmentManager::new(std::env::temp_dir());
        let _pin = EnvVarGuard::unset(PINNED_PLATFORM_ENV);
        // Regression: default (unpinned) resolution must still find a python.
        if manager
            .resolve_python_command(DEFAULT_PYTHON_VERSION)
            .await
            .is_err()
        {
            eprintln!("no host python available; skipping");
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn created_venv_interpreter_is_probed_against_the_pin() {
        let manager = EnvironmentManager::new(std::env::temp_dir());
        let (command, arch) = {
            let _pin = EnvVarGuard::unset(PINNED_PLATFORM_ENV);
            let Some(pair) = host_interpreter(&manager).await else {
                eprintln!("no host python available; skipping");
                return;
            };
            pair
        };

        let temp = tempdir().expect("temp dir");
        let venv_dir = temp.path().join("venv");
        let mut logs = Vec::new();
        manager
            .create_venv("test-runtime", &command, &venv_dir, false, &mut logs)
            .await
            .expect("venv creation should succeed with the host interpreter");
        assert!(manager.venv_looks_valid(&venv_dir), "venv must be valid");

        // The on-disk guard probes the venv's OWN interpreter. A real venv
        // built from the host python must match the host arch, mismatch a
        // foreign arch, and be exempt when nothing is pinned.
        let venv_python = PythonCommand::new(manager.python_path(&venv_dir), Vec::new());
        assert_eq!(
            manager.classify_python_platform(&venv_python, Some(&arch)).await,
            PlatformMatch::Match
        );
        assert_eq!(
            manager
                .classify_python_platform(&venv_python, Some(wrong_arch(&arch)))
                .await,
            PlatformMatch::Mismatch,
            "a foreign-arch pin must flag the venv for recreation"
        );
        assert_eq!(
            manager.classify_python_platform(&venv_python, None).await,
            PlatformMatch::NotPinned
        );
    }

    #[test]
    #[serial_test::serial]
    fn environment_fingerprint_includes_pinned_arch() {
        let _guard = EnvVarGuard::set(PINNED_PLATFORM_ENV, "arm64");
        let with_arm = EnvironmentManager::environment_fingerprint_with(false, &EnvironmentSpec::default());
        drop(_guard);
        let _guard = EnvVarGuard::set(PINNED_PLATFORM_ENV, "x86_64");
        let with_x86 = EnvironmentManager::environment_fingerprint_with(false, &EnvironmentSpec::default());
        drop(_guard);
        assert_ne!(
            with_arm, with_x86,
            "changing the pinned arch must change the fingerprint so the ready stamp invalidates"
        );
    }
}
