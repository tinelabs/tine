export function fileQuery(path, projectId) {
  const params = new URLSearchParams();
  params.set("path", path || "");
  if (projectId) params.set("project_id", projectId);
  return params.toString();
}

export function hasHttpOrigin(locationLike) {
  const protocol = String(locationLike?.protocol || "");
  return protocol === "http:" || protocol === "https:";
}

export async function resolveApiBaseUrl({
  locationLike,
  hasDesktopBridge,
  invoke,
  retryDelayMs = 250,
  retryLimit = 20,
  desktopHost = "127.0.0.1",
  sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
}) {
  if (hasHttpOrigin(locationLike) && locationLike?.host) {
    return `${locationLike.protocol}//${locationLike.host}`;
  }

  if (!hasDesktopBridge || typeof invoke !== "function") {
    return "";
  }

  const info = await resolveServerInfo({
    invoke,
    retryDelayMs,
    retryLimit,
    sleep,
  });
  return `http://${desktopHost}:${info.port}`;
}

export async function resolveServerInfo({
  invoke,
  retryDelayMs = 250,
  retryLimit = 20,
  sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms)),
}) {
  let lastError = null;
  for (let attempt = 0; attempt < retryLimit; attempt += 1) {
    try {
      const info = await invoke("server_info");
      if (info && info.port) {
        return {
          port: info.port,
          preferredPort: info.preferredPort ?? info.port,
          fellBack: Boolean(info.fellBack),
        };
      }
    } catch (error) {
      lastError = error;
    }
    await sleep(retryDelayMs);
  }
  throw lastError || new Error("embedded server info unavailable");
}

export function resolveApiUrl(path, baseUrl) {
  return baseUrl ? new URL(path, `${baseUrl}/`).toString() : path;
}

export function resolveWebSocketUrl(locationLike, baseUrl) {
  if (baseUrl) {
    const url = new URL(baseUrl);
    url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
    url.pathname = "/ws";
    url.search = "";
    url.hash = "";
    return url.toString();
  }
  return `${locationLike?.protocol === "https:" ? "wss" : "ws"}://${locationLike?.host || ""}/ws`;
}

export function normalizeFileTreePath(path) {
  if (!path || path === "/" || path === ".") return "";
  return String(path).replace(/^\/+/, "").replace(/\/+$/, "");
}

export function watchedDirForPath(path) {
  const normalized = String(path || "").replace(/^\/+/, "").replace(/\/+$/, "");
  const idx = normalized.lastIndexOf("/");
  if (idx < 0) return "";
  return normalizeFileTreePath(normalized.slice(0, idx));
}

export function pickActiveBranchId(tree, currentBranchId) {
  const branches = tree?.branches || [];
  if (currentBranchId && branches.some(branch => branch.id === currentBranchId)) {
    return currentBranchId;
  }
  return tree?.root_branch_id || "main";
}

export function activeBranchPathCellIds(tree, branchId) {
  if (!tree?.branches?.length) return [];
  const branchById = new Map((tree.branches || []).map(branch => [branch.id, branch]));
  const targetBranchId = pickActiveBranchId(tree, branchId);
  const lineage = [];
  let current = branchById.get(targetBranchId) || null;
  while (current) {
    lineage.push(current);
    current = current.parent_branch_id ? branchById.get(current.parent_branch_id) || null : null;
  }
  lineage.reverse();

  const ordered = [];
  for (let i = 0; i < lineage.length; i += 1) {
    const branch = lineage[i];
    const next = lineage[i + 1] || null;
    const order = branch.cell_order || [];
    if (next?.branch_point_cell_id) {
      const stopIdx = order.indexOf(next.branch_point_cell_id);
      if (stopIdx >= 0) ordered.push(...order.slice(0, stopIdx + 1));
      continue;
    }
    ordered.push(...order);
  }
  return ordered;
}

export function normalizeSavedExperimentTreePayload(payload, fallbackDefinition = null) {
  if (payload && typeof payload === "object") {
    if (payload.experiment && typeof payload.experiment === "object") {
      return normalizeSavedExperimentTreePayload(payload.experiment, fallbackDefinition);
    }
    if (payload.id && Array.isArray(payload.cells)) {
      return payload;
    }
    if (payload.id && fallbackDefinition && typeof fallbackDefinition === "object") {
      return {
        ...fallbackDefinition,
        id: payload.id,
      };
    }
  }
  return fallbackDefinition;
}

const EXECUTION_PHASE_LABELS = {
  queued: "Queued",
  preparing_environment: "Preparing environment",
  acquiring_runtime: "Acquiring runtime",
  replaying_context: "Replaying context",
  running: "Running",
  cancellation_requested: "Cancelling",
  serializing_artifacts: "Serializing artifacts",
  retrying: "Retrying",
  completed: "Completed",
  failed: "Failed",
  cancelled: "Cancelled",
  rejected: "Rejected",
  timed_out: "Timed out",
};

function normalizedExecutionPhaseParts(status) {
  return {
    phase: String(status?.phase || "").trim().toLowerCase(),
    lifecycle: String(status?.status || "").trim().toLowerCase(),
    queuePosition: Number(status?.queue_position),
  };
}

function lowerCaseFirst(text) {
  if (!text) return "";
  return text.charAt(0).toLowerCase() + text.slice(1);
}

function executionTargetLabel(status, executionTarget) {
  const branchId = status?.branch_id || executionTarget?.branchId || null;
  const treeId = status?.tree_id || executionTarget?.treeId || null;
  const executionId = status?.execution_id || null;
  if (branchId) return `Branch ${branchId}`;
  if (treeId) return `Tree ${treeId}`;
  if (executionId) return `Execution ${executionId}`;
  return "Execution";
}

export function describeExecutionProgress(status, fallbackCellStatus = "idle") {
  const { phase, lifecycle } = normalizedExecutionPhaseParts(status);
  const fallback = String(fallbackCellStatus || "").trim().toLowerCase();
  const effectivePhase = phase || lifecycle || fallback;
  const queuePosition = Number(status?.queue_position);
  const hasQueuePosition = Number.isFinite(queuePosition) && queuePosition > 0;

  if (effectivePhase === "queued") {
    return {
      label: hasQueuePosition ? `Queued #${queuePosition}` : "Queued",
      message: hasQueuePosition ? `Queued. Position ${queuePosition}.` : "Queued…",
      active: true,
    };
  }

  const label =
    EXECUTION_PHASE_LABELS[effectivePhase] ||
    (effectivePhase
      ? effectivePhase
          .split("_")
          .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
          .join(" ")
      : "Idle");

  const active = [
    "preparing_environment",
    "acquiring_runtime",
    "replaying_context",
    "cancellation_requested",
    "serializing_artifacts",
    "retrying",
    "running",
    "saving",
  ].includes(effectivePhase);

  return {
    label,
    message: active ? `${label}…` : label,
    active,
  };
}

export function executionStatusLevel(status) {
  const { phase, lifecycle } = normalizedExecutionPhaseParts(status);
  const effectivePhase = phase || lifecycle;
  if (["failed", "timed_out", "rejected"].includes(effectivePhase)) {
    return "error";
  }
  if (["cancelled", "cancellation_requested"].includes(effectivePhase)) {
    return "warn";
  }
  return "info";
}

export function buildExecutionStatusEvent(previousStatus, nextStatus, executionTarget = null) {
  if (!nextStatus || typeof nextStatus !== "object") return null;

  const previous = normalizedExecutionPhaseParts(previousStatus);
  const next = normalizedExecutionPhaseParts(nextStatus);
  const effectivePhase = next.phase || next.lifecycle;
  if (!effectivePhase) return null;

  const previousQueue = Number.isFinite(previous.queuePosition)
    ? previous.queuePosition
    : null;
  const nextQueue = Number.isFinite(next.queuePosition) ? next.queuePosition : null;

  if (
    previous.phase === next.phase &&
    previous.lifecycle === next.lifecycle &&
    previousQueue === nextQueue
  ) {
    return null;
  }

  const progress = describeExecutionProgress(nextStatus, next.lifecycle || "idle");
  const label = executionTargetLabel(nextStatus, executionTarget);
  return {
    level: executionStatusLevel(nextStatus),
    kind: "execution",
    status: effectivePhase,
    scope: {
      executionId: nextStatus.execution_id || null,
      treeId: nextStatus.tree_id || executionTarget?.treeId || null,
      branchId: nextStatus.branch_id || executionTarget?.branchId || null,
      nodeId: null,
      runtimeId: null,
    },
    message: `${label} ${lowerCaseFirst(progress.message)}`,
  };
}
