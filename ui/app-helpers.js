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

export function desktopMcpCommand(serverInfo) {
  if (!serverInfo?.port) return null;
  return `tine-mcp --api-url http://127.0.0.1:${serverInfo.port}`;
}

function stripAnsiText(text) {
  return String(text || "").replace(/\x1b\[[0-9;]*m/g, "");
}

function normalizeLogText(text) {
  return stripAnsiText(text).trim();
}

export function shouldRenderStderr(logs) {
  const stderr = normalizeLogText(logs?.stderr || "");
  if (!stderr) return false;

  const error = logs?.error || null;
  if (!error) return true;

  const errorSummary = normalizeLogText(
    `${error.ename || "Error"}${error.evalue ? `: ${error.evalue}` : ""}`,
  );
  const traceback = normalizeLogText(
    Array.isArray(error.traceback) ? error.traceback.join("\n") : "",
  );

  if (stderr === errorSummary || stderr === traceback) {
    return false;
  }
  if (traceback && (traceback.includes(stderr) || stderr.includes(traceback))) {
    return false;
  }

  return true;
}

function terminalEventSeverity(level) {
  switch (String(level || "info").trim().toLowerCase()) {
    case "error":
      return 3;
    case "warn":
    case "warning":
      return 2;
    default:
      return 1;
  }
}

function sameTerminalScope(left = {}, right = {}) {
  return (
    (left.executionId || null) === (right.executionId || null) &&
    (left.runtimeId || null) === (right.runtimeId || null) &&
    (left.treeId || null) === (right.treeId || null) &&
    (left.branchId || null) === (right.branchId || null) &&
    (left.nodeId || null) === (right.nodeId || null)
  );
}

function isTerminalFailureStatus(status) {
  return ["failed", "timed_out", "rejected"].includes(
    String(status || "").trim().toLowerCase(),
  );
}

export function shouldCoalesceTerminalEvents(previousEvent, nextEvent) {
  if (!previousEvent || !nextEvent) return false;
  return (
    String(previousEvent.kind || "") === String(nextEvent.kind || "") &&
    String(previousEvent.status || "") === String(nextEvent.status || "") &&
    String(previousEvent.stream || "") === String(nextEvent.stream || "") &&
    String(previousEvent.message || "") === String(nextEvent.message || "") &&
    String(previousEvent.error?.ename || "") === String(nextEvent.error?.ename || "") &&
    String(previousEvent.error?.evalue || "") === String(nextEvent.error?.evalue || "") &&
    sameTerminalScope(previousEvent.scope, nextEvent.scope)
  );
}

export function shouldReplaceTerminalEvent(previousEvent, nextEvent) {
  if (!previousEvent || !nextEvent) return false;

  return (
    String(previousEvent.kind || "") === String(nextEvent.kind || "") &&
    sameTerminalScope(previousEvent.scope, nextEvent.scope) &&
    terminalEventSeverity(nextEvent.level) > terminalEventSeverity(previousEvent.level) &&
    isTerminalFailureStatus(nextEvent.status)
  );
}

export function appendTerminalEvent(events, nextEvent, maxEvents = 400) {
  const existingEvents = Array.isArray(events) ? events : [];
  if (!nextEvent) return existingEvents.slice(-maxEvents);

  const previousEvent = existingEvents[existingEvents.length - 1] || null;
  if (
    !shouldCoalesceTerminalEvents(previousEvent, nextEvent) &&
    !shouldReplaceTerminalEvent(previousEvent, nextEvent)
  ) {
    return [...existingEvents, nextEvent].slice(-maxEvents);
  }

  const keepNext = terminalEventSeverity(nextEvent.level) >= terminalEventSeverity(previousEvent.level);
  const mergedEvent = keepNext
    ? {
        ...previousEvent,
        ...nextEvent,
        id: nextEvent.id || previousEvent.id,
      }
    : {
        ...nextEvent,
        ...previousEvent,
        id: previousEvent.id,
      };

  return [...existingEvents.slice(0, -1), mergedEvent].slice(-maxEvents);
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

export function fileTreeRefreshKey(dirPath = "", projectId = null) {
  const dirKey = normalizeFileTreePath(dirPath);
  return `${projectId || "workspace"}:${dirKey || "."}`;
}

export function registerPendingFileTreeRefresh(
  pendingRefreshKeys,
  dirPath = "",
  projectId = null,
) {
  const nextPendingRefreshKeys = new Set(pendingRefreshKeys || []);
  const refreshKey = fileTreeRefreshKey(dirPath, projectId);
  const shouldSchedule = !nextPendingRefreshKeys.has(refreshKey);
  nextPendingRefreshKeys.add(refreshKey);
  return {
    refreshKey,
    shouldSchedule,
    pendingRefreshKeys: nextPendingRefreshKeys,
  };
}

export function pickActiveBranchId(tree, currentBranchId, runtimeState = null) {
  const branches = tree?.branches || [];
  if (currentBranchId && branches.some(branch => branch.id === currentBranchId)) {
    return currentBranchId;
  }
  const persistedBranchId =
    runtimeState?.active_branch_id || runtimeState?.activeBranchId || null;
  if (persistedBranchId && branches.some(branch => branch.id === persistedBranchId)) {
    return persistedBranchId;
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

export function branchRequiresReplay({
  treeId,
  branchId,
  runtimeState = null,
  runtimeHealth = null,
  executionStatuses = null,
}) {
  const kernelState = String(
    runtimeState?.kernel_state || runtimeState?.kernelState || "",
  )
    .trim()
    .toLowerCase();
  const hasLiveKernel =
    runtimeHealth?.has_live_kernel ?? runtimeHealth?.hasLiveKernel ?? null;
  if (["kernel_lost", "switching"].includes(kernelState)) {
    return true;
  }
  if (kernelState === "needs_replay" && hasLiveKernel !== false) {
    return true;
  }

  return Object.values(executionStatuses || {}).some((status) => {
    const statusTreeId = status?.tree_id || status?.treeId || null;
    const statusBranchId = status?.branch_id || status?.branchId || null;
    const runtimeHasLiveKernel =
      status?.runtime?.has_live_kernel ?? status?.runtime?.hasLiveKernel ?? null;
    return (
      statusTreeId === treeId &&
      statusBranchId === branchId &&
      Boolean(status?.runtime?.replay_required) &&
      runtimeHasLiveKernel !== false
    );
  });
}

export function markReplayRequiredCellStatuses(
  cellStatuses,
  { treeId, branchId, nodeIds = [] },
) {
  let changed = false;
  const nextStatuses = { ...cellStatuses };
  for (const nodeId of nodeIds) {
    const cellKey = `${treeId}_${branchId}_${nodeId}`;
    if (["done", "cached"].includes(nextStatuses[cellKey])) {
      nextStatuses[cellKey] = "stale";
      changed = true;
    }
  }
  return changed ? nextStatuses : cellStatuses;
}

// Backoff schedule for execution status polling. Polling has no iteration
// cap — a long-running execution with a dropped WebSocket must keep
// converging — so the interval grows to bound load: 1s at first for snappy
// short executions, settling at 5s for long ones.
export function nextPollDelay(prevDelayMs) {
  const base =
    Number.isFinite(prevDelayMs) && prevDelayMs > 0 ? prevDelayMs : 1000;
  return Math.min(Math.round(base * 1.5), 5000);
}

export function reconnectResyncTargets({
  activePollIds = {},
  executionTargets = {},
}) {
  return Object.entries(activePollIds)
    .filter(([, active]) => active !== false)
    .map(([executionId]) => ({
      executionId,
      target: executionTargets[executionId] || null,
    }))
    .filter((item) => Boolean(item.executionId));
}

export function nextAsyncRequestId(previousId = 0) {
  const normalizedPreviousId = Number(previousId);
  if (!Number.isFinite(normalizedPreviousId) || normalizedPreviousId < 0) {
    return 1;
  }
  return Math.floor(normalizedPreviousId) + 1;
}

export function shouldApplyScopedRequestResult({
  requestId,
  latestRequestId,
  requestScope = null,
  currentScope = null,
}) {
  if (!Number.isFinite(Number(requestId)) || !Number.isFinite(Number(latestRequestId))) {
    return false;
  }
  if (Number(requestId) !== Number(latestRequestId)) {
    return false;
  }
  return String(requestScope || "") === String(currentScope || "");
}

export function nodeStatusToCellStatus(status) {
  switch (String(status || "")) {
    case "queued":
    case "pending":
      return "queued";
    case "running":
      return "running";
    case "completed":
      return "done";
    case "cache_hit":
      return "cached";
    case "failed":
    case "interrupted":
      return "failed";
    case "skipped":
      return "idle";
    default:
      return null;
  }
}

function snapshotCellKey({ runtimeId = null, treeId = null, branchId = null, nodeId }) {
  if (!nodeId) return null;
  if (treeId && branchId) return `${treeId}_${branchId}_${nodeId}`;
  const fallbackRuntimeId = treeId || runtimeId;
  return fallbackRuntimeId ? `${fallbackRuntimeId}_${nodeId}` : null;
}

export function applyExecutionSnapshotToState(
  state,
  { executionId, status, target = null, runtimeId = null },
) {
  if (!executionId || !status) return state;

  const previousState = state || {};
  const treeId = status.tree_id || target?.treeId || null;
  const branchId = status.branch_id || target?.branchId || null;
  const targetKind = status.target_kind || target?.targetKind || null;
  const normalizedStatus = {
    ...status,
    execution_id: status.execution_id || executionId,
    tree_id: treeId,
    branch_id: branchId,
    target_kind: targetKind,
  };
  const nextExecutionTargets = {
    ...(previousState.executionTargets || {}),
    [executionId]: {
      treeId,
      branchId,
      targetKind,
    },
  };
  const nextExecutionStatuses = {
    ...(previousState.executionStatuses || {}),
    [executionId]: normalizedStatus,
  };
  const nextCellStatuses = { ...(previousState.cellStatuses || {}) };

  for (const nodeId of Object.keys(status.node_statuses || {})) {
    const cellKey = snapshotCellKey({ runtimeId, treeId, branchId, nodeId });
    if (!cellKey) continue;
    const nextStatus = nodeStatusToCellStatus(status.node_statuses?.[nodeId]);
    if (nextStatus) nextCellStatuses[cellKey] = nextStatus;
  }

  return {
    ...previousState,
    executionTargets: nextExecutionTargets,
    executionStatuses: nextExecutionStatuses,
    cellStatuses: nextCellStatuses,
  };
}

export function finishTrackedExecutionState(state, executionId) {
  if (!executionId) return state;
  const previousState = state || {};
  const activePollIds = { ...(previousState.activePollIds || {}) };
  delete activePollIds[executionId];
  const executionTargets = { ...(previousState.executionTargets || {}) };
  const executionStatuses = { ...(previousState.executionStatuses || {}) };
  delete executionTargets[executionId];
  delete executionStatuses[executionId];
  const activeCellExecutions = { ...(previousState.activeCellExecutions || {}) };
  for (const [cellKey, currentExecutionId] of Object.entries(activeCellExecutions)) {
    if (currentExecutionId === executionId) delete activeCellExecutions[cellKey];
  }

  return {
    ...previousState,
    activePollIds,
    executionTargets,
    executionStatuses,
    activeCellExecutions,
  };
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

  const skippedAfterEarlierBranchFailure =
    effectivePhase === "failed" &&
    (previous.phase === "queued" || previous.lifecycle === "queued") &&
    Object.keys(nextStatus.node_statuses || {}).length === 0;

  if (skippedAfterEarlierBranchFailure) {
    return {
      level: "warn",
      kind: "execution",
      status: "rejected",
      scope: {
        executionId: nextStatus.execution_id || null,
        treeId: nextStatus.tree_id || executionTarget?.treeId || null,
        branchId: nextStatus.branch_id || executionTarget?.branchId || null,
        nodeId: null,
        runtimeId: null,
      },
      message: "stopped after earlier branch failure",
    };
  }

  const progress = describeExecutionProgress(nextStatus, next.lifecycle || "idle");
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
    message: lowerCaseFirst(progress.message),
  };
}

export function shouldRequestExecutionResync({
  eventType,
  executionId,
  cellKey,
  executionTarget = null,
}) {
  if (!executionId) return false;
  if (cellKey) return false;
  if (executionTarget?.treeId && executionTarget?.branchId) return false;
  return [
    "NodeStarted",
    "NodeStream",
    "NodeDisplayData",
    "NodeDisplayUpdate",
    "NodeCompleted",
    "NodeCacheHit",
    "NodeFailed",
  ].includes(String(eventType || ""));
}

function hasRenderableLogs(logs) {
  return !!(
    logs &&
    (logs.stdout ||
      logs.stderr ||
      logs.error ||
      (Array.isArray(logs.outputs) && logs.outputs.length) ||
      (logs.metrics && Object.keys(logs.metrics).length) ||
      logs.duration_ms != null)
  );
}

export function shouldHydrateTerminalLogs({
  status,
  logs,
  hydrationKey,
  loadedHydrationKey,
  isTreeExecutionRuntime,
  treeId,
  branchId,
}) {
  const normalizedStatus = String(status || "").trim().toLowerCase();
  if (!["done", "failed", "cached"].includes(normalizedStatus)) {
    return false;
  }
  if (!isTreeExecutionRuntime || !treeId || !branchId || !hydrationKey) {
    return false;
  }
  if (loadedHydrationKey === hydrationKey) {
    return false;
  }
  if (hasRenderableLogs(logs)) {
    return false;
  }
  return true;
}

const RUNTIME_UI_DEFAULT = Object.freeze({
  tone: "muted",
  label: "Off",
  title: "Kernel off. The next branch run starts from a fresh kernel.",
  runBlocked: false,
  menuActions: [],
});

const RESTART_ACTION = Object.freeze({
  id: "restart",
  label: "Restart kernel",
  kind: "normal",
});
const SHUTDOWN_ACTION = Object.freeze({
  id: "shutdown",
  label: "Turn off kernel",
  kind: "danger",
});

export function deriveRuntimeUi({
  kernelState = "",
  hasLiveKernel = false,
  isBusy = false,
} = {}) {
  const state = String(kernelState || "").trim().toLowerCase();
  if (isBusy) {
    return {
      tone: "busy",
      label: "Busy",
      title: "Kernel busy. A run is using this branch runtime.",
      runBlocked: false,
      menuActions: [],
    };
  }
  if (state === "kernel_lost") {
    return {
      tone: "error",
      label: "Lost",
      title: "Kernel lost. Restart to recover this runtime.",
      runBlocked: true,
      menuActions: [RESTART_ACTION],
    };
  }
  if (state === "switching") {
    return {
      tone: "busy",
      label: "Preparing",
      title: "Preparing runtime. Branch context is being prepared.",
      runBlocked: true,
      menuActions: [],
    };
  }
  if (!hasLiveKernel) {
    return { ...RUNTIME_UI_DEFAULT };
  }
  if (state === "needs_replay") {
    return {
      tone: "warn",
      label: "Replay required",
      title:
        "Replay required. Live kernel context is stale. Restart or turn it off to start clean.",
      runBlocked: true,
      menuActions: [RESTART_ACTION, SHUTDOWN_ACTION],
    };
  }
  return {
    tone: "ready",
    label: "Ready",
    title: "Kernel ready.",
    runBlocked: false,
    menuActions: [RESTART_ACTION, SHUTDOWN_ACTION],
  };
}
