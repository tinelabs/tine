/*
    tine — Infinite Canvas Workbench
    Layout: Sidebar (Experiments + Files) | Main (Canvas/Notebook)

    Migration note:
    notebook execution and branching now use the tree/branch/cell model
    end to end, and the UI should treat experiment trees as the primary
    notebook abstraction.

    Glossary:
    - "experiment" in UI copy should mean experiment tree / separate runtime
    - "branch" should mean an in-tree path inside the selected experiment tree
*/
import { h, render } from "preact";
import { useState, useEffect, useRef, useCallback } from "preact/hooks";
import htm from "htm";
import DOMPurify from "dompurify";
import CodeMirror from "https://esm.sh/@uiw/react-codemirror@4.23.6?alias=react:preact/compat,react-dom:preact/compat&deps=preact@10.25.4";
import { python } from "https://esm.sh/@codemirror/lang-python@6.1.6";
import { createTheme } from "https://esm.sh/@uiw/codemirror-themes@4.23.6";
import { tags as t } from "https://esm.sh/@lezer/highlight@1.2.1";
import hljs from "https://esm.sh/highlight.js@11.11.1/lib/core";
import pythonLanguage from "https://esm.sh/highlight.js@11.11.1/lib/languages/python";
import {
  ArrowDown,
  ArrowLeft,
  ArrowUp,
  ChevronLeft,
  ChevronRight,
  Copy,
  Download,
  FolderSearch,
  GitBranch,
  Loader,
  MoonStar,
  Play,
  Plus,
  Trash2,
} from "https://esm.sh/lucide-preact@0.511.0";
import {
  activeBranchPathCellIds,
  buildExecutionStatusEvent,
  describeExecutionProgress,
  fileQuery,
  hasHttpOrigin,
  normalizeFileTreePath,
  pickActiveBranchId,
  resolveApiBaseUrl,
  resolveApiUrl,
  resolveWebSocketUrl,
  watchedDirForPath,
} from "./app-helpers.js";

const html = htm.bind(h);
const MAX_TERMINAL_EVENTS = 400;
const TEXT_PREVIEW_LIMIT_BYTES = 512 * 1024;
let terminalEventCounter = 0;

const CODEMIRROR_THEME_SETTINGS = {
  background: "var(--cm-bg)",
  backgroundImage: "none",
  foreground: "var(--fg)",
  caret: "var(--cm-cursor)",
  selection: "var(--cm-selection)",
  selectionMatch: "var(--cm-selection)",
  lineHighlight: "transparent",
  gutterBackground: "var(--cm-bg)",
  gutterForeground: "var(--fg-2)",
};

const CODEMIRROR_THEME_STYLES = [
  { tag: t.comment, color: "var(--fg-3)", fontStyle: "italic" },
  { tag: [t.string, t.special(t.string)], color: "var(--green)" },
  { tag: [t.number, t.integer, t.float, t.bool, t.null], color: "var(--yellow)" },
  { tag: [t.keyword, t.operatorKeyword, t.modifier], color: "var(--purple)" },
  { tag: [t.definitionKeyword, t.controlKeyword], color: "var(--purple)" },
  { tag: [t.variableName, t.name], color: "var(--fg)" },
  { tag: [t.definition(t.variableName), t.function(t.variableName)], color: "var(--blue)" },
  { tag: [t.typeName, t.className, t.namespace], color: "var(--blue)" },
  { tag: [t.propertyName, t.attributeName], color: "var(--accent-dim)" },
  { tag: [t.punctuation, t.separator, t.bracket], color: "var(--fg-2)" },
  { tag: [t.meta, t.annotation], color: "var(--accent-dim)" },
];

const CM_LIGHT_THEME = createTheme({
  theme: "light",
  settings: CODEMIRROR_THEME_SETTINGS,
  styles: CODEMIRROR_THEME_STYLES,
});

const CM_DARK_THEME = createTheme({
  theme: "dark",
  settings: CODEMIRROR_THEME_SETTINGS,
  styles: CODEMIRROR_THEME_STYLES,
});

if (!hljs.getLanguage("python")) {
  hljs.registerLanguage("python", pythonLanguage);
}

function currentThemeMode() {
  if (typeof document !== "undefined") {
    const attr = document.documentElement.getAttribute("data-theme");
    if (attr === "dark" || attr === "light") return attr;
  }
  try {
    if (typeof localStorage !== "undefined") {
      const saved = localStorage.getItem("tine-theme");
      if (saved === "dark" || saved === "light") return saved;
    }
  } catch {}
  return "light";
}

if (typeof document !== "undefined") {
  document.documentElement.setAttribute("data-theme", currentThemeMode());
}

function loadSidebarCollapsed() {
  try {
    return localStorage.getItem("tine-sidebar-collapsed") === "1";
  } catch {
    return false;
  }
}

function persistSidebarCollapsed(collapsed) {
  try {
    localStorage.setItem("tine-sidebar-collapsed", collapsed ? "1" : "0");
  } catch {}
}

function nextTerminalEventId() {
  terminalEventCounter += 1;
  return `term-${Date.now()}-${terminalEventCounter}`;
}

function normalizeTerminalScope(scope = {}) {
  return {
    executionId: scope.executionId || null,
    runtimeId: scope.runtimeId || null,
    treeId: scope.treeId || null,
    branchId: scope.branchId || null,
    nodeId: scope.nodeId || null,
  };
}

function executionEventScope(data = {}) {
  return normalizeTerminalScope({
    executionId: data.execution_id,
    treeId: data.tree_id,
    branchId: data.branch_id,
    nodeId: data.node_id,
  });
}

function normalizeTerminalMetrics(metrics) {
  if (!metrics || typeof metrics !== "object") return null;
  const pairs = Object.entries(metrics).filter(
    ([, value]) => typeof value === "number" && Number.isFinite(value),
  );
  return pairs.length ? Object.fromEntries(pairs) : null;
}

function normalizeTerminalError(error) {
  if (!error) return null;
  return {
    ename: String(error.ename || "Error"),
    evalue: String(error.evalue || error.message || ""),
    traceback: Array.isArray(error.traceback) ? error.traceback : [],
  };
}

function normalizeTerminalEvent(entry, level = "info") {
  const raw =
    typeof entry === "string" ? { message: entry, level } : { ...(entry || {}) };
  return {
    id: raw.id || nextTerminalEventId(),
    ts: raw.ts ?? Date.now(),
    level: raw.level || level || "info",
    kind: raw.kind || "system",
    scope: normalizeTerminalScope(raw.scope),
    stream: raw.stream || null,
    message: raw.message != null ? String(raw.message) : null,
    status: raw.status || null,
    metrics: normalizeTerminalMetrics(raw.metrics),
    error: normalizeTerminalError(raw.error),
    duration_ms: raw.duration_ms ?? null,
  };
}

function appendTerminalEvent(events, event) {
  return [...(events || []), event].slice(-MAX_TERMINAL_EVENTS);
}

function terminalEventSummary(event) {
  if (event.message) return event.message;
  const target =
    event.scope.nodeId ||
    event.scope.branchId ||
    event.scope.treeId ||
    event.scope.runtimeId ||
    event.scope.executionId ||
    event.kind;
  if (event.kind === "node") {
    if (event.status === "started") return `${target} started`;
    if (event.status === "done")
      return `${target} done${event.duration_ms != null ? ` (${event.duration_ms}ms)` : ""}`;
    if (event.status === "cached") return `${target} cache hit`;
    if (event.status === "failed")
      return `${target} failed${event.error?.evalue ? `: ${event.error.evalue}` : ""}`;
  }
  if (event.kind === "execution") {
    if (event.status === "started") return `${target} started`;
    if (event.status === "done")
      return `${target} done${event.duration_ms != null ? ` (${event.duration_ms}ms)` : ""}`;
    if (event.status === "failed") return `${target} failed`;
  }
  if (event.kind === "runtime_state") {
    if (event.status === "switching")
      return `${target} switching runtime context`;
    if (event.status === "ready") return `${target} runtime ready`;
    if (event.status === "needs_replay") return `${target} marked for replay`;
    if (event.status === "kernel_lost") return `${target} kernel lost`;
  }
  if (event.error?.evalue) return event.error.evalue;
  return target;
}

function terminalEventBadges(event) {
  const badges = [];
  if (event.kind && event.kind !== "system") badges.push(event.kind);
  if (event.status) badges.push(event.status);
  if (event.scope.runtimeId) badges.push(`runtime:${event.scope.runtimeId}`);
  if (event.scope.treeId) badges.push(`tree:${event.scope.treeId}`);
  if (event.scope.branchId) badges.push(`branch:${event.scope.branchId}`);
  if (event.scope.nodeId) badges.push(`node:${event.scope.nodeId}`);
  if (event.stream) badges.push(event.stream);
  return badges;
}

// ── API ───────────────────────────────────────────────────────
async function fetchJSON(method, url, body) {
  const opts = { method, headers: {} };
  if (body) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await apiFetch(url, opts);
  if (!res.ok) {
    const text = await res.text();
    let data = null;
    try {
      data = text ? JSON.parse(text) : null;
    } catch {
      data = null;
    }
    const err = new Error(data?.error || `${res.status}: ${text}`);
    err.status = res.status;
    err.data = data;
    throw err;
  }
  if (res.status === 204) return null;
  const text = await res.text();
  if (!text.trim()) return null;
  return JSON.parse(text);
}

function filenameFromDisposition(disposition, fallbackName) {
  if (!disposition) return fallbackName;
  const utf8Match = disposition.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) return decodeURIComponent(utf8Match[1]);
  const quotedMatch = disposition.match(/filename="([^"]+)"/i);
  if (quotedMatch?.[1]) return quotedMatch[1];
  const bareMatch = disposition.match(/filename=([^;]+)/i);
  return bareMatch?.[1]?.trim() || fallbackName;
}

function downloadBlob(blob, filename) {
  const href = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(href), 0);
}

async function saveTextExport(contents, filename) {
  if (hasDesktopBridge()) {
    const savedPath = await tauriInvoke("save_export_file", {
      suggestedName: filename,
      contents,
    });
    return savedPath || null;
  }
  downloadBlob(
    new Blob([contents], { type: "application/octet-stream;charset=utf-8" }),
    filename,
  );
  return null;
}

function codeSourceToLines(source) {
  const text = String(source || "");
  if (!text) return [];
  return text.split(/(?<=\n)/);
}

function treeBranchExportCells(tree, branchId) {
  const orderedIds = activeBranchPathCellIds(tree, branchId);
  const cellById = new Map((tree?.cells || []).map((cell) => [cell.id, cell]));
  return orderedIds.map((cellId) => cellById.get(cellId)).filter(Boolean);
}

function buildBranchPythonExport(tree, branch, cells) {
  const header = [
    "# Exported from Tine",
    `# Tree: ${tree.id}`,
    `# Branch: ${branch.name || branch.id}`,
    "",
  ].join("\n");
  const body = cells
    .map((cell) => String(cell.code?.source || "").replace(/\s+$/u, ""))
    .join("\n\n");
  return body ? `${header}${body}\n` : `${header}\n`;
}

function buildBranchNotebookExport(tree, branch, cells) {
  return {
    cells: cells.map((cell) => ({
      cell_type: "code",
      execution_count: null,
      metadata: {
        tine: {
          cell_id: cell.id,
          branch_id: branch.id,
          tree_id: tree.id,
        },
      },
      outputs: [],
      source: codeSourceToLines(cell.code?.source || ""),
    })),
    metadata: {
      kernelspec: {
        display_name: "Python 3",
        language: "python",
        name: "python3",
      },
      language_info: {
        name: "python",
      },
      tine: {
        tree_id: tree.id,
        branch_id: branch.id,
        branch_name: branch.name || branch.id,
      },
    },
    nbformat: 4,
    nbformat_minor: 5,
  };
}

function resolveDesktopInvoke() {
  const tauriInvoke = globalThis.__TAURI__?.core?.invoke;
  if (typeof tauriInvoke === "function") return tauriInvoke;
  const internalsInvoke = globalThis.__TAURI_INTERNALS__?.invoke;
  if (typeof internalsInvoke === "function") return internalsInvoke;
  return null;
}

function tauriInvoke(command, args) {
  const invoke = resolveDesktopInvoke();
  if (typeof invoke !== "function") {
    return Promise.reject(new Error("desktop bridge unavailable"));
  }
  return invoke(command, args);
}

function hasDesktopBridge() {
  return typeof resolveDesktopInvoke() === "function";
}

let desktopApiBaseUrlPromise = null;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function currentApiBaseUrl() {
  if (!hasDesktopBridge()) {
    return hasHttpOrigin(location) ? `${location.protocol}//${location.host}` : "";
  }
  if (!desktopApiBaseUrlPromise) {
    desktopApiBaseUrlPromise = resolveApiBaseUrl({
      locationLike: location,
      hasDesktopBridge: hasDesktopBridge(),
      invoke: tauriInvoke,
      sleep,
    }).catch((error) => {
      desktopApiBaseUrlPromise = null;
      throw error;
    });
  }
  return desktopApiBaseUrlPromise;
}

async function apiFetch(path, opts) {
  return fetch(resolveApiUrl(path, await currentApiBaseUrl()), opts);
}

function previewTypeForPath(path) {
  const ext = String(path || "")
    .split(".")
    .pop()
    ?.toLowerCase();
  if (["csv", "tsv"].includes(ext)) return "csv";
  if (["png", "jpg", "jpeg", "gif", "svg", "webp"].includes(ext)) return "image";
  return "text";
}

const api = {
  experimentTrees: () => fetchJSON("GET", "/api/experiment-trees"),
  experimentTree: (id) => fetchJSON("GET", `/api/experiment-trees/${id}`),
  treeRuntimeState: (id) =>
    fetchJSON("GET", `/api/experiment-trees/${id}/runtime-state`),
  createTreeBranch: (treeId, payload) =>
    fetchJSON("POST", `/api/experiment-trees/${treeId}/branches`, payload),
  deleteTreeBranch: (treeId, branchId) =>
    fetchJSON("DELETE", `/api/experiment-trees/${treeId}/branches/${branchId}`),
  addTreeCell: (treeId, branchId, cell, afterCellId) =>
    fetchJSON(
      "POST",
      `/api/experiment-trees/${treeId}/branches/${branchId}/cells`,
      {
        cell,
        after_cell_id: afterCellId || null,
      },
    ),
  updateTreeCellCode: (treeId, branchId, cellId, source) =>
    fetchJSON(
      "POST",
      `/api/experiment-trees/${treeId}/branches/${branchId}/cells/${cellId}/code`,
      { source },
    ),
  moveTreeCell: (treeId, branchId, cellId, direction) =>
    fetchJSON(
      "POST",
      `/api/experiment-trees/${treeId}/branches/${branchId}/cells/${cellId}/move`,
      { direction },
    ),
  deleteTreeCell: (treeId, branchId, cellId) =>
    fetchJSON(
      "DELETE",
      `/api/experiment-trees/${treeId}/branches/${branchId}/cells/${cellId}`,
    ),
  executeTreeCell: (treeId, branchId, cellId) =>
    fetchJSON(
      "POST",
      `/api/experiment-trees/${treeId}/branches/${branchId}/cells/${cellId}/execute`,
    ),
  treeCellLogs: (treeId, branchId, cellId) =>
    fetchJSON(
      "GET",
      `/api/experiment-trees/${treeId}/branches/${branchId}/cells/${cellId}/logs`,
    ),
  executeTreeBranch: (treeId, branchId) =>
    fetchJSON(
      "POST",
      `/api/experiment-trees/${treeId}/branches/${branchId}/execute`,
    ),
  executeAllTreeBranches: (treeId) =>
    fetchJSON("POST", `/api/experiment-trees/${treeId}/execute-all-branches`),
  status: (id) => fetchJSON("GET", `/api/executions/${id}`),
  cancelExecution: (id) => fetchJSON("POST", `/api/executions/${id}/cancel`),
  createTree: (name, projectId) =>
    fetchJSON("POST", "/api/experiment-trees", { name, project_id: projectId }),
  deleteTree: (treeId) =>
    fetchJSON("DELETE", `/api/experiment-trees/${treeId}`),
  renameTree: (treeId, name) =>
    fetchJSON("POST", `/api/experiment-trees/${treeId}/rename`, { name }),
  listFiles: (path, projectId) =>
    fetchJSON("GET", `/api/files?${fileQuery(path, projectId)}`),
  defaultProjectsDir: () => fetchJSON("GET", "/api/system/default-projects-dir"),
  pickDirectory: (initialDir) =>
    fetchJSON("POST", "/api/system/pick-directory", {
      initial_dir: initialDir || null,
    }),
  readFile: async (path, projectId) => {
    const r = await apiFetch(`/api/files/read?${fileQuery(path, projectId)}`);
    if (!r.ok) throw new Error(`${r.status}`);
    return r.text();
  },
  projects: () => fetchJSON("GET", "/api/projects"),
  project: (id) => fetchJSON("GET", `/api/projects/${id}`),
  createProject: (p) => fetchJSON("POST", "/api/projects", p),
  experiments: (pid) => fetchJSON("GET", `/api/projects/${pid}/experiments`),
  writeFile: (path, content, projectId) =>
    fetchJSON("POST", "/api/files/write", {
      path,
      content,
      project_id: projectId || null,
    }),
};

// ── Store ─────────────────────────────────────────────────────
function createStore(init) {
  let s = init;
  const ls = new Set();
  return {
    get: () => s,
    set: (fn) => {
      s = typeof fn === "function" ? fn(s) : fn;
      ls.forEach((l) => l());
    },
    sub: (fn) => {
      ls.add(fn);
      return () => ls.delete(fn);
    },
  };
}

const store = createStore({
  view: "dashboard",
  currentProject: null,
  activeExperiment: null,
  experimentTrees: [],
  activeTreeId: null,
  activeBranchId: null,
  projects: [],
  experiments: [],
  cellStatuses: {},
  cellLogs: {},
  cellDisplays: {},
  activePollIds: {},
  executionTargets: {},
  executionStatuses: {},
  activeCellExecutions: {},
  treeRuntimeStates: {},
  sidebarTab: "experiments",
  sidebarCollapsed: loadSidebarCollapsed(),
  fileTree: {},
  filePreview: null,
  wsConnected: false,
  terminalEvents: [],
  toast: null,
  compareSelection: [],
});

function useStore(sel) {
  const [, fu] = useState(0);
  const ref = useRef(sel(store.get()));
  useEffect(
    () =>
      store.sub(() => {
        const n = sel(store.get());
        if (n !== ref.current) {
          ref.current = n;
          fu((c) => c + 1);
        }
      }),
    [],
  );
  return sel(store.get());
}

function defaultBranchIdForTree(tree) {
  return pickActiveBranchId(tree, null);
}

function setActiveExperimentState(exp) {
  store.set((s) => {
    const matchingTree =
      (s.experimentTrees || []).find((tree) => tree.id === exp?.id) || null;
    return {
      ...s,
      activeExperiment: exp,
      activeTreeId: exp?.id || null,
      activeBranchId: exp ? defaultBranchIdForTree(matchingTree) : null,
    };
  });
}

function termLog(msg, level = "info") {
  const event = normalizeTerminalEvent(msg, level);
  store.set((s) => ({
    ...s,
    terminalEvents: appendTerminalEvent(s.terminalEvents, event),
  }));
  return event;
}
function showToast(msg) {
  store.set((s) => ({ ...s, toast: { msg, ts: Date.now() } }));
  setTimeout(() => store.set((s) => ({ ...s, toast: null })), 2500);
}

function nodeStatusToCellStatus(status) {
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

function isTerminalExecutionStatus(status) {
  const lifecycle = String(status?.status || "").trim().toLowerCase();
  if (["completed", "failed", "cancelled", "timed_out", "rejected"].includes(lifecycle)) {
    return true;
  }
  return Boolean(status?.finished_at);
}

function applyExecutionStatusSnapshot(execId, status, fallbackTarget = null) {
  if (!execId || !status) return;
  const previousStatus = store.get().executionStatuses?.[execId] || null;
  const currentTarget = store.get().executionTargets?.[execId] || null;
  const executionTarget = fallbackTarget || currentTarget;
  const treeId = status.tree_id || executionTarget?.treeId || null;
  const branchId = status.branch_id || executionTarget?.branchId || null;
  const targetKind = status.target_kind || executionTarget?.targetKind || null;
  const normalizedStatus = {
    ...status,
    execution_id: status.execution_id || execId,
    tree_id: treeId,
    branch_id: branchId,
    target_kind: targetKind,
  };
  const nodeIds = Object.keys(status.node_statuses || {});
  store.set((s) => {
    const cellStatuses = { ...s.cellStatuses };
    const executionTargets = { ...s.executionTargets };
    const executionStatuses = { ...s.executionStatuses };
    executionTargets[execId] = {
      treeId,
      branchId,
      targetKind,
    };
    executionStatuses[execId] = {
      ...normalizedStatus,
    };
    for (const nodeId of nodeIds) {
      const cellKey = runtimeCellKey({
        treeId,
        branchId,
        nodeId,
      });
      if (!cellKey) continue;
      const nextStatus = nodeStatusToCellStatus(status.node_statuses?.[nodeId]);
      if (nextStatus) cellStatuses[cellKey] = nextStatus;
    }
    return {
      ...s,
      cellStatuses,
      executionTargets,
      executionStatuses,
    };
  });
  const statusEvent = buildExecutionStatusEvent(previousStatus, normalizedStatus, {
    treeId,
    branchId,
    targetKind,
  });
  if (statusEvent) {
    termLog(
      statusEvent,
      ["failed", "timed_out", "rejected"].includes(statusEvent.status)
        ? "error"
        : "info",
    );
  }
}

function stripAnsi(text) {
  return String(text || "").replace(/\u001b\[[0-9;]*m/g, "");
}

function createNewExperimentName() {
  return `experiment_${Date.now()}`;
}

function defaultCellName(index) {
  return `Cell ${Math.max(1, Number(index) || 1)}`;
}

function createTreeBranchPayload(
  treeId,
  parentBranchId,
  branchPointCellId,
  name,
) {
  return {
    parent_branch_id: parentBranchId,
    name,
    branch_point_cell_id: branchPointCellId,
    first_cell: {
      id: `cell_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      tree_id: treeId,
      branch_id: parentBranchId,
      name: defaultCellName(1),
      code: { source: "", language: "python" },
      upstream_cell_ids: [branchPointCellId],
      declared_outputs: [],
      cache: true,
      map_over: null,
      map_concurrency: null,
      tags: {},
      revision_id: null,
      state: "clean",
    },
  };
}

async function createTreeBranchAndSelect(
  tree,
  parentBranchId,
  branchPointCellId,
  name,
) {
  const result = await api.createTreeBranch(
    tree.id,
    createTreeBranchPayload(tree.id, parentBranchId, branchPointCellId, name),
  );
  const branchId = typeof result === "string" ? result : result?.id || result;
  if (!branchId)
    throw new Error("Branch created but no branch id was returned");
  await loadExperiments();
  store.set((s) => ({
    ...s,
    activeTreeId: tree.id,
    activeBranchId: branchId,
    view: "notebook",
  }));
  showToast("Branch created");
  return branchId;
}

function emptyLogs() {
  return {
    stdout: "",
    stderr: "",
    outputs: [],
    error: null,
    metrics: {},
    duration_ms: null,
  };
}

function mergeLogs(prev, next) {
  const base = { ...emptyLogs(), ...(prev || {}) };
  const incoming = next || {};
  return {
    ...base,
    ...incoming,
    stdout:
      incoming.stdout !== undefined && incoming.stdout !== ""
        ? incoming.stdout
        : base.stdout || "",
    stderr:
      incoming.stderr !== undefined && incoming.stderr !== ""
        ? incoming.stderr
        : base.stderr || "",
    outputs: incoming.outputs?.length
      ? [...incoming.outputs]
      : [...(base.outputs || [])],
    error: incoming.error ?? base.error ?? null,
    duration_ms: incoming.duration_ms ?? base.duration_ms ?? null,
    metrics: { ...(base.metrics || {}), ...(incoming.metrics || {}) },
  };
}

function hasLogContent(logs) {
  return !!(
    logs &&
    (logs.stdout ||
      logs.stderr ||
      logs.error ||
      logs.duration_ms != null ||
      (logs.outputs && logs.outputs.length) ||
      (logs.metrics && Object.keys(logs.metrics).length))
  );
}

function deriveStatusFromLogs(logs) {
  if (!hasLogContent(logs)) return "idle";
  if (logs.error) return "failed";
  return "done";
}

function eventTypeName(type) {
  return String(type || "")
    .split("_")
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("");
}

function parseExecutionEvent(raw) {
  if (!raw || typeof raw !== "object") return { type: null, data: null };
  if (typeof raw.type === "string")
    return { type: eventTypeName(raw.type), data: raw };
  const [type] = Object.keys(raw);
  return type ? { type, data: raw[type] } : { type: null, data: null };
}

function runtimeCellKey({
  runtimeId = null,
  treeId = null,
  branchId = null,
  nodeId,
}) {
  if (!nodeId) return null;
  if (treeId && branchId) return `${treeId}_${branchId}_${nodeId}`;
  const fallbackRuntimeId = treeId || runtimeId;
  return fallbackRuntimeId ? `${fallbackRuntimeId}_${nodeId}` : null;
}

function resolveCellKey(data, state) {
  if (!data?.node_id) return null;
  const executionTarget = state.executionTargets?.[data.execution_id] || null;
  return runtimeCellKey({
    treeId: data.tree_id || executionTarget?.treeId || null,
    branchId: data.branch_id || executionTarget?.branchId || null,
    nodeId: data.node_id,
  });
}

function cancelTrackedExecution(execId) {
  if (!execId) return;
  store.set((s) => {
    if (!(execId in s.activePollIds) && !(execId in s.executionTargets))
      return s;
    const activePollIds = { ...s.activePollIds, [execId]: false };
    const executionTargets = { ...s.executionTargets };
    const executionStatuses = { ...s.executionStatuses };
    delete executionTargets[execId];
    delete executionStatuses[execId];
    const activeCellExecutions = { ...s.activeCellExecutions };
    for (const [cellKey, currentExecId] of Object.entries(
      activeCellExecutions,
    )) {
      if (currentExecId === execId) delete activeCellExecutions[cellKey];
    }
    return {
      ...s,
      activePollIds,
      executionTargets,
      executionStatuses,
      activeCellExecutions,
    };
  });
}

function finishTrackedExecution(execId) {
  if (!execId) return;
  store.set((s) => {
    const activePollIds = { ...s.activePollIds };
    delete activePollIds[execId];
    const executionTargets = { ...s.executionTargets };
    const executionStatuses = { ...s.executionStatuses };
    delete executionTargets[execId];
    delete executionStatuses[execId];
    const activeCellExecutions = { ...s.activeCellExecutions };
    for (const [cellKey, currentExecId] of Object.entries(
      activeCellExecutions,
    )) {
      if (currentExecId === execId) delete activeCellExecutions[cellKey];
    }
    return {
      ...s,
      activePollIds,
      executionTargets,
      executionStatuses,
      activeCellExecutions,
    };
  });
}

function registerExecution(execId, treeId, nodeIds, target = null, status = null) {
  if (!execId || !treeId) return;
  const targetNodeIds = nodeIds || [];
  store.set((s) => {
    const activePollIds = { ...s.activePollIds, [execId]: true };
    const executionTargets = {
      ...s.executionTargets,
      [execId]: target || {
        treeId,
        branchId: null,
        targetKind: "experiment_tree_branch",
      },
    };
    const executionStatuses = { ...s.executionStatuses };
    if (status) executionStatuses[execId] = { ...status };
    const activeCellExecutions = { ...s.activeCellExecutions };
    const cellLogs = { ...s.cellLogs };
    const executionTarget = executionTargets[execId] || null;
    for (const nodeId of targetNodeIds) {
      const cellKey = runtimeCellKey({
        treeId: executionTarget?.treeId || treeId,
        branchId: executionTarget?.branchId || null,
        nodeId,
      });
      const prevExecId = activeCellExecutions[cellKey];
      if (prevExecId && prevExecId !== execId)
        activePollIds[prevExecId] = false;
      activeCellExecutions[cellKey] = execId;
      cellLogs[cellKey] = emptyLogs();
    }
    return {
      ...s,
      activePollIds,
      executionTargets,
      executionStatuses,
      activeCellExecutions,
      cellLogs,
    };
  });
  pollExecution(execId, treeId, targetNodeIds);
}

async function runSelectedExecution(ae, activeTree, selectedBranch) {
  if (!ae || !activeTree) return;
  const targetBranchId = selectedBranch?.id || activeTree.root_branch_id;
  const branchNodeIds = activeBranchPathCellIds(activeTree, targetBranchId);
  store.set((s) => ({
    ...s,
    cellStatuses: {
      ...s.cellStatuses,
      ...Object.fromEntries(
        branchNodeIds.map((nodeId) => [
          runtimeCellKey({
            treeId: activeTree.id,
            branchId: targetBranchId,
            nodeId,
          }),
          "queued",
        ]),
      ),
    },
  }));
  let r;
  try {
    r = await api.executeTreeBranch(activeTree.id, targetBranchId);
  } catch (e) {
    throw e;
  }
  if (r?.execution_id) {
    registerExecution(r.execution_id, activeTree.id, branchNodeIds, {
      treeId: activeTree.id,
      branchId: targetBranchId,
      targetKind: "experiment_tree_branch",
    }, r);
  }
}

async function runAllBranchesExecution(ae, activeTree) {
  if (!ae || !activeTree) return;
  const branchIds = (activeTree.branches || []).map(branch => branch.id);
  const queuedEntries = branchIds.flatMap(branchId =>
    activeBranchPathCellIds(activeTree, branchId).map(nodeId => [
      runtimeCellKey({ treeId: activeTree.id, branchId, nodeId }),
      "queued",
    ])
  );
  store.set(s => ({
    ...s,
    cellStatuses: {
      ...s.cellStatuses,
      ...Object.fromEntries(queuedEntries),
    },
  }));
  let r;
  try {
    r = await api.executeAllTreeBranches(activeTree.id);
  } catch (e) {
    throw e;
  }
  for (const item of r?.executions || []) {
    const branchId = item.target?.branch_id;
    const branchNodeIds = activeBranchPathCellIds(activeTree, branchId);
    registerExecution(
      item.execution_id,
      activeTree.id,
      branchNodeIds,
      {
        treeId: activeTree.id,
        branchId,
        targetKind: "experiment_tree_branch",
      },
      item,
    );
  }
  showToast("Running all branches");
}

function cellDefToNode(cell) {
  return {
    id: cell.id,
    name: cell.name,
    code: cell.code,
    cache: !!cell.cache,
    inputs: {},
    outputs: cell.declared_outputs || [],
  };
}

function buildTreeBranchColumns(tree, activePipeline, activeBranchId) {
  if (!tree?.branches?.length) return [];

  const branchById = new Map(
    (tree.branches || []).map((branch) => [branch.id, branch]),
  );
  const cellById = new Map((tree.cells || []).map((cell) => [cell.id, cell]));
  const childrenByParent = new Map();
  for (const branch of tree.branches || []) {
    const parentId = branch.parent_branch_id || null;
    if (!childrenByParent.has(parentId)) childrenByParent.set(parentId, []);
    childrenByParent.get(parentId).push(branch);
  }

  const orderedBranches = [];
  const visit = (branchId) => {
    const branch = branchById.get(branchId);
    if (!branch) return;
    orderedBranches.push(branch);
    const children = childrenByParent.get(branch.id) || [];
    children.forEach((child) => visit(child.id));
  };
  visit(tree.root_branch_id);

  const runtimeId = activePipeline?.id || tree.id;
  return orderedBranches.map((branch) => {
    const parentBranch = branch.parent_branch_id
      ? branchById.get(branch.parent_branch_id)
      : null;
    const nodes = (branch.cell_order || [])
      .map((cellId) => cellById.get(cellId))
      .filter(Boolean)
      .map(cellDefToNode);
    const selectedBranchId = activeBranchId || tree.root_branch_id;
    const isSelected = branch.id === selectedBranchId;
    const runtimeBacked = isSelected && !!activePipeline;
    return {
      key: `tree-branch-${branch.id}`,
      mode: "tree",
      branch,
      branchId: branch.id,
      parentBranchId: branch.parent_branch_id || null,
      parentCellId: branch.branch_point_cell_id || null,
      pipeline: runtimeBacked
        ? {
            ...activePipeline,
            name: branch.name || activePipeline.name,
            nodes,
          }
        : {
            id: `${runtimeId}::${branch.id}`,
            name: branch.name || branch.id,
            nodes,
          },
      nodes,
      subtitle: parentBranch
        ? `Branch from ${branch.branch_point_cell_id || "selected cell"}`
        : "Main experiment",
      // The selected branch is always editable — you need to type code before
      // a kernel is running.  Non-selected branches stay read-only.
      readOnly: !isSelected,
      deletable: branch.id !== tree.root_branch_id,
      active: isSelected,
    };
  });
}

function updateNodeSourceInStore(treeId, nodeId, source) {
  store.set((s) => {
    const experimentTrees = (s.experimentTrees || []).map((tree) => {
      if (!tree) return tree;
      if (treeId && tree.id !== treeId) return tree;
      if (!(tree.cells || []).some((cell) => cell.id === nodeId)) return tree;
      return {
        ...tree,
        cells: (tree.cells || []).map((cell) =>
          cell.id === nodeId
            ? {
                ...cell,
                code: {
                  ...(cell.code || {}),
                  source,
                  language: cell.code?.language || "python",
                },
              }
            : cell,
        ),
      };
    });
    const activeExperiment = s.activeExperiment
      ? experimentTrees.find(t => t.id === s.activeExperiment.id) || s.activeExperiment
      : null;
    return {
      ...s,
      activeExperiment,
      experiments: experimentTrees,
      experimentTrees,
    };
  });
}

async function hydrateTreeBranchLogs(treeId, branchId, nodes) {
  if (!treeId || !branchId || !(nodes || []).length) return;
  const nodeIds = (nodes || []).map((node) => node.id);
  const results = await Promise.all(
    nodeIds.map((nodeId) =>
      api
        .treeCellLogs(treeId, branchId, nodeId)
        .then((logs) => ({ nodeId, logs }))
        .catch(() => ({ nodeId, logs: null })),
    ),
  );
  store.set((s) => {
    const cellLogs = { ...s.cellLogs };
    const cellStatuses = { ...s.cellStatuses };
    for (const { nodeId, logs } of results) {
      if (!logs || !hasLogContent(logs)) continue;
      const cellKey = runtimeCellKey({ treeId, branchId, nodeId });
      cellLogs[cellKey] = mergeLogs(cellLogs[cellKey], logs);
      if (!["queued", "saving", "running"].includes(cellStatuses[cellKey])) {
        cellStatuses[cellKey] = deriveStatusFromLogs(logs);
      }
    }
    return { ...s, cellLogs, cellStatuses };
  });
}

// ── WebSocket (exponential backoff) ──────────────────────────
let wsRetryDelay = 2000;
const WS_MAX_DELAY = 30000;
let wsInstance = null;

function connectWS() {
  currentApiBaseUrl()
    .then((baseUrl) => {
      const ws = new WebSocket(resolveWebSocketUrl(location, baseUrl));
      wsInstance = ws;
      ws.onopen = () => {
        wsRetryDelay = 2000;
        store.set((s) => ({ ...s, wsConnected: true }));
        termLog({
          kind: "system",
          status: "connected",
          message: "WebSocket connected",
        });
      };
      ws.onclose = () => {
        store.set((s) => ({ ...s, wsConnected: false }));
        termLog({
          kind: "system",
          status: "disconnected",
          message: `WebSocket disconnected, retrying in ${wsRetryDelay / 1000}s`,
        });
        setTimeout(connectWS, wsRetryDelay);
        wsRetryDelay = Math.min(wsRetryDelay * 2, WS_MAX_DELAY);
      };
      ws.onerror = () => {};
      ws.onmessage = (e) => {
        try {
          handleEvent(JSON.parse(e.data));
        } catch (err) {
          termLog(
            {
              kind: "system",
              status: "parse_error",
              message: `WebSocket parse error: ${err}`,
              error: {
                ename: "WebSocketParseError",
                evalue: String(err),
                traceback: [],
              },
            },
            "error",
          );
        }
      };
    })
    .catch(() => {
      store.set((s) => ({ ...s, wsConnected: false }));
      setTimeout(connectWS, wsRetryDelay);
      wsRetryDelay = Math.min(wsRetryDelay * 2, WS_MAX_DELAY);
    });
}

function handleEvent(evt) {
  const { type, data: d } = parseExecutionEvent(evt);
  if (!type || !d) return;
  store.set(s => {
    const ns = {
      ...s,
      cellStatuses: { ...s.cellStatuses },
      cellLogs: { ...s.cellLogs },
      cellDisplays: { ...s.cellDisplays },
      activePollIds: { ...s.activePollIds },
      executionTargets: { ...s.executionTargets },
      executionStatuses: { ...s.executionStatuses },
      activeCellExecutions: { ...s.activeCellExecutions },
      treeRuntimeStates: { ...s.treeRuntimeStates },
    };
    if (d.execution_id) {
      ns.executionTargets[d.execution_id] = {
        treeId: d.tree_id || null,
        branchId: d.branch_id || null,
        targetKind: d.target_kind || null,
      };
    }
    const k = resolveCellKey(d, ns);
    if (
      k &&
      d.execution_id &&
      ns.activeCellExecutions[k] &&
      ns.activeCellExecutions[k] !== d.execution_id
    ) {
      return s;
    }
    switch (type) {
      case "NodeStarted":
        if (k) {
          ns.cellStatuses[k] = "running";
          ns.activeCellExecutions[k] = d.execution_id;
          ns.cellLogs[k] = emptyLogs();
        }
        termLog({
          kind: "node",
          status: "started",
          scope: executionEventScope(d),
        });
        break;
      case "NodeStream":
        if (k) {
          const prev = mergeLogs(ns.cellLogs[k]);
          if (d.stream === "stdout")
            ns.cellLogs[k] = { ...prev, stdout: prev.stdout + (d.text || "") };
          else
            ns.cellLogs[k] = { ...prev, stderr: prev.stderr + (d.text || "") };
        }
        break;
      case "NodeDisplayData":
        if (k) {
          const prev = mergeLogs(ns.cellLogs[k]);
          const out = d.output || { data: {}, metadata: {} };
          ns.cellLogs[k] = { ...prev, outputs: [...prev.outputs, out] };
          if (d.display_id) {
            ns.cellDisplays[`${k}_${d.display_id}`] = prev.outputs.length;
          }
        }
        break;
      case "NodeDisplayUpdate":
        if (k && d.display_id) {
          const prev = mergeLogs(ns.cellLogs[k]);
          if (prev) {
            const idx = ns.cellDisplays[`${k}_${d.display_id}`];
            if (idx !== undefined && idx < prev.outputs.length) {
              const outs = [...(prev.outputs || [])];
              outs[idx] = d.output || { data: {}, metadata: {} };
              ns.cellLogs[k] = { ...prev, outputs: outs };
            } else {
              ns.cellLogs[k] = {
                ...prev,
                outputs: [
                  ...(prev.outputs || []),
                  d.output || { data: {}, metadata: {} },
                ],
              };
            }
          }
        }
        break;
      case "NodeCompleted":
        if (k) {
          ns.cellStatuses[k] = "done";
          if (ns.activeCellExecutions[k] === d.execution_id) {
            delete ns.activeCellExecutions[k];
          }
          ns.cellLogs[k] = {
            ...mergeLogs(ns.cellLogs[k]),
            duration_ms: d.duration_ms ?? ns.cellLogs[k]?.duration_ms ?? null,
          };
        }
        termLog({
          kind: "node",
          status: "done",
          scope: executionEventScope(d),
          duration_ms: d.duration_ms ?? null,
        });
        break;
      case "NodeCacheHit":
        if (k) ns.cellStatuses[k] = "cached";
        termLog({
          kind: "node",
          status: "cached",
          scope: executionEventScope(d),
        });
        break;
      case "NodeFailed":
        if (k) {
          ns.cellStatuses[k] = "failed";
          if (ns.activeCellExecutions[k] === d.execution_id) {
            delete ns.activeCellExecutions[k];
          }
          const prev = mergeLogs(ns.cellLogs[k]);
          ns.cellLogs[k] = {
            ...prev,
            error: d.error || {
              ename: "Error",
              evalue: "Unknown",
              traceback: [],
            },
          };
        }
        termLog(
          {
            kind: "node",
            status: "failed",
            scope: executionEventScope(d),
            error: d.error,
          },
          "error",
        );
        break;
      case "FileChanged":
        if (d.path) {
          const dir = watchedDirForPath(d.path);
          const projectId = store.get().currentProject?.id || null;
          api
            .listFiles(dir, projectId)
            .then((entries) => {
              store.set((s2) => ({
                ...s2,
                fileTree: {
                  ...s2.fileTree,
                  [dir]: { entries, expanded: true },
                },
              }));
            })
            .catch(() => {});
        }
        break;
      case "ExecutionStarted":
        termLog({
          kind: "execution",
          status: "started",
          scope: executionEventScope(d),
        });
        break;
      case "ExecutionCompleted":
        delete ns.activePollIds[d.execution_id];
        delete ns.executionTargets[d.execution_id];
        delete ns.executionStatuses[d.execution_id];
        for (const [cellKey, currentExecId] of Object.entries(
          ns.activeCellExecutions,
        )) {
          if (currentExecId === d.execution_id)
            delete ns.activeCellExecutions[cellKey];
        }
        termLog({
          kind: "execution",
          status: "done",
          scope: executionEventScope(d),
          duration_ms: d.duration_ms ?? null,
        });
        break;
      case "ExecutionFailed":
        delete ns.activePollIds[d.execution_id];
        delete ns.executionTargets[d.execution_id];
        delete ns.executionStatuses[d.execution_id];
        for (const [cellKey, currentExecId] of Object.entries(
          ns.activeCellExecutions,
        )) {
          if (currentExecId === d.execution_id)
            delete ns.activeCellExecutions[cellKey];
        }
        termLog(
          {
            kind: "execution",
            status: "failed",
            scope: executionEventScope(d),
          },
          "error",
        );
        break;
      case "IsolationAttempted":
        termLog({
          kind: "runtime",
          status: "isolation_attempted",
          scope: executionEventScope(d),
        });
        break;
      case "IsolationSucceeded":
        termLog({
          kind: "runtime",
          status: "isolation_succeeded",
          scope: executionEventScope(d),
        });
        break;
      case "ContaminationDetected":
        termLog(
          {
            kind: "runtime",
            status: "contamination_detected",
            scope: executionEventScope(d),
            error: { evalue: (d.signals || []).join(", ") || "contamination" },
          },
          "error",
        );
        break;
      case "FallbackRestartTriggered":
        termLog({
          kind: "runtime",
          status: "fallback_restart",
          scope: executionEventScope(d),
          error: d.reason ? { evalue: d.reason } : undefined,
        });
        break;
      case "TreeRuntimeStateChanged":
        termLog({
          kind: "runtime_state",
          status: d.kernel_state,
          scope: normalizeTerminalScope({
            treeId: d.tree_id,
            branchId: d.branch_id,
            nodeId: d.last_prepared_cell_id || null,
          }),
          message:
            d.kernel_state === "switching"
              ? `Preparing branch ${d.branch_id}`
              : d.kernel_state === "ready"
                ? `Runtime ready for branch ${d.branch_id}`
                : d.kernel_state === "needs_replay"
                  ? `Runtime for branch ${d.branch_id} needs replay`
                  : d.kernel_state === "kernel_lost"
                    ? `Kernel lost for branch ${d.branch_id}`
                    : null,
        });
        break;
    }
    const runtimeEventTypes = new Set([
      "IsolationAttempted",
      "IsolationSucceeded",
      "ContaminationDetected",
      "FallbackRestartTriggered",
      "TreeRuntimeStateChanged",
    ]);
    if (d.tree_id && runtimeEventTypes.has(type)) {
      api
        .treeRuntimeState(d.tree_id)
        .then((runtimeState) => {
          store.set((s2) => ({
            ...s2,
            treeRuntimeStates: {
              ...s2.treeRuntimeStates,
              [d.tree_id]: runtimeState,
            },
          }));
        })
        .catch(() => {});
    }
    return ns;
  });
}

// ── Data loaders ──────────────────────────────────────────────
async function loadProjects() {
  try {
    const list = await api.projects();
    store.set((s) => ({ ...s, projects: list }));
  } catch (e) {
    termLog(`Load projects: ${e}`, "error");
  }
}

async function loadExperiments() {
  const project = store.get().currentProject;
  if (!project) return;
  try {
    const rawTrees = await api.experimentTrees().catch(() => []);
    const trees = (rawTrees || []).filter(
      (tree) => !project?.id || tree.project_id === project.id,
    );
    const runtimeStates = Object.fromEntries(
      await Promise.all(
        trees.map(async (tree) => [
          tree.id,
          await api.treeRuntimeState(tree.id).catch(() => null),
        ]),
      ),
    );
    store.set((s) => {
      const activeId = s.activeExperiment?.id;
      const activeExperiment = activeId
        ? trees.find((t) => t.id === activeId) || s.activeExperiment
        : s.activeExperiment;
      const activeTree =
        trees.find((tree) => tree.id === activeExperiment?.id) || null;
      const activeBranchId = activeExperiment
        ? pickActiveBranchId(activeTree, s.activeBranchId)
        : null;
      return {
        ...s,
        experiments: trees,
        experimentTrees: trees,
        activeExperiment,
        activeTreeId: activeExperiment?.id || null,
        activeBranchId,
        treeRuntimeStates: runtimeStates,
      };
    });
    return trees;
  } catch (e) {
    termLog(`Load experiments: ${e}`, "error");
  }
}

async function pollExecution(execId, runtimeId, nodeIds) {
  const targetNodes = nodeIds || [];
  for (let i = 0; i < 120; i++) {
    if (
      !(execId in store.get().activePollIds) ||
      store.get().activePollIds[execId] === false
    )
      return;
    await new Promise((r) => setTimeout(r, 1000));
    try {
      const st = await api.status(execId);
      const executionTarget = store.get().executionTargets?.[execId] || null;
      applyExecutionStatusSnapshot(execId, st, executionTarget);
      const treeId = st.tree_id || executionTarget?.treeId || null;
      const branchId = st.branch_id || executionTarget?.branchId || null;
      const wsConnected = store.get().wsConnected;
      const liveNodes = targetNodes.length
        ? targetNodes
        : Object.keys(st.node_statuses || {});
      if (liveNodes.length && !wsConnected) {
        store.set((s) => {
          const cellStatuses = { ...s.cellStatuses };
          for (const nid of liveNodes) {
            const cellKey = runtimeCellKey({
              runtimeId,
              treeId,
              branchId,
              nodeId: nid,
            });
            if (!cellKey) continue;
            const nextStatus = nodeStatusToCellStatus(st.node_statuses?.[nid]);
            if (nextStatus) cellStatuses[cellKey] = nextStatus;
          }
          return { ...s, cellStatuses };
        });
      }
      if (isTerminalExecutionStatus(st)) {
        const nodes = liveNodes;
        for (const nid of nodes) {
          try {
            const l = treeId && branchId
              ? await api.treeCellLogs(treeId, branchId, nid)
              : null;
            if (!l) continue;
            const cellKey = runtimeCellKey({
              runtimeId,
              treeId,
              branchId,
              nodeId: nid,
            });
            store.set((s) => ({
              ...s,
              cellLogs: {
                ...s.cellLogs,
                [cellKey]: mergeLogs(undefined, l),
              },
              cellStatuses: {
                ...s.cellStatuses,
                [cellKey]:
                  !s.wsConnected || ["idle", "queued", "running", "timeout"].includes(s.cellStatuses[cellKey] || "idle")
                    ? (deriveStatusFromLogs(l) === "idle"
                        ? s.cellStatuses[cellKey] || "idle"
                        : deriveStatusFromLogs(l))
                    : s.cellStatuses[cellKey],
              },
            }));
          } catch {}
        }
        finishTrackedExecution(execId);
        return;
      }
    } catch (err) {
      if (String(err).includes("404")) {
        finishTrackedExecution(execId);
        termLog(`Execution ${execId} no longer exists`, "error");
        return;
      }
      termLog(`Execution ${execId} poll error: ${err}`, "error");
    }
  }
  const wsConnected = store.get().wsConnected;
  if (targetNodes.length && !wsConnected) {
    const executionTarget = store.get().executionTargets?.[execId] || null;
    for (const nid of targetNodes) {
      const k = runtimeCellKey({
        runtimeId,
        treeId: executionTarget?.treeId || null,
        branchId: executionTarget?.branchId || null,
        nodeId: nid,
      });
      store.set((s) => ({
        ...s,
        cellStatuses: {
          ...s.cellStatuses,
          [k]: s.cellStatuses[k] === "running" ? "timeout" : s.cellStatuses[k],
        },
        cellLogs: {
          ...s.cellLogs,
          [k]: {
            ...(s.cellLogs[k] || {}),
            error: {
              ename: "Timeout",
              evalue: "Execution polling timed out after 120s",
              traceback: [],
            },
          },
        },
      }));
    }
  } else if (targetNodes.length) {
    termLog(
      `Execution ${execId} polling window ended; waiting for WebSocket updates`,
      "info",
    );
  }
  finishTrackedExecution(execId);
  termLog(
    wsConnected
      ? `Execution ${execId} poll window ended`
      : `Execution ${execId} polling timed out`,
    wsConnected ? "info" : "error",
  );
}

async function cancelExecutionById(execId) {
  if (!execId) return;
  const executionTarget = store.get().executionTargets?.[execId] || null;
  try {
    await api.cancelExecution(execId);
    const status = await api.status(execId).catch(() => null);
    if (status) applyExecutionStatusSnapshot(execId, status, executionTarget);
    finishTrackedExecution(execId);
    showToast("Run terminated");
  } catch (e) {
    termLog(`Terminate execution: ${e}`, "error");
  }
}

// ── Cell Component ────────────────────────────────────────────

const PYTHON_EXTENSIONS = [python()];

function HighlightEditor({
  value,
  onChange,
  onKeyDown,
  readOnly = false,
  placeholder = "",
  textareaRef = null,
  language = "python",
}) {
  const viewRef = useRef(null);
  const [editorTheme, setEditorTheme] = useState(() => currentThemeMode());

  const handleCreateEditor = useCallback(
    (view) => {
      viewRef.current = view;
      if (!textareaRef) return;
      const focusHandle = { focus: () => view.focus() };
      if (typeof textareaRef === "function") textareaRef(focusHandle);
      else textareaRef.current = focusHandle;
    },
    [textareaRef],
  );

  useEffect(() => {
    return () => {
      if (textareaRef && typeof textareaRef !== "function") {
        textareaRef.current = null;
      }
      viewRef.current = null;
    };
  }, [textareaRef]);

  useEffect(() => {
    if (typeof document === "undefined") return undefined;
    const root = document.documentElement;
    const syncTheme = () => setEditorTheme(currentThemeMode());
    syncTheme();
    const observer = new MutationObserver(syncTheme);
    observer.observe(root, {
      attributes: true,
      attributeFilter: ["data-theme"],
    });
    return () => observer.disconnect();
  }, []);

  const handleChange = useCallback(
    (next) => {
      onChange?.(next);
    },
    [onChange],
  );

  const handleKeyDown = useCallback(
    (event) => {
      if (!readOnly && event.shiftKey && event.key === "Enter") {
        event.preventDefault();
        onKeyDown?.(event);
      }
    },
    [onKeyDown, readOnly],
  );

  const extensions = language === "python" ? PYTHON_EXTENSIONS : [];

  return html`
    <div
      class="cell-editor-shell ${!(value || "").trim() ? "empty" : ""}"
      data-placeholder=${placeholder}
      onKeyDown=${handleKeyDown}
    >
      <${CodeMirror}
        value=${value || ""}
        height="auto"
        theme=${editorTheme === "dark" ? CM_DARK_THEME : CM_LIGHT_THEME}
        extensions=${extensions}
        readOnly=${readOnly}
        editable=${!readOnly}
        placeholder=${placeholder}
        basicSetup=${{
          lineNumbers: false,
          foldGutter: false,
          highlightActiveLineGutter: false,
          highlightActiveLine: false,
          bracketMatching: true,
          autocompletion: false,
          indentOnInput: true,
        }}
        onCreateEditor=${handleCreateEditor}
        onChange=${(nextValue) => handleChange(nextValue)}
      />
    </div>
  `;
}

function Cell({
  node,
  pipeline,
  index,
  shellRef,
  showBranchHandle = true,
  readOnly = false,
  treeContext = null,
}) {
  const runtimePid = treeContext?.treeId || pipeline.id;
  const pid = pipeline.id,
    nid = node.id;
  const experiments = useStore((s) => s.experiments);
  const experimentTrees = useStore((s) => s.experimentTrees);
  const activeTreeId = useStore((s) => s.activeTreeId);
  const activeBranchId = useStore((s) => s.activeBranchId);
  const project = useStore((s) => s.currentProject);
  const treeExecutionBranchId =
    treeContext?.branchId || treeContext?.rootBranchId || null;
  const isTreeBranchRuntime = !!treeExecutionBranchId;
  const isTreeExecutionRuntime = !!(treeContext && treeExecutionBranchId);
  const key = runtimeCellKey({
    runtimeId: runtimePid,
    treeId: treeContext?.treeId || null,
    branchId: treeExecutionBranchId,
    nodeId: nid,
  });
  const [code, setCode] = useState(node.code?.source || "");
  const [unsaved, setUnsaved] = useState(false);
  const status = useStore((s) => s.cellStatuses[key] || "idle");
  const logs = useStore((s) => s.cellLogs[key]);
  const executionProgress = useStore((s) => {
    const execId = s.activeCellExecutions[key] || null;
    return describeExecutionProgress(
      execId ? s.executionStatuses?.[execId] || null : null,
      s.cellStatuses[key] || "idle",
    );
  });
  const saveT = useRef(null);
  const latestCode = useRef(code);
  const saveInFlight = useRef(null);
  const textareaRef = useRef(null);
  const branchLocked =
    readOnly;
  const activeTree =
    (experimentTrees || []).find(
      (tree) => tree.id === (activeTreeId || runtimePid),
    ) || null;

  // Hard reset when the cell identity changes (e.g. switching to a different
  // notebook cell).
  useEffect(() => {
    const incoming = node.code?.source || "";
    setCode(incoming);
    latestCode.current = incoming;
    setUnsaved(false);
  }, [nid]);

  // Soft sync when only the server-side source changes for the *same* cell.
  // This fires on every experiments-heartbeat refresh, so we must not clobber
  // local edits that are still being typed or are mid-save.
  useEffect(() => {
    const incoming = node.code?.source || "";
    if (unsaved) return;
    if (saveInFlight.current) return;
    if (incoming === latestCode.current) return;
    setCode(incoming);
    latestCode.current = incoming;
  }, [node.code?.source]);

  const commitSave = useCallback(
    async (source) => {
      if (readOnly) return false;
      if (!isTreeBranchRuntime || !treeContext?.treeId) return false;
      store.set((s) => ({
        ...s,
        cellStatuses: { ...s.cellStatuses, [key]: "saving" },
      }));
      const task = api.updateTreeCellCode(
          treeContext.treeId,
          treeExecutionBranchId,
          nid,
          source,
        )
        .then(() => {
          updateNodeSourceInStore(
            treeContext.treeId,
            nid,
            source,
          );
          setUnsaved(false);
          store.set((s) => ({
            ...s,
            cellStatuses: {
              ...s.cellStatuses,
              [key]:
                s.cellStatuses[key] === "saving" ? "idle" : s.cellStatuses[key],
            },
          }));
          return true;
        })
        .catch((e) => {
          termLog(`Save: ${e}`, "error");
          store.set((s) => ({
            ...s,
            cellStatuses: { ...s.cellStatuses, [key]: "save_error" },
            cellLogs: {
              ...s.cellLogs,
              [key]: {
                ...mergeLogs(s.cellLogs[key]),
                error: { ename: "SaveError", evalue: String(e), traceback: [] },
              },
            },
          }));
          return false;
        })
        .finally(() => {
          if (saveInFlight.current === task) saveInFlight.current = null;
        });
      saveInFlight.current = task;
      return task;
    },
    [
      nid,
      key,
      readOnly,
      runtimePid,
      isTreeBranchRuntime,
      treeContext,
      treeExecutionBranchId,
    ],
  );

  const flushSave = useCallback(async () => {
    clearTimeout(saveT.current);
    if (!unsaved && !saveInFlight.current) return true;
    if (saveInFlight.current) await saveInFlight.current;
    if (!unsaved) return true;
    return commitSave(latestCode.current);
  }, [unsaved, commitSave]);

  const recordExecutionResult = useCallback(
    (nodeKey, resultLogs, fallbackStatus = "done", executionId = null) => {
      const nextLogs = mergeLogs(store.get().cellLogs[nodeKey], resultLogs);
      const derived = deriveStatusFromLogs(nextLogs);
      const nextStatus = derived === "idle" ? fallbackStatus : derived;
      store.set((s) => {
        const activeExecId = s.activeCellExecutions[nodeKey] || null;
        if (executionId && activeExecId && activeExecId !== executionId) {
          return s;
        }
        const activeCellExecutions = { ...s.activeCellExecutions };
        if (executionId && activeCellExecutions[nodeKey] === executionId) {
          delete activeCellExecutions[nodeKey];
        }
        return {
          ...s,
          activeCellExecutions,
          cellLogs: { ...s.cellLogs, [nodeKey]: nextLogs },
          cellStatuses: { ...s.cellStatuses, [nodeKey]: nextStatus },
        };
      });
      return { nextLogs, nextStatus };
    },
    [],
  );

  const ensureBranchContext = useCallback(async () => true, []);

  const onChange = useCallback(
    (c) => {
      setCode(c);
      latestCode.current = c;
      setUnsaved(true);
      clearTimeout(saveT.current);
      saveT.current = setTimeout(async () => {
        await commitSave(c);
      }, 800);
    },
    [runtimePid, nid, key, commitSave, treeContext],
  );

  const runCell = useCallback(async () => {
    if (readOnly) return;
    const saved = await flushSave();
    if (!saved) return;
    const contextReady = await ensureBranchContext();
    if (!contextReady) return;
    cancelTrackedExecution(store.get().activeCellExecutions[key]);
    store.set((s) => ({
      ...s,
      cellStatuses: { ...s.cellStatuses, [key]: "queued" },
      cellLogs: { ...s.cellLogs, [key]: emptyLogs() },
    }));
    try {
      const r = await api.executeTreeCell(
        treeContext.treeId,
        treeExecutionBranchId,
        nid,
      );
      if (!r?.execution_id) throw new Error("missing execution id");
      registerExecution(r.execution_id, treeContext.treeId, [nid], {
        treeId: treeContext.treeId,
        branchId: treeExecutionBranchId,
        targetKind: "experiment_tree_branch",
      }, r);
    } catch (e) {
      termLog(`Run: ${e}`, "error");
      store.set((s) => ({
        ...s,
        cellStatuses: { ...s.cellStatuses, [key]: "failed" },
        cellLogs: {
          ...s.cellLogs,
          [key]: {
            ...mergeLogs(s.cellLogs[key]),
            error: { ename: "RunError", evalue: String(e), traceback: [] },
          },
        },
      }));
    }
  }, [
    nid,
    key,
    flushSave,
    recordExecutionResult,
    readOnly,
    runtimePid,
    isTreeExecutionRuntime,
    treeContext,
    treeExecutionBranchId,
  ]);

  const branchHere = useCallback(async () => {
    if (readOnly) return;
    const saved = await flushSave();
    if (!saved) return;
    const parentBranchId =
      activeBranchId || activeTree?.root_branch_id || "main";
    const siblingCount = activeTree
      ? (activeTree.branches || []).filter(
          (branch) =>
            (branch.parent_branch_id || null) === (parentBranchId || null),
        ).length
      : 0;
    const branchIndex = siblingCount + 1;
    const name = `${(pipeline.name || pid).replace(/\s+/g, "_")}_branch_${branchIndex}`;
    try {
      if (activeTree) {
        await createTreeBranchAndSelect(activeTree, parentBranchId, nid, name);
        return;
      }
      throw new Error("Branching requires tree context");
    } catch (e) {
      termLog(`Branch: ${e}`, "error");
    }
  }, [
    activeBranchId,
    activeTree,
    pipeline,
    index,
    project,
    flushSave,
    nid,
    readOnly,
  ]);

  const moveCell = useCallback(
    async (dir) => {
      if (readOnly) return;
      try {
        if (!isTreeBranchRuntime) throw new Error("Move requires tree context");
        await api.moveTreeCell(
          treeContext.treeId,
          treeExecutionBranchId,
          nid,
          dir,
        );
        await loadExperiments();
      } catch (e) {
        termLog(`Move: ${e}`, "error");
      }
    },
    [
      nid,
      project,
      readOnly,
      runtimePid,
      isTreeBranchRuntime,
      treeContext,
      treeExecutionBranchId,
    ],
  );

  const deleteCell = useCallback(async () => {
    if (readOnly) return;
    if (!confirm("Delete this cell?")) return;
    try {
      if (!isTreeBranchRuntime) throw new Error("Delete requires tree context");
      await api.deleteTreeCell(
        treeContext.treeId,
        treeExecutionBranchId,
        nid,
      );
      await loadExperiments();
    } catch (e) {
      termLog(`Delete: ${e}`, "error");
    }
  }, [
    nid,
    project,
    readOnly,
    runtimePid,
    isTreeBranchRuntime,
    treeContext,
    treeExecutionBranchId,
  ]);

  useEffect(() => {
    if (status === "done" || status === "failed" || status === "cached") {
      const loadLogs =
        isTreeExecutionRuntime && treeContext?.treeId && treeExecutionBranchId
          ? api.treeCellLogs(treeContext.treeId, treeExecutionBranchId, nid)
          : Promise.resolve(null);
      loadLogs
        .then((l) => {
          if (!l || !hasLogContent(l)) return;
          store.set((s) => ({
            ...s,
            cellLogs: { ...s.cellLogs, [key]: mergeLogs(s.cellLogs[key], l) },
          }));
        })
        .catch(() => {});
    }
  }, [
    status,
    runtimePid,
    nid,
    key,
    isTreeExecutionRuntime,
    treeContext,
    treeExecutionBranchId,
  ]);

  useEffect(() => () => clearTimeout(saveT.current), []);

  const statusIcon =
    status === "saving"
      ? "…"
      : status === "queued"
        ? "◔"
        : status === "running"
          ? "⟳"
          : status === "done"
            ? "✓"
            : status === "failed" || status === "save_error"
              ? "✗"
              : status === "cached"
                ? "⚡"
                : status === "timeout"
                  ? "⏱"
                  : "○";
  const statusCls =
    status === "running" || status === "queued" || status === "saving"
      ? "running"
      : status === "done" || status === "cached"
        ? "done"
        : status === "failed" || status === "timeout" || status === "save_error"
          ? "failed"
          : "idle";
  const saveStateLabel =
    status === "saving" ? "Saving…" : unsaved ? "Edited" : "";
  const statusLabel =
    status === "saving"
      ? "Saving"
      : status === "done"
        ? "Done"
        : status === "cached"
          ? "Cached"
          : status === "timeout"
            ? "Timed out"
            : status === "save_error"
              ? "Failed"
              : executionProgress.label;
  const statusIndicator =
    status === "running" || status === "queued" || status === "saving"
      ? html`<span
          class="cell-toolbar-status ${statusCls}"
          title=${statusLabel}
          aria-label=${statusLabel}
        >
          <${Loader}
            class="cell-status-icon spinning"
            size=${14}
            strokeWidth=${2}
          />
        </span>`
      : html`<span
          class="cell-toolbar-status ${statusCls}"
          title=${statusLabel}
          aria-label=${statusLabel}
        >
          ${statusIcon}
        </span>`;

  return html`
    <div class="cell-shell" ref=${shellRef}>
      <div class="notebook-cell">
        <div class="cell-header">
          <span class="cell-number">In[${index + 1}]</span>
          <span class="cell-name">${node.name || ""}</span>
          <span class="cell-save-state ${saveStateLabel ? "visible" : ""}"
            >${saveStateLabel}</span
          >
        </div>

        <div
          class="cell-code ${!code.trim() ? "empty" : ""}"
          onClick=${() => {
            if (!readOnly) textareaRef.current?.focus();
          }}
        >
          <${HighlightEditor}
            value=${code}
            readOnly=${readOnly}
            textareaRef=${textareaRef}
            placeholder=${readOnly
              ? "Tree branch preview"
              : "Click here to enter Python code"}
            onChange=${onChange}
            onKeyDown=${(e) => {
              if (!readOnly && e.shiftKey && e.key === "Enter") {
                e.preventDefault();
                runCell();
              }
            }}
          />
        </div>

        <div class="cell-toolbar">
          <button
            class="cell-btn run btn btn-primary btn-icon"
            disabled=${readOnly}
            onClick=${(e) => {
              e.stopPropagation();
              runCell();
            }}
            title=${readOnly
              ? "Execution is not wired for tree preview branches yet"
              : "Run (Shift+Enter)"}
            aria-label="Run cell"
          >
            <${Play} size=${15} strokeWidth=${2} />
          </button>
          ${statusIndicator}
          <span class="cell-spacer" />
          <div class="cell-action-group" role="group" aria-label="Cell actions">
            <button
              class="cell-btn btn btn-ghost btn-icon"
              disabled=${branchLocked}
              onClick=${(e) => {
                e.stopPropagation();
                moveCell("up");
              }}
              title=${branchLocked
                ? "Reordering disabled while branching is active"
                : "Move up"}
              aria-label="Move cell up"
            >
              <${ArrowUp} size=${15} strokeWidth=${2} />
            </button>
            <button
              class="cell-btn btn btn-ghost btn-icon"
              disabled=${branchLocked}
              onClick=${(e) => {
                e.stopPropagation();
                moveCell("down");
              }}
              title=${branchLocked
                ? "Reordering disabled while branching is active"
                : "Move down"}
              aria-label="Move cell down"
            >
              <${ArrowDown} size=${15} strokeWidth=${2} />
            </button>
            <button
              class="cell-btn delete btn btn-ghost btn-icon"
              aria-label="Delete cell"
              disabled=${readOnly}
              onClick=${(e) => {
                e.stopPropagation();
                deleteCell();
              }}
              title=${readOnly
                ? "Deletion is not wired for tree preview branches yet"
                : "Delete cell"}
            >
              <${Trash2} size=${15} strokeWidth=${2} />
            </button>
          </div>
        </div>

        ${renderOutput(logs, index, status, executionProgress)}
      </div>
      ${showBranchHandle && !readOnly
        ? html`<button
            class="branch-plus-btn btn btn-secondary btn-icon"
            onClick=${(e) => {
              e.stopPropagation();
              branchHere();
            }}
            title="Create branch from this cell"
            aria-label="Create branch from this cell"
          >
            <${Plus} size=${16} strokeWidth=${2} />
          </button>`
        : null}
    </div>
  `;
}

function renderOutput(logs, idx, status, executionProgress = null) {
  const safeLogs = logs || emptyLogs();
  const hasRenderableContent = !!(
    safeLogs.stdout ||
    safeLogs.stderr ||
    safeLogs.error ||
    (safeLogs.outputs && safeLogs.outputs.length) ||
    (safeLogs.metrics && Object.keys(safeLogs.metrics).length)
  );
  const showStatusOnly = ["failed", "timeout", "save_error"].includes(status);
  const showPanel = hasRenderableContent || showStatusOnly;
  if (!showPanel) return null;

  let statusMessage = null;
  if (!hasRenderableContent) {
    if (status === "queued" || status === "running") {
      statusMessage = executionProgress?.message || (status === "queued" ? "Queued…" : "Running…");
    }
    else if (status === "timeout") statusMessage = "Execution timed out.";
    else if (status === "save_error")
      statusMessage = "Code could not be saved.";
    else if (status === "failed") statusMessage = "Execution failed.";
  }

  return html`
    <div class="cell-output">
      <div class="cell-output-header">
        <span class="cell-number">Out[${idx + 1}]</span>
      </div>
      <div class="cell-output-content">
        ${statusMessage
          ? html`<div class="output-status">${statusMessage}</div>`
          : null}
        ${safeLogs.metrics &&
        Object.keys(safeLogs.metrics).length > 0 &&
        html`
          <div class="output-metrics">
            ${Object.entries(safeLogs.metrics).map(
              ([k, v]) =>
                html`<span class="metric" key=${k}
                  ><span class="metric-key">${k}</span> ${typeof v === "number"
                    ? v.toFixed(4)
                    : v}</span
                >`,
            )}
          </div>
        `}
        ${safeLogs.outputs?.map((o, i) => renderRich(o, i))}
        ${safeLogs.stdout
          ? html`<pre class="output-text">${stripAnsi(safeLogs.stdout)}</pre>`
          : null}
        ${safeLogs.stderr
          ? html`<pre class="output-text out-stderr">
${stripAnsi(safeLogs.stderr)}</pre
            >`
          : null}
        ${safeLogs.error
          ? html`<div class="output-error">
              <b>${safeLogs.error.ename}:</b> ${stripAnsi(
                safeLogs.error.evalue,
              )}${safeLogs.error.traceback?.length
                ? html`<pre class="out-traceback">
${stripAnsi(safeLogs.error.traceback.join("\n"))}</pre
                  >`
                : null}
            </div>`
          : null}
      </div>
    </div>
  `;
}

function renderRich(out, i) {
  const d = out.data || {};
  if (d["image/png"])
    return html`<div key=${i} class="output-image">
      <img src="data:image/png;base64,${d["image/png"]}" />
    </div>`;
  if (d["image/jpeg"])
    return html`<div key=${i} class="output-image">
      <img src="data:image/jpeg;base64,${d["image/jpeg"]}" />
    </div>`;
  if (d["image/svg+xml"]) {
    const clean = DOMPurify.sanitize(d["image/svg+xml"], {
      USE_PROFILES: { svg: true },
    });
    return html`<div
      key=${i}
      class="output-image"
      dangerouslySetInnerHTML=${{ __html: clean }}
    />`;
  }
  if (d["text/html"]) {
    const clean = DOMPurify.sanitize(d["text/html"], {
      ADD_TAGS: ["style"],
      ADD_ATTR: ["class", "style"],
    });
    return html`<div
      key=${i}
      class="output-html"
      dangerouslySetInnerHTML=${{ __html: clean }}
    />`;
  }
  if (d["text/plain"])
    return html`<pre key=${i} class="output-text">${d["text/plain"]}</pre>`;
  const [mime, value] = Object.entries(d)[0] || [];
  return mime
    ? html`<pre key=${i} class="output-text">
${mime}
${String(value)}</pre
      >`
    : null;
}

// ── Sidebar: Experiments ──────────────────────────────────────
function ExperimentsPanel() {
  const project = useStore((s) => s.currentProject);
  const experiments = useStore((s) => s.experiments);
  const active = useStore((s) => s.activeExperiment);
  const experimentMenuRefs = useRef({});
  const [menuExperimentId, setMenuExperimentId] = useState(null);
  const [renameExperimentTarget, setRenameExperimentTarget] = useState(null);
  const [renameExperimentName, setRenameExperimentName] = useState("");

  useEffect(() => {
    if (project) loadExperiments();
  }, [project]);

  useEffect(() => {
    if (!menuExperimentId) return;
    const onMouseDown = (event) => {
      const activeMenu = experimentMenuRefs.current[menuExperimentId];
      if (!activeMenu?.contains(event.target)) {
        setMenuExperimentId(null);
      }
    };
    window.addEventListener("mousedown", onMouseDown);
    return () => window.removeEventListener("mousedown", onMouseDown);
  }, [menuExperimentId]);

  const closeMenu = () => setMenuExperimentId(null);

  const newExp = async () => {
    if (!project) return;
    try {
      const tree = await api.createTree(
        `experiment_${Date.now()}`,
        project.id,
      );
      await loadExperiments();
      setActiveExperimentState(tree);
      store.set((s) => ({ ...s, view: "notebook" }));
      window.location.hash = `/projects/${project.id}/exp/${tree.id}`;
      showToast("Experiment created");
    } catch (e) {
      termLog(`New experiment: ${e}`, "error");
      showToast("Could not create experiment");
    }
  };

  const select = (exp) => {
    closeMenu();
    setActiveExperimentState(exp);
    store.set((s) => ({ ...s, view: "notebook" }));
    if (project) window.location.hash = `/projects/${project.id}/exp/${exp.id}`;
  };

  const openRenameExperiment = (exp) => {
    closeMenu();
    setRenameExperimentTarget(exp);
    setRenameExperimentName(exp.name || exp.id.slice(0, 8));
  };

  const closeRenameExperiment = () => {
    setRenameExperimentTarget(null);
    setRenameExperimentName("");
  };

  const renameExperiment = async () => {
    const exp = renameExperimentTarget;
    if (!exp) return;
    const currentName = exp.name || exp.id.slice(0, 8);
    const trimmedName = renameExperimentName.trim();
    if (!trimmedName || trimmedName === currentName) return;
    try {
      await api.renameTree(exp.id, trimmedName);
      const trees = await loadExperiments();
      if (active?.id === exp.id) {
        const renamed = (trees || []).find((tree) => tree.id === exp.id) || null;
        if (renamed) setActiveExperimentState(renamed);
      }
      closeRenameExperiment();
      showToast("Experiment renamed");
    } catch (e) {
      termLog(`Rename experiment: ${e}`, "error");
    }
  };

  const deleteExperiment = async (exp) => {
    const label = exp.name || exp.id.slice(0, 8);
    closeMenu();
    if (!confirm(`Delete experiment "${label}"?`)) return;
    try {
      await api.deleteTree(exp.id);
      const trees = (await loadExperiments()) || [];
      if (active?.id === exp.id) {
        const nextExperiment = trees[0] || null;
        setActiveExperimentState(nextExperiment);
        if (project) {
          window.location.hash = nextExperiment
            ? `/projects/${project.id}/exp/${nextExperiment.id}`
            : `/projects/${project.id}`;
        }
      }
      showToast("Experiment deleted");
    } catch (e) {
      termLog(`Delete experiment: ${e}`, "error");
    }
  };

  const roots = experiments;

  return html`
    <div class="sidebar-content">
      <button
        class="new-experiment-btn btn btn-secondary btn-wide"
        onClick=${newExp}
      >
        <${Plus} size=${15} strokeWidth=${2} /> New Experiment
      </button>
      <div class="section-title">Experiments</div>
      ${roots.map((exp) => {
        const isActive = active?.id === exp.id;
        const branchCount = Math.max(0, (exp.branches || []).length - 1);
        return html`
          <div key=${exp.id} class="experiment-group">
            <div
              class="experiment-item ${isActive ? "active" : ""}"
              onClick=${() => select(exp)}
            >
              <span class="experiment-status ${getExpStatus(exp)}">●</span>
               <span class="experiment-name"
                 >${exp.name || exp.id.slice(0, 8)}</span
               >
               <div
                 class="experiment-actions"
                 ref=${(el) => {
                   if (el) experimentMenuRefs.current[exp.id] = el;
                   else delete experimentMenuRefs.current[exp.id];
                 }}
               >
                 <button
                   class="experiment-menu-button"
                   aria-label="Experiment actions"
                   title="Experiment actions"
                   onClick=${(e) => {
                     e.stopPropagation();
                     setMenuExperimentId(
                       menuExperimentId === exp.id ? null : exp.id,
                     );
                   }}
                 >
                   <svg viewBox="0 0 24 24" aria-hidden="true">
                     <circle cx="12" cy="5" r="1.75" fill="currentColor" />
                     <circle cx="12" cy="12" r="1.75" fill="currentColor" />
                     <circle cx="12" cy="19" r="1.75" fill="currentColor" />
                   </svg>
                 </button>
                 ${menuExperimentId === exp.id
                   ? html`<div class="ctx-menu experiment-menu">
                        <button
                          class="ctx-item"
                          onClick=${(e) => {
                            e.stopPropagation();
                            openRenameExperiment(exp);
                          }}
                        >
                         Rename
                       </button>
                       <button
                         class="ctx-item danger"
                         onClick=${(e) => {
                           e.stopPropagation();
                           deleteExperiment(exp);
                         }}
                       >
                         Delete
                       </button>
                     </div>`
                   : null}
               </div>
                 ${branchCount > 0
                   ? html`<span class="experiment-count">${branchCount}</span>`
                   : null}
              </div>
           </div>
          `;
      })}
      ${renameExperimentTarget
        ? html`<div
            class="modal-backdrop"
            onClick=${closeRenameExperiment}
          >
            <div
              class="modal-card rename-modal"
              onClick=${(e) => e.stopPropagation()}
            >
              <div class="rename-modal-header">
                <div>
                  <div class="rename-modal-title">Rename experiment</div>
                </div>
              </div>
              <input
                class="form-input rename-modal-input"
                value=${renameExperimentName}
                onInput=${(e) => setRenameExperimentName(e.target.value)}
                onKeyDown=${(e) => {
                  if (e.key === "Enter") renameExperiment();
                  if (e.key === "Escape") closeRenameExperiment();
                }}
                autofocus
              />
              <div class="form-actions rename-modal-actions">
                <button
                  class="form-btn btn btn-secondary"
                  onClick=${closeRenameExperiment}
                  type="button"
                >
                  Cancel
                </button>
                <button
                  class="form-btn btn btn-primary"
                  onClick=${renameExperiment}
                  type="button"
                  disabled=${!renameExperimentName.trim()}
                >
                  Save
                </button>
              </div>
            </div>
          </div>`
        : null}
    </div>
  `;
}

function getExpStatus(exp) {
  const rootBranchId = exp.root_branch_id;
  const rootBranch = (exp.branches || []).find(b => b.id === rootBranchId);
  const cellIds = rootBranch?.cell_order || (exp.cells || []).map(c => c.id);
  const sts = cellIds.map(
    (cellId) => store.get().cellStatuses[`${exp.id}_${rootBranchId}_${cellId}`] || "idle",
  );
  if (sts.some((s) => s === "running" || s === "queued" || s === "saving"))
    return "running";
  if (sts.some((s) => s === "failed" || s === "timeout" || s === "save_error"))
    return "failed";
  if (sts.length && sts.every((s) => s === "done" || s === "cached"))
    return "done";
  return "idle";
}

// ── Sidebar: Files ────────────────────────────────────────────
function FilesPanel() {
  const fileTree = useStore((s) => s.fileTree);
  const filePreview = useStore((s) => s.filePreview);
  const project = useStore((s) => s.currentProject);

  useEffect(() => {
    store.set((s) => ({ ...s, fileTree: {}, filePreview: null }));
    loadDir("");
  }, [project?.id]);

  async function loadDir(path) {
    try {
      const dirKey = normalizeFileTreePath(path);
      const entries = await api.listFiles(dirKey, project?.id);
      store.set((s) => ({
        ...s,
        fileTree: { ...s.fileTree, [dirKey]: { entries, expanded: true } },
      }));
    } catch (e) {
      termLog(`Files: ${e}`, "error");
    }
  }

  function toggle(path, isDir) {
    if (!isDir) {
      const filePath = normalizeFileTreePath(path);
      api
        .readFile(filePath, project?.id)
        .then((content) => {
          const ext = filePath.split(".").pop()?.toLowerCase();
          const type = ["csv", "tsv"].includes(ext)
            ? "csv"
            : ["png", "jpg", "jpeg", "gif", "svg"].includes(ext)
              ? "image"
              : "text";
          store.set((s) => ({
            ...s,
            filePreview: { path: filePath, content, type },
          }));
        })
        .catch((e) => termLog(`Read: ${e}`, "error"));
      return;
    }
    const dirKey = normalizeFileTreePath(path);
    if (fileTree[dirKey]?.expanded) {
      store.set((s) => ({
        ...s,
        fileTree: {
          ...s.fileTree,
          [dirKey]: { ...s.fileTree[dirKey], expanded: false },
        },
      }));
    } else {
      loadDir(dirKey);
    }
  }

  function copyPath(p) {
    navigator.clipboard
      .writeText(p)
      .then(() => showToast("Copied!"))
      .catch(() => {});
  }

  function renderTree(path, depth = 0) {
    const dirKey = normalizeFileTreePath(path);
    const node = fileTree[dirKey];
    if (!node?.entries) return null;
    return node.entries.map((e) => {
      const fp = dirKey ? `${dirKey}/${e.name}` : e.name;
      const exp = fileTree[fp]?.expanded;
      return html`
        <div
          key=${e.name}
          class="file-item"
          style="padding-left:${depth * 16}px"
        >
          <div class="file-row" onClick=${() => toggle(fp, e.is_dir)}>
            <span class="file-icon"
              >${e.is_dir ? (exp ? "▾ 📁" : "▸ 📁") : "  📄"}</span
            >
            <span class="file-name">${e.name}</span>
            ${e.size !== undefined
              ? html`<span class="file-size">${formatSize(e.size)}</span>`
              : null}
            <button
              class="file-copy btn btn-ghost btn-icon"
              onClick=${(ev) => {
                ev.stopPropagation();
                copyPath(fp);
              }}
              title="Copy path"
              aria-label="Copy path"
            >
              <${Copy} size=${14} strokeWidth=${2} />
            </button>
          </div>
          ${e.is_dir && exp ? renderTree(fp, depth + 1) : null}
        </div>
      `;
    });
  }

  if (filePreview) {
    return html`
      <div class="sidebar-content">
        <div class="file-preview">
          <div class="file-preview-header">
            <button
              class="back-btn btn btn-ghost btn-sm"
              onClick=${() => store.set((s) => ({ ...s, filePreview: null }))}
            >
              <${ArrowLeft} size=${15} strokeWidth=${2} /> Back
            </button>
            <span class="file-preview-name"
              >${filePreview.path.split("/").pop()}</span
            >
            <button
              class="file-copy btn btn-ghost btn-icon"
              style="opacity:1"
              onClick=${() => copyPath(filePreview.path)}
              title="Copy path"
              aria-label="Copy path"
            >
              <${Copy} size=${14} strokeWidth=${2} />
            </button>
          </div>
          <div class="file-preview-content">
            ${filePreview.type === "image"
              ? html`<img
                  src="/api/files/read?${fileQuery(
                    filePreview.path,
                    project?.id,
                  )}"
                  style="max-width:100%"
                />`
              : filePreview.type === "csv"
                ? renderCSV(filePreview.content)
                : html`<pre class="code-preview">${filePreview.content}</pre>`}
          </div>
        </div>
      </div>
    `;
  }

  return html`
    <div class="sidebar-content">
      <div class="section-title">Files</div>
      ${renderTree("")}
    </div>
  `;
}

function formatSize(bytes) {
  if (bytes == null) return "";
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)}K`;
  return `${(bytes / 1048576).toFixed(1)}M`;
}

function renderCSV(text) {
  const lines = text
    .split("\n")
    .filter((l) => l.trim())
    .slice(0, 50);
  if (!lines.length) return html`<div style="color:var(--fg-3)">Empty</div>`;
  const hdr = lines[0].split(",");
  const rows = lines.slice(1).map((l) => l.split(","));
  return html`
    <table class="csv-preview">
      <thead>
        <tr>
          ${hdr.map((h) => html`<th key=${h}>${h.trim()}</th>`)}
        </tr>
      </thead>
      <tbody>
        ${rows.map(
          (r, i) =>
            html`<tr key=${i}>
              ${r.map((c, j) => html`<td key=${j}>${c.trim()}</td>`)}
            </tr>`,
        )}
      </tbody>
    </table>
  `;
}

// ── Sidebar container ─────────────────────────────────────────
function Sidebar() {
  const project = useStore((s) => s.currentProject);
  const tab = useStore((s) => s.sidebarTab);
  const collapsed = useStore((s) => s.sidebarCollapsed);
  const goHome = () => {
    window.location.hash = "/";
  };
  const setTab = (t) => store.set((s) => ({ ...s, sidebarTab: t }));
  const toggleSidebar = () => {
    store.set((s) => {
      const next = !s.sidebarCollapsed;
      persistSidebarCollapsed(next);
      return { ...s, sidebarCollapsed: next };
    });
  };
  return html`
    <aside class="sidebar ${collapsed ? "collapsed" : ""}">
      <button
        class="sidebar-toggle btn btn-icon btn-secondary"
        onClick=${toggleSidebar}
        aria-label=${collapsed ? "Expand side panel" : "Collapse side panel"}
        title=${collapsed ? "Show side panel" : "Hide side panel"}
      >
        ${collapsed
          ? html`<${ChevronRight}
              size=${16}
              strokeWidth=${2}
              aria-hidden="true"
            />`
          : html`<${ChevronLeft}
              size=${16}
              strokeWidth=${2}
              aria-hidden="true"
            />`}
      </button>
      <div class="sidebar-panel">
        ${project &&
        html`
          <div class="sidebar-project-header">
            <button
              class="sidebar-back-btn btn btn-ghost btn-sm"
              onClick=${goHome}
            >
              <${ArrowLeft} size=${15} strokeWidth=${2} /> Projects
            </button>
          </div>
        `}
        <div class="sidebar-tabs">
          <button
            class="sidebar-tab ${tab === "experiments" ? "active" : ""}"
            onClick=${() => setTab("experiments")}
          >
            Experiments
          </button>
          <button
            class="sidebar-tab ${tab === "files" ? "active" : ""}"
            onClick=${() => setTab("files")}
          >
            Files
          </button>
        </div>
        ${tab === "experiments"
          ? html`<${ExperimentsPanel} />`
          : html`<${FilesPanel} />`}
      </div>
    </aside>
  `;
}

// ── Titlebar ──────────────────────────────────────────────────
function Titlebar() {
  const ws = useStore((s) => s.wsConnected);
  const ae = useStore((s) => s.activeExperiment);
  const exps = useStore((s) => s.experiments);
  const experimentTrees = useStore((s) => s.experimentTrees);
  const activeTreeId = useStore((s) => s.activeTreeId);
  const activeBranchId = useStore((s) => s.activeBranchId);
  const proj = useStore((s) => s.currentProject);
  const activeTree =
    (experimentTrees || []).find(
      (tree) => tree.id === (activeTreeId || ae?.id),
    ) || null;
  const selectedBranch = activeTree
    ? (activeTree.branches || []).find(
        (branch) => branch.id === (activeBranchId || activeTree.root_branch_id),
      ) || null
    : null;
  const previewBranchSelected = !!(
    activeTree &&
    (activeTree.branches || []).length > 1 &&
    selectedBranch &&
    selectedBranch.id !== activeTree.root_branch_id
  );

  const goHome = () => {
    window.location.hash = "/";
  };

  const runAll = async () => {
    if (!ae) return;
    try {
      await runSelectedExecution(ae, activeTree, selectedBranch);
    } catch (e) {
      termLog(`Run all: ${e}`, "error");
    }
  };

  const branchExperiment = async () => {
    if (!ae || !activeTree) return;
    const name = prompt("Branch name:", `${ae.name || ae.id}_branch`);
    if (!name) return;
    try {
      const parentBranchId = activeBranchId || activeTree.root_branch_id;
      const parentBranch =
        (activeTree.branches || []).find(
          (branch) => branch.id === parentBranchId,
        ) || null;
      const branchPointCellId = parentBranch?.cell_order?.length
        ? parentBranch.cell_order[parentBranch.cell_order.length - 1]
        : null;
      if (!branchPointCellId)
        throw new Error("No branch point cell available");
      await createTreeBranchAndSelect(
        activeTree,
        parentBranchId,
        branchPointCellId,
        name,
      );
    } catch (e) {
      termLog(`Branch: ${e}`, "error");
    }
  };

  const switchExp = (e) => {
    const exp = exps.find((x) => x.id === e.target.value);
    if (exp) {
      setActiveExperimentState(exp);
      if (proj) window.location.hash = `/projects/${proj.id}/exp/${exp.id}`;
    }
  };

  const toggleTheme = () => {
    const el = document.documentElement;
    const next = el.getAttribute("data-theme") === "dark" ? "light" : "dark";
    el.setAttribute("data-theme", next);
    localStorage.setItem("tine-theme", next);
  };

  return html`
    <header class="titlebar ${proj ? "titlebar-project" : ""}">
      <div class="titlebar-left">
        <button class="titlebar-brand" onClick=${goHome} aria-label="Go to projects">
          <img
            class="titlebar-logo-mark titlebar-logo-dark"
            src="/tinelogowhitetext.png"
            alt="Tine"
          />
          <img
            class="titlebar-logo-mark titlebar-logo-light"
            src="/tinelogoblacktext.png"
            alt="Tine"
          />
        </button>
        ${proj
          ? html`<button
              class="titlebar-btn btn btn-ghost btn-sm projects-nav-btn"
              onClick=${goHome}
            >
              <${ArrowLeft} size=${15} strokeWidth=${2} /> Projects
            </button>`
          : null}
        ${ae &&
        html`
          <select
            class="experiment-select"
            value=${ae.id}
            onChange=${switchExp}
          >
            ${exps.map(
              (x) =>
                html`<option key=${x.id} value=${x.id}>
                  ${x.name || x.id.slice(0, 8)}
                </option>`,
            )}
          </select>
        `}
        ${selectedBranch && activeTree && (activeTree.branches || []).length > 1
          ? html`<span class="titlebar-branch-label"
              >${selectedBranch.name || selectedBranch.id}</span
            >`
          : null}
      </div>
      <div class="titlebar-right">
        ${ae &&
        html`
          <button
            class="titlebar-btn btn btn-primary"
            onClick=${runAll}
            title="Run all cells"
          >
            <${GitBranch} size=${15} strokeWidth=${2} /> Run All
          </button>
          <button
            class="titlebar-btn btn btn-secondary"
            onClick=${branchExperiment}
          >
            <${Play} size=${15} strokeWidth=${2} /> Branch
          </button>
        `}
        <button
          class="titlebar-btn btn btn-ghost btn-icon"
          onClick=${toggleTheme}
          title="Toggle theme"
          aria-label="Toggle theme"
        >
          <${MoonStar} size=${15} strokeWidth=${2} />
        </button>
        <span
          class="ws-indicator ${ws ? "connected" : ""}"
          title=${ws ? "Connected" : "Disconnected"}
        />
      </div>
    </header>
  `;
}

// ── Canvas Layout Engine ──────────────────────────────────────
const TRACK_WIDTH = 680;
const TRACK_GAP = 100;
const TRACK_PADDING = 40;

// ── Minimap ──────────────────────────────────────────────────
function Minimap({ positions, viewport, totalWidth, totalHeight, onNavigate }) {
  const scale = Math.min(160 / (totalWidth || 1), 100 / (totalHeight || 1), 1);
  return html`
    <div
      class="minimap"
      onClick=${(e) => {
        const rect = e.currentTarget.getBoundingClientRect();
        const mx = (e.clientX - rect.left) / scale;
        const my = (e.clientY - rect.top) / scale;
        onNavigate(mx, my);
      }}
    >
      ${Object.entries(positions).map(
        ([id, pos]) => html`
          <div
            key=${id}
            class="minimap-dot"
            style="left:${pos.x * scale}px;top:${pos.y *
            scale}px;width:${Math.max(
              4,
              TRACK_WIDTH * scale,
            )}px;height:4px;border-radius:2px;"
          />
        `,
      )}
      <div
        class="minimap-viewport"
        style="left:${(-viewport.x * scale) /
        viewport.zoom}px;top:${(-viewport.y * scale) /
        viewport.zoom}px;width:${(window.innerWidth * scale) /
        viewport.zoom}px;height:${(window.innerHeight * scale) /
        viewport.zoom}px;"
      />
    </div>
  `;
}

// ── Notebook View ────────────────────────────────────────────
function NotebookView() {
  const ae = useStore((s) => s.activeExperiment);
  const experiments = useStore((s) => s.experiments);
  const experimentTrees = useStore((s) => s.experimentTrees);
  const activeTreeId = useStore((s) => s.activeTreeId);
  const activeBranchId = useStore((s) => s.activeBranchId);
  const project = useStore((s) => s.currentProject);
  const activePipeline = ae || experiments[0] || null;
  const activeTree =
    (experimentTrees || []).find(
      (tree) => tree.id === (activeTreeId || activePipeline?.id),
    ) || null;
  const treeColumns = activeTree
    ? buildTreeBranchColumns(activeTree, activePipeline, activeBranchId)
    : [];
  const branchColumns = treeColumns.length
    ? treeColumns.map((column) => ({
        ...column,
        active:
          column.branchId === (activeBranchId || activeTree?.root_branch_id),
      }))
    : [];
  const stripRef = useRef(null);
  const cellShellRefs = useRef({});
  const branchColumnRefs = useRef({});
  const exportMenuRefs = useRef({});
  const [branchOffsets, setBranchOffsets] = useState({});
  const [exportMenuBranchKey, setExportMenuBranchKey] = useState(null);
  const selectedBranch = activeTree
    ? (activeTree.branches || []).find(
        (branch) => branch.id === (activeBranchId || activeTree.root_branch_id),
      ) || null
    : null;
  const activeExecutionId =
    selectedBranch && activeTree
      ? activeBranchPathCellIds(activeTree, selectedBranch.id)
          .map((nodeId) =>
            runtimeCellKey({
              treeId: activeTree.id,
              branchId: selectedBranch.id,
              nodeId,
            }),
          )
          .map((cellKey) => store.get().activeCellExecutions[cellKey])
          .find(Boolean)
      : null;

  useEffect(() => {
    if (!exportMenuBranchKey) return;
    const onMouseDown = (event) => {
      const activeMenu = exportMenuRefs.current[exportMenuBranchKey];
      if (!activeMenu?.contains(event.target)) {
        setExportMenuBranchKey(null);
      }
    };
    window.addEventListener("mousedown", onMouseDown);
    return () => window.removeEventListener("mousedown", onMouseDown);
  }, [exportMenuBranchKey]);

  useEffect(() => {
    setExportMenuBranchKey(null);
  }, [activeTree?.id, activeBranchId]);

  useEffect(() => {
    if (!branchColumns.length) return;
    Promise.all(
      branchColumns
        .filter((column) => column.mode === "tree")
        .map((column) =>
          hydrateTreeBranchLogs(
            activeTree.id,
            column.branchId,
            column.nodes,
          ).catch((e) => termLog(`Load logs: ${e}`, "error")),
        ),
    ).catch(() => {});
  }, [
    branchColumns
      .map(
        (column) =>
          `${column.key}:${column.nodes?.length || 0}:${column.readOnly ? "r" : "w"}`,
      )
      .join("|"),
  ]);

  useEffect(() => {
    if (!stripRef.current || !activePipeline) return;
    stripRef.current.scrollTo({
      left: stripRef.current.scrollWidth,
      behavior: "smooth",
    });
  }, [activePipeline?.id, branchColumns.length]);

  useEffect(() => {
    const measure = () => {
      const strip = stripRef.current;
      if (!strip) return;
      const stripRect = strip.getBoundingClientRect();
      const next = {};
      for (const column of branchColumns) {
        if (!column.parentBranchId || !column.parentCellId || !activeTree)
          continue;
        const sourceEl =
          cellShellRefs.current[
            runtimeCellKey({
              treeId: activeTree.id,
              branchId: column.parentBranchId,
              nodeId: column.parentCellId,
            })
          ];
        const targetEl = branchColumnRefs.current[column.key];
        if (!sourceEl || !targetEl) continue;
        const sourceRect = sourceEl.getBoundingClientRect();
        const sourceCenter =
          sourceRect.top - stripRect.top + sourceRect.height / 2;
        next[column.key] = Math.max(0, sourceCenter - 28);
      }
      setBranchOffsets(next);
    };

    const id = requestAnimationFrame(measure);
    window.addEventListener("resize", measure);
    return () => {
      cancelAnimationFrame(id);
      window.removeEventListener("resize", measure);
    };
  }, [
    branchColumns
      .map((column) => `${column.key}:${column.nodes?.length || 0}`)
      .join("|"),
    activePipeline?.id,
    activeTree?.id,
  ]);

  if (!activePipeline) {
    return html`<div class="notebook-empty">
      <div class="empty-message">
        <h3>No experiment selected</h3>
        <p>Pick one from the sidebar or create new.</p>
      </div>
    </div>`;
  }

  const addCellForColumn = async (column, pipeline) => {
    const nodes = column.nodes || pipeline.nodes || [];
    const last = nodes[nodes.length - 1];
    try {
      if (!activeTree || column.mode !== "tree" || !column.branchId) {
        throw new Error("Add cell requires tree branch context");
      }
      await api.addTreeCell(
        activeTree.id,
        column.branchId,
        {
          id: `cell_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
          tree_id: activeTree.id,
          branch_id: column.branchId,
          name: defaultCellName(nodes.length + 1),
          code: { source: "", language: "python" },
          upstream_cell_ids: last?.id ? [last.id] : [],
          declared_outputs: [],
          cache: true,
          map_over: null,
          map_concurrency: null,
          tags: {},
          revision_id: null,
          state: "clean",
        },
        last?.id || null,
      );
      await loadExperiments();
      showToast("Cell added");
    } catch (e) {
      termLog(`Add cell: ${e}`, "error");
    }
  };

  const exportBranchForColumn = async (column, format) => {
    if (!activeTree || column.mode !== "tree" || !column.branchId) return;
    try {
      const branch =
        (activeTree.branches || []).find((item) => item.id === column.branchId) || null;
      if (!branch) throw new Error(`Branch not found: ${column.branchId}`);
      const cells = treeBranchExportCells(activeTree, column.branchId);
      const baseName = `${activeTree.id}-${column.branchId}`;
      if (format === "py") {
        await saveTextExport(
          buildBranchPythonExport(activeTree, branch, cells),
          `${baseName}.py`,
        );
      } else if (format === "ipynb") {
        await saveTextExport(
          JSON.stringify(buildBranchNotebookExport(activeTree, branch, cells), null, 2),
          `${baseName}.ipynb`,
        );
      } else {
        throw new Error(`Unsupported export format: ${format}`);
      }
      setExportMenuBranchKey(null);
    } catch (e) {
      termLog(`Export branch: ${e}`, "error");
    }
  };

  const deleteTreeBranch = async (column) => {
    if (!activeTree || !column?.branchId || column.branchId === activeTree.root_branch_id)
      return;
    const branchName = column.branch?.name || column.branchId;
    const childCount = (activeTree.branches || []).filter(
      (branch) => branch.parent_branch_id === column.branchId,
    ).length;
    const detail =
      childCount > 0
        ? ` This will also delete ${childCount} child branch${childCount === 1 ? "" : "es"}.`
        : "";
    if (!confirm(`Delete branch "${branchName}"?${detail}`)) return;
    try {
      await api.deleteTreeBranch(activeTree.id, column.branchId);
      await loadExperiments();
      showToast("Branch deleted");
    } catch (e) {
      termLog(`Delete branch: ${e}`, "error");
    }
  };

  const runSelected = async () => {
    try {
      await runSelectedExecution(activePipeline, activeTree, selectedBranch);
    } catch (e) {
      termLog(`Run selected: ${e}`, "error");
    }
  };

  const runAllBranches = async () => {
    try {
      await runAllBranchesExecution(activePipeline, activeTree);
    } catch (e) {
      termLog(`Run all branches: ${e}`, "error");
    }
  };

  return html`
    <div class="notebook-page">
      <div class="notebook-toolbar">
        <div class="notebook-toolbar-left">
          <div class="notebook-toolbar-title">
            ${activePipeline.name || activePipeline.id.slice(0, 8)}
          </div>
          ${selectedBranch &&
          activeTree &&
          (activeTree.branches || []).length > 1
            ? html`<div class="notebook-toolbar-subtitle">
                ${selectedBranch.name || selectedBranch.id}
              </div>`
            : null}
        </div>
        <div class="notebook-toolbar-actions">
          ${activeExecutionId
            ? html`<button
                class="titlebar-btn btn btn-secondary btn-danger"
                onClick=${() => cancelExecutionById(activeExecutionId)}
                title="Terminate active run"
              >
                <svg viewBox="0 0 24 24" aria-hidden="true">
                  <path
                    d="M7 7h10v10H7z"
                    fill="currentColor"
                  />
                </svg>
                Terminate
              </button>`
            : null}
          <button
            class="titlebar-btn btn btn-primary"
            onClick=${runSelected}
            title="Run selected branch"
          >
            <${GitBranch} size=${15} strokeWidth=${2} /> Run Branch
          </button>
          ${activeTree && (activeTree.branches || []).length > 1
            ? html`
                <button
                  class="titlebar-btn btn btn-secondary"
                  onClick=${runAllBranches}
                  title="Run every branch from scratch"
                >
                  <${Play} size=${15} strokeWidth=${2} /> Run All
                </button>
              `
            : null}
        </div>
      </div>
      <div class="branch-strip" ref=${stripRef}>
        ${branchColumns.map((column) => {
          const pipeline = column.pipeline || {
            id: `${column.key}-placeholder`,
            name: column.branch?.name || column.branchId || "branch",
            nodes: column.nodes || [],
          };
          const visibleNodes = column.nodes || [];
          return html`
            <div
              key=${column.key}
              class="branch-column ${column.parentBranchId
                ? "branch-linked-column"
                : ""}"
              ref=${(el) => {
                if (el) branchColumnRefs.current[column.key] = el;
                else delete branchColumnRefs.current[column.key];
              }}
              style=${branchOffsets[column.key] != null
                ? { marginTop: `${branchOffsets[column.key]}px` }
                : null}
            >
                <div
                  class="notebook-doc branch-doc ${column.active
                  ? "active"
                  : ""} ${column.parentBranchId
                  ? "branch-child-doc"
                  : "branch-root-doc"}"
                >
                <div
                  class="branch-doc-header"
                    onClick=${() => {
                      store.set((s) => ({
                        ...s,
                        activeBranchId: column.branchId,
                      }));
                      if (!column.readOnly && activePipeline) {
                        setActiveExperimentState(activePipeline);
                        if (project)
                          window.location.hash = `/projects/${project.id}/exp/${activePipeline.id}`;
                      }
                    }}
                  >
                  <div class="branch-doc-heading">
                  <div
                    class="branch-doc-title-row"
                    ref=${(el) => {
                      if (el) exportMenuRefs.current[column.key] = el;
                      else delete exportMenuRefs.current[column.key];
                    }}
                  >
                      <div class="branch-doc-title">
                        ${pipeline.name || pipeline.id.slice(0, 8)}
                      </div>
                      ${exportMenuBranchKey === column.key
                        ? html`<div class="ctx-menu branch-export-menu">
                            <button
                              class="ctx-item"
                              onClick=${(e) => {
                                e.stopPropagation();
                                exportBranchForColumn(column, "py");
                              }}
                            >
                              Export .py
                            </button>
                            <button
                              class="ctx-item"
                              onClick=${(e) => {
                                e.stopPropagation();
                                exportBranchForColumn(column, "ipynb");
                              }}
                            >
                              Export .ipynb
                            </button>
                          </div>`
                        : null}
                    </div>
                    <div class="branch-doc-actions">
                      ${column.mode === "tree" && column.branchId
                        ? html`<button
                            class="branch-doc-export"
                            aria-label="Export branch"
                            title="Export branch"
                            onClick=${(e) => {
                              e.stopPropagation();
                              setExportMenuBranchKey(
                                exportMenuBranchKey === column.key
                                  ? null
                                  : column.key,
                              );
                            }}
                          >
                            <${Download} size=${14} strokeWidth=${2} />
                          </button>`
                        : null}
                      ${column.deletable
                        ? html`
                          <button
                            class="branch-doc-delete"
                            aria-label="Delete branch"
                            title="Delete branch"
                            onClick=${(e) => {
                              e.stopPropagation();
                              deleteTreeBranch(column);
                            }}
                          >
                            <svg viewBox="0 0 24 24" aria-hidden="true">
                              <path
                                d="M9 3h6l1 2h4v2H4V5h4l1-2Zm1 6h2v8h-2V9Zm4 0h2v8h-2V9ZM7 9h2v8H7V9Zm1 12a2 2 0 0 1-2-2V8h12v11a2 2 0 0 1-2 2H8Z"
                                fill="currentColor"
                              />
                            </svg>
                          </button>
                        `
                        : null}
                    </div>
                  </div>
                  <div class="branch-doc-subtitle">${column.subtitle}</div>
                </div>

                ${visibleNodes.length
                  ? visibleNodes.map(
                      (node, index) => html`
                        <div
                          key=${`${pipeline.id}_${node.id}`}
                          class="branch-node-stack"
                        >
                          <${Cell}
                            node=${node}
                            pipeline=${pipeline}
                            index=${index}
                            treeContext=${{
                              treeId: activeTree?.id || pipeline.id,
                              branchId: column.branchId,
                              rootBranchId:
                                activeTree?.root_branch_id || null,
                            }}
                            shellRef=${(el) => {
                              const refKey = runtimeCellKey({
                                treeId: activeTree?.id || pipeline.id,
                                branchId: column.branchId,
                                nodeId: node.id,
                              });
                              if (el) cellShellRefs.current[refKey] = el;
                              else delete cellShellRefs.current[refKey];
                            }}
                            showBranchHandle=${column.active && !column.readOnly}
                            readOnly=${column.readOnly}
                          />
                        </div>
                      `,
                    )
                  : html`<div class="branch-empty">
                      No cells in this branch yet.
                    </div>`}

                <div class="add-cell-section">
                  <button
                    class="add-cell-btn btn btn-secondary"
                    disabled=${column.branchId !== activeTree?.root_branch_id &&
                      !column.active}
                    title=${column.branchId !== activeTree?.root_branch_id &&
                    !column.active
                      ? "Select this branch to add a cell"
                      : "Add Cell"}
                    onClick=${() => {
                      const canAdd =
                        column.branchId === activeTree?.root_branch_id ||
                        column.active;
                      if (canAdd) addCellForColumn(column, pipeline);
                    }}
                  >
                    <${Plus} size=${15} strokeWidth=${2} /> Add Cell
                  </button>
                </div>
              </div>
            </div>
          `;
        })}
      </div>
    </div>
  `;
}

// ── Output Log ────────────────────────────────────────────────
function LogPanel() {
  const events = useStore((s) => s.terminalEvents);
  const [open, setOpen] = useState(false);
  const ref = useRef(null);
  useEffect(() => {
    if (ref.current && open) ref.current.scrollTop = ref.current.scrollHeight;
  }, [events, open]);

  return html`
    <div class="output-log ${open ? "" : "collapsed"}">
      <div class="output-log-header" onClick=${() => setOpen(!open)}>
        <span class="output-log-title">Output Log</span>
        <span class="output-log-toggle">${open ? "▾" : "▸"}</span>
      </div>
      ${open &&
      html`
        <div class="output-log-body" ref=${ref}>
          ${events.map(
            (event) => html`
              <div key=${event.id} class="log-line ${event.level}">
                <span class="log-time"
                  >[${new Date(event.ts).toLocaleTimeString()}]</span
                >
                ${terminalEventBadges(event).length
                  ? html`
                      <span class="log-badges">
                        ${terminalEventBadges(event).map(
                          (badge) =>
                            html`<span
                              key=${`${event.id}-${badge}`}
                              class="log-badge"
                              >${badge}</span
                            >`,
                        )}
                      </span>
                    `
                  : null}
                <span class="log-message">${terminalEventSummary(event)}</span>
              </div>
            `,
          )}
        </div>
      `}
    </div>
  `;
}

// ── Toast ──────────────────────────────────────────────────────
function Toast() {
  const t = useStore((s) => s.toast);
  if (!t) return null;
  return html`<div class="toast">${t.msg}</div>`;
}

// ── Project Dashboard ─────────────────────────────────────────
function Dashboard() {
  const projects = useStore((s) => s.projects);
  const [showForm, setShowForm] = useState(false);
  const [name, setName] = useState("");
  const [baseDir, setBaseDir] = useState("");
  const [desc, setDesc] = useState("");
  const [defaultProjectsDir, setDefaultProjectsDir] = useState("");
  const [pickerSource, setPickerSource] = useState(() =>
    hasDesktopBridge() ? "desktop" : null,
  );
  const supportsNativePicker =
    pickerSource === "desktop" || pickerSource === "server";
  const resolvedBaseDir = baseDir || defaultProjectsDir || "";
  const [submitAttempted, setSubmitAttempted] = useState(false);
  const [formError, setFormError] = useState("");
  const nameError = submitAttempted && !name.trim() ? "Project name is required." : "";
  const locationError =
    submitAttempted && !resolvedBaseDir.trim() ? "Choose a location for the project." : "";

  useEffect(() => {
    loadProjects();
  }, []);

  useEffect(() => {
    if (pickerSource === "desktop") return;
    let attempts = 0;
    const interval = setInterval(() => {
      attempts += 1;
      if (hasDesktopBridge()) {
        setPickerSource("desktop");
        clearInterval(interval);
      } else if (attempts >= 20) {
        clearInterval(interval);
      }
    }, 250);
    return () => clearInterval(interval);
  }, [pickerSource]);

  useEffect(() => {
    let cancelled = false;
    const loadDefaultProjectsDir = async () => {
      if (pickerSource === "desktop" || hasDesktopBridge()) {
        const value = await tauriInvoke("default_projects_dir");
        if (!cancelled) {
          if (value) setDefaultProjectsDir(String(value));
          setPickerSource("desktop");
        }
        return;
      }

      const value = await api.defaultProjectsDir();
      if (!cancelled) {
        if (value?.path) setDefaultProjectsDir(String(value.path));
        setPickerSource(value?.native_picker_available ? "server" : null);
      }
    };
    loadDefaultProjectsDir()
      .catch((e) => termLog(`Default projects dir: ${e}`, "error"));
    return () => {
      cancelled = true;
    };
  }, [pickerSource]);

  useEffect(() => {
    if (!defaultProjectsDir) return;
    setBaseDir((current) => current || defaultProjectsDir);
  }, [defaultProjectsDir]);

  const create = async () => {
    setSubmitAttempted(true);
    setFormError("");
    if (!name.trim() || !resolvedBaseDir.trim()) {
      return;
    }
    try {
      const created = await api.createProject({
        name: name.trim(),
        description: desc.trim() || null,
        workspace_dir: resolvedBaseDir.trim(),
      });
      const project = await api.project(created.id);
      setName("");
      setBaseDir(defaultProjectsDir || "");
      setDesc("");
      setSubmitAttempted(false);
      setFormError("");
      setShowForm(false);
      loadProjects();
      pick(project);
    } catch (e) {
      setFormError(String(e?.message || e));
      termLog(`Create project: ${e}`, "error");
    }
  };

  const openCreateForm = () => {
    setShowForm(true);
    setBaseDir((current) => current || defaultProjectsDir);
    setSubmitAttempted(false);
    setFormError("");
  };

  const resetCreateForm = () => {
    setShowForm(false);
    setName("");
    setBaseDir(defaultProjectsDir || "");
    setDesc("");
    setSubmitAttempted(false);
    setFormError("");
  };

  const chooseFolder = async () => {
    if (!supportsNativePicker) return;
    try {
      const initialDir = resolvedBaseDir || null;
      const picked =
        pickerSource === "desktop"
          ? await tauriInvoke("pick_project_folder", {
              initialDir,
            })
          : (await api.pickDirectory(initialDir))?.path;
        if (picked) {
          setBaseDir(String(picked));
          setFormError("");
        }
    } catch (e) {
      termLog(`Choose folder: ${e}`, "error");
    }
  };

  const pick = (p) => {
    store.set((s) => ({ ...s, currentProject: p, view: "notebook" }));
    window.location.hash = `/projects/${p.id}`;
  };

  return html`
    <div class="project-dashboard">
      <${Titlebar} />
      <div class="dashboard-content">
        <div class="dashboard-section">
          ${showForm
            ? html`
                <div class="project-card project-form-card project-create-dialog">
                  <div class="project-create-header">
                    <div class="project-create-title">New Project</div>
                  </div>
                  <div class="project-form-shell">
                  <div class="project-form">
                    <div class="project-form-group">
                      <label class="project-form-label" htmlFor="project-name-input">
                        Name
                      </label>
                      <input
                        id="project-name-input"
                        class="form-input"
                        placeholder="Project name"
                        value=${name}
                        onInput=${(e) => {
                          setName(e.target.value);
                          setFormError("");
                        }}
                        onKeyDown=${(e) => e.key === "Enter" && create()}
                        aria-invalid=${nameError ? "true" : "false"}
                      />
                      ${nameError &&
                      html`<div class="project-form-error">${nameError}</div>`}
                    </div>
                    <div class="project-form-group">
                      <label class="project-form-label" htmlFor="project-description-input">
                        Description
                        <span class="project-form-label-muted">(optional)</span>
                      </label>
                      <input
                        id="project-description-input"
                        class="form-input"
                        placeholder="What this project is for"
                        value=${desc}
                        onInput=${(e) => {
                          setDesc(e.target.value);
                          setFormError("");
                        }}
                        onKeyDown=${(e) => e.key === "Enter" && create()}
                      />
                    </div>
                    <div class="project-form-group">
                      <div class="project-form-label-row">
                        <label class="project-form-label" htmlFor="project-location-input">
                          Location
                        </label>
                      </div>
                      <div class="project-location-row">
                        <div
                          class="project-location-field ${locationError ? "invalid" : ""}"
                        >
                          <input
                            id="project-location-input"
                            class="form-input project-path-input project-location-input"
                            placeholder="Choose a folder"
                            value=${resolvedBaseDir}
                            onInput=${(e) => {
                              setBaseDir(e.target.value);
                              setFormError("");
                            }}
                            aria-invalid=${locationError ? "true" : "false"}
                          />
                          ${supportsNativePicker
                            ? html`<button
                                class="project-location-browse"
                                onClick=${chooseFolder}
                                type="button"
                              >
                                <${FolderSearch} size=${15} strokeWidth=${2} />
                                Browse
                              </button>`
                            : null}
                        </div>
                      </div>
                      ${locationError &&
                      html`<div class="project-form-error">
                        ${locationError}
                      </div>`}
                    </div>
                    ${formError &&
                    html`<div class="project-form-banner" role="alert">
                      ${formError}
                    </div>`}
                    <div class="form-actions">
                      <button
                        class="form-btn btn btn-secondary"
                        onClick=${resetCreateForm}
                      >
                        Cancel
                      </button>
                      <button
                        class="form-btn create btn btn-primary project-submit-btn"
                        onClick=${create}
                        disabled=${!name.trim() || !resolvedBaseDir.trim()}
                      >
                        Create Project
                      </button>
                    </div>
                  </div>
                  </div>
                </div>
              `
            : html`
                <div
                  class="project-card new-project-card project-create-card"
                  onClick=${openCreateForm}
                >
                  <div class="project-create-hero">
                    <div>
                      <div class="project-create-title">New Project</div>
                      <div class="project-create-copy">
                        Create a new workspace.
                      </div>
                    </div>
                    <span class="new-project-icon project-create-icon"
                      ><${Plus} size=${22} strokeWidth=${2} /></span
                    >
                  </div>
                </div>
              `}
        </div>

        ${projects.length
          ? html`
              <div class="dashboard-section">
                <div class="dashboard-section-title">Your Projects</div>
                <div class="project-grid">
                  ${projects.map(
                    (p) => html`
                      <div
                        class="project-card"
                        key=${p.id}
                        onClick=${() => pick(p)}
                      >
                        <div class="project-card-name">${p.name}</div>
                        ${p.description &&
                        html`<div class="project-card-desc">
                          ${p.description}
                        </div>`}
                        <div class="project-card-meta">
                          <span class="project-card-date"
                            >${p.created_at
                              ? new Date(p.created_at).toLocaleDateString()
                              : ""}</span
                          >
                        </div>
                      </div>
                    `,
                  )}
                </div>
              </div>
            `
          : html`
              <div class="empty-projects">
                <div class="empty-text">No projects yet</div>
                <div class="empty-hint">
                  Create a project to start experimenting
                </div>
              </div>
            `}
      </div>
    </div>
  `;
}

// ── Hash Router ───────────────────────────────────────────────
function parseHash() {
  const hash = window.location.hash.slice(1) || "/";
  const parts = hash.split("/").filter(Boolean);
  if (parts[0] === "projects" && parts[1]) {
    return { route: "project", projectId: parts[1], expId: parts[3] || null };
  }
  return { route: "dashboard" };
}

async function navigateFromHash() {
  const { route, projectId, expId } = parseHash();
  if (route === "dashboard") {
    store.set((s) => ({
      ...s,
      view: "dashboard",
      currentProject: null,
      activeExperiment: null,
      experimentTrees: [],
      activeTreeId: null,
      activeBranchId: null,
    }));
    loadProjects();
    return;
  }
  if (route === "project" && projectId) {
    try {
      const proj = await api.project(projectId);
      store.set((s) => ({ ...s, currentProject: proj, view: "notebook" }));
      const experiments = await loadExperiments();
      if (expId) {
        const exp =
          (store.get().experimentTrees || []).find((tree) => tree.id === expId) ||
          (experiments || []).find((tree) => tree.id === expId) ||
          null;
        if (exp) setActiveExperimentState(exp);
      } else if (experiments?.length) {
        setActiveExperimentState(experiments[0]);
      }
    } catch (e) {
      termLog(`Navigate: ${e}`, "error");
      store.set((s) => ({ ...s, view: "dashboard" }));
    }
  }
}

// ── App Root ──────────────────────────────────────────────────
function App() {
  const view = useStore((s) => s.view);
  const proj = useStore((s) => s.currentProject);
  const activeExperiment = useStore((s) => s.activeExperiment);
  const sidebarCollapsed = useStore((s) => s.sidebarCollapsed);
  const experimentHeartbeatRef = useRef(false);

  useEffect(() => {
    const saved = localStorage.getItem("tine-theme");
    if (saved) document.documentElement.setAttribute("data-theme", saved);

    connectWS();
    navigateFromHash();
    window.addEventListener("hashchange", navigateFromHash);
    return () => window.removeEventListener("hashchange", navigateFromHash);
  }, []);

  useEffect(() => {
    if (view !== "notebook" || !proj?.id) return;
    let disposed = false;
    const tick = async () => {
      if (
        disposed ||
        experimentHeartbeatRef.current ||
        document.visibilityState === "hidden"
      ) {
        return;
      }
      experimentHeartbeatRef.current = true;
      try {
        await loadExperiments();
      } finally {
        experimentHeartbeatRef.current = false;
      }
    };
    const interval = setInterval(tick, 2000);
    return () => {
      disposed = true;
      clearInterval(interval);
    };
  }, [view, proj?.id, activeExperiment?.id]);

  if (view === "dashboard" || !proj) {
    return html`<div class="app"><${Dashboard} /><${Toast} /></div>`;
  }

  return html`
    <div
      class="app notebook-app ${sidebarCollapsed ? "sidebar-collapsed" : ""}"
    >
      <${Sidebar} />
      <div class="main-content">
        <div class="notebook-container">
          <${NotebookView} />
          <${LogPanel} />
        </div>
      </div>
      <${Toast} />
    </div>
  `;
}

render(html`<${App} />`, document.getElementById("root"));
