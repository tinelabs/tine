import test from "node:test";
import assert from "node:assert/strict";

import {
  activeBranchPathCellIds,
  appendTerminalEvent,
  branchRequiresReplay,
  buildExecutionStatusEvent,
  desktopMcpCommand,
  deriveRuntimeUi,
  describeExecutionProgress,
  executionStatusLevel,
  fileTreeRefreshKey,
  fileQuery,
  hasHttpOrigin,
  markReplayRequiredCellStatuses,
  nextAsyncRequestId,
  normalizeFileTreePath,
  normalizeSavedExperimentTreePayload,
  pickActiveBranchId,
  registerPendingFileTreeRefresh,
  reconnectResyncTargets,
  nextPollDelay,
  resolveApiBaseUrl,
  resolveApiUrl,
  resolveWebSocketUrl,
  shouldHydrateTerminalLogs,
  shouldRequestExecutionResync,
  shouldApplyScopedRequestResult,
  shouldCoalesceTerminalEvents,
  shouldReplaceTerminalEvent,
  shouldRenderStderr,
  watchedDirForPath,
} from "./app-helpers.js";

test("fileQuery includes project scope when present", () => {
  assert.equal(fileQuery("nested/file.txt", "project-1"), "path=nested%2Ffile.txt&project_id=project-1");
  assert.equal(fileQuery("", null), "path=");
});

test("hasHttpOrigin only accepts normal browser origins", () => {
  assert.equal(hasHttpOrigin({ protocol: "http:" }), true);
  assert.equal(hasHttpOrigin({ protocol: "https:" }), true);
  assert.equal(hasHttpOrigin({ protocol: "tauri:" }), false);
  assert.equal(hasHttpOrigin({ protocol: "" }), false);
});

test("resolveApiBaseUrl preserves the browser origin unchanged", async () => {
  const baseUrl = await resolveApiBaseUrl({
    locationLike: { protocol: "http:", host: "127.0.0.1:9473" },
    hasDesktopBridge: true,
    invoke: async () => 9999,
  });

  assert.equal(baseUrl, "http://127.0.0.1:9473");
});

test("resolveApiBaseUrl falls back to the embedded desktop server port", async () => {
  let attempts = 0;
  const baseUrl = await resolveApiBaseUrl({
    locationLike: { protocol: "tauri:", host: "tauri.localhost" },
    hasDesktopBridge: true,
    invoke: async () => {
      attempts += 1;
      return attempts >= 2
        ? { port: 63125, preferredPort: 9473, fellBack: true }
        : null;
    },
    retryDelayMs: 0,
    retryLimit: 3,
    sleep: async () => {},
  });

  assert.equal(baseUrl, "http://127.0.0.1:63125");
  assert.equal(attempts, 2);
});

test("resolveApiBaseUrl fails after retrying desktop bootstrap", async () => {
  await assert.rejects(
    resolveApiBaseUrl({
      locationLike: { protocol: "tauri:", host: "tauri.localhost" },
      hasDesktopBridge: true,
      invoke: async () => {
        throw new Error("bridge not ready");
      },
      retryDelayMs: 0,
      retryLimit: 2,
      sleep: async () => {},
    }),
    /bridge not ready/,
  );
});

test("resolveApiUrl builds absolute API URLs from a desktop base", () => {
  assert.equal(
    resolveApiUrl("/api/projects", "http://127.0.0.1:63125"),
    "http://127.0.0.1:63125/api/projects",
  );
  assert.equal(resolveApiUrl("/api/projects", ""), "/api/projects");
});

test("resolveWebSocketUrl preserves browser ws and desktop absolute ws behavior", () => {
  assert.equal(
    resolveWebSocketUrl({ protocol: "http:", host: "127.0.0.1:9473" }, ""),
    "ws://127.0.0.1:9473/ws",
  );
  assert.equal(
    resolveWebSocketUrl({ protocol: "tauri:", host: "tauri.localhost" }, "http://127.0.0.1:63125"),
    "ws://127.0.0.1:63125/ws",
  );
});

test("desktopMcpCommand uses the actual embedded server port", () => {
  assert.equal(desktopMcpCommand(null), null);
  assert.equal(
    desktopMcpCommand({ port: 9473, preferredPort: 9473, fellBack: false }),
    "tine-mcp --api-url http://127.0.0.1:9473",
  );
  assert.equal(
    desktopMcpCommand({ port: 63125, preferredPort: 9473, fellBack: true }),
    "tine-mcp --api-url http://127.0.0.1:63125",
  );
});

test("normalizeFileTreePath keeps root keys stable", () => {
  assert.equal(normalizeFileTreePath(""), "");
  assert.equal(normalizeFileTreePath("/"), "");
  assert.equal(normalizeFileTreePath("."), "");
  assert.equal(normalizeFileTreePath("/nested/"), "nested");
  assert.equal(normalizeFileTreePath("nested/deeper"), "nested/deeper");
});

test("watchedDirForPath maps root and nested file events to file-tree keys", () => {
  assert.equal(watchedDirForPath("foo.txt"), "");
  assert.equal(watchedDirForPath("/foo.txt"), "");
  assert.equal(watchedDirForPath("nested/foo.txt"), "nested");
  assert.equal(watchedDirForPath("/nested/deeper/foo.txt"), "nested/deeper");
});

test("fileTreeRefreshKey scopes refreshes by project and normalized directory", () => {
  assert.equal(fileTreeRefreshKey("", null), "workspace:.");
  assert.equal(fileTreeRefreshKey("/nested/", "project-a"), "project-a:nested");
  assert.equal(fileTreeRefreshKey("nested/deeper", "project-a"), "project-a:nested/deeper");
});

test("registerPendingFileTreeRefresh coalesces duplicate directory refreshes", () => {
  const first = registerPendingFileTreeRefresh(new Set(), "nested", "project-a");
  assert.equal(first.shouldSchedule, true);
  assert.equal(first.refreshKey, "project-a:nested");

  const duplicate = registerPendingFileTreeRefresh(
    first.pendingRefreshKeys,
    "/nested/",
    "project-a",
  );
  assert.equal(duplicate.shouldSchedule, false);
  assert.equal(duplicate.pendingRefreshKeys.size, 1);

  const sibling = registerPendingFileTreeRefresh(
    duplicate.pendingRefreshKeys,
    "nested/deeper",
    "project-a",
  );
  assert.equal(sibling.shouldSchedule, true);
  assert.equal(sibling.pendingRefreshKeys.size, 2);
});

test("nextAsyncRequestId monotonically increments request generations", () => {
  assert.equal(nextAsyncRequestId(), 1);
  assert.equal(nextAsyncRequestId(0), 1);
  assert.equal(nextAsyncRequestId(4), 5);
  assert.equal(nextAsyncRequestId(-10), 1);
  assert.equal(nextAsyncRequestId(Number.NaN), 1);
});

test("shouldApplyScopedRequestResult only accepts the latest matching scoped result", () => {
  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: 3,
      latestRequestId: 3,
      requestScope: "project-a",
      currentScope: "project-a",
    }),
    true,
  );
  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: 2,
      latestRequestId: 3,
      requestScope: "project-a",
      currentScope: "project-a",
    }),
    false,
  );
  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: 3,
      latestRequestId: 3,
      requestScope: "project-a",
      currentScope: "project-b",
    }),
    false,
  );
});

test("pickActiveBranchId preserves valid active branch and falls back to root", () => {
  const tree = {
    root_branch_id: "main",
    branches: [
      { id: "main" },
      { id: "alt-1" },
    ],
  };

  assert.equal(pickActiveBranchId(tree, "alt-1"), "alt-1");
  assert.equal(pickActiveBranchId(tree, "missing"), "main");
  assert.equal(pickActiveBranchId(null, "missing"), "main");
});

test("pickActiveBranchId falls back to persisted runtime branch before root", () => {
  const tree = {
    root_branch_id: "main",
    branches: [
      { id: "main" },
      { id: "alt-1" },
    ],
  };

  assert.equal(
    pickActiveBranchId(tree, null, { active_branch_id: "alt-1" }),
    "alt-1",
  );
  assert.equal(
    pickActiveBranchId(tree, "missing", { active_branch_id: "alt-1" }),
    "alt-1",
  );
  assert.equal(
    pickActiveBranchId(tree, null, { active_branch_id: "missing" }),
    "main",
  );
});

test("branchRequiresReplay allows a fresh branch run after intentional kernel shutdown", () => {
  assert.equal(
    branchRequiresReplay({
      treeId: "tree_1",
      branchId: "main",
      runtimeState: { kernel_state: "needs_replay" },
      runtimeHealth: { has_live_kernel: false },
      executionStatuses: {},
    }),
    false,
  );
});

test("branchRequiresReplay still blocks replay-required live kernels", () => {
  assert.equal(
    branchRequiresReplay({
      treeId: "tree_1",
      branchId: "main",
      runtimeState: { kernel_state: "needs_replay" },
      runtimeHealth: { has_live_kernel: true },
      executionStatuses: {},
    }),
    true,
  );
});

test("branchRequiresReplay allows a fresh branch run after kernel restart", () => {
  // After restart_tree_kernel resets the persisted state to Ready, a live
  // kernel exists but the state is no longer needs_replay, so Run Branch must
  // be runnable again (the reported bug: it stayed disabled post-restart).
  assert.equal(
    branchRequiresReplay({
      treeId: "tree_1",
      branchId: "main",
      runtimeState: { kernel_state: "ready" },
      runtimeHealth: { has_live_kernel: true },
      executionStatuses: {},
    }),
    false,
  );
});

test("activeBranchPathCellIds returns the selected branch lineage cells", () => {
  const tree = {
    root_branch_id: "main",
    branches: [
      { id: "main", parent_branch_id: null, branch_point_cell_id: null, cell_order: ["cell_1", "cell_2"] },
      { id: "alt", parent_branch_id: "main", branch_point_cell_id: "cell_1", cell_order: ["branch_1", "branch_2"] },
      { id: "sibling", parent_branch_id: "main", branch_point_cell_id: "cell_2", cell_order: ["sibling_1"] },
    ],
  };

  assert.deepEqual(activeBranchPathCellIds(tree, "alt"), ["cell_1", "branch_1", "branch_2"]);
  assert.deepEqual(activeBranchPathCellIds(tree, "main"), ["cell_1", "cell_2"]);
  assert.deepEqual(activeBranchPathCellIds(tree, "missing"), ["cell_1", "cell_2"]);
});

test("normalizeSavedExperimentTreePayload accepts canonical saved tree responses", () => {
  const tree = {
    id: "tree_1",
    name: "demo",
    cells: [{ id: "cell_1" }],
  };

  assert.deepEqual(normalizeSavedExperimentTreePayload(tree), tree);
});

test("normalizeSavedExperimentTreePayload unwraps wrapped experiment payloads", () => {
  const tree = {
    id: "tree_1",
    name: "demo",
    cells: [{ id: "cell_1", code: { source: "value = 3\n" } }],
  };

  assert.deepEqual(normalizeSavedExperimentTreePayload({ experiment: tree }), tree);
});

test("normalizeSavedExperimentTreePayload preserves legacy id-only compatibility", () => {
  const previousTree = {
    id: "tree_old",
    name: "demo",
    cells: [{ id: "cell_1", code: { source: "value = 1\n" } }],
  };

  assert.deepEqual(
    normalizeSavedExperimentTreePayload({ id: "tree_1" }, previousTree),
    {
      ...previousTree,
      id: "tree_1",
    },
  );
});

test("describeExecutionProgress surfaces queue positions for queued executions", () => {
  assert.deepEqual(
    describeExecutionProgress({ phase: "queued", queue_position: 3 }, "queued"),
    {
      label: "Queued #3",
      message: "Queued. Position 3.",
      active: true,
    },
  );
});

test("describeExecutionProgress prefers detailed lifecycle phases over generic running", () => {
  assert.deepEqual(
    describeExecutionProgress({ phase: "preparing_environment", status: "running" }, "running"),
    {
      label: "Preparing environment",
      message: "Preparing environment…",
      active: true,
    },
  );
});

test("describeExecutionProgress treats cancellation_requested as active work", () => {
  assert.deepEqual(
    describeExecutionProgress(
      { phase: "cancellation_requested", status: "running" },
      "running",
    ),
    {
      label: "Cancelling",
      message: "Cancelling…",
      active: true,
    },
  );
});

test("describeExecutionProgress treats serializing_artifacts as active work", () => {
  assert.deepEqual(
    describeExecutionProgress(
      { phase: "serializing_artifacts", status: "running" },
      "running",
    ),
    {
      label: "Serializing artifacts",
      message: "Serializing artifacts…",
      active: true,
    },
  );
});

test("describeExecutionProgress renders retrying as active work", () => {
  assert.deepEqual(
    describeExecutionProgress({ phase: "retrying", status: "running" }, "running"),
    {
      label: "Retrying",
      message: "Retrying…",
      active: true,
    },
  );
});

test("describeExecutionProgress renders timed_out as terminal work", () => {
  assert.deepEqual(
    describeExecutionProgress({ phase: "timed_out", status: "timed_out" }, "running"),
    {
      label: "Timed out",
      message: "Timed out",
      active: false,
    },
  );
});

test("describeExecutionProgress falls back to simple cell status when no snapshot exists", () => {
  assert.deepEqual(describeExecutionProgress(null, "failed"), {
    label: "Failed",
    message: "Failed",
    active: false,
  });
});

test("buildExecutionStatusEvent summarizes environment preparation for the output panel", () => {
  assert.deepEqual(
    buildExecutionStatusEvent(
      { execution_id: "exec_1", phase: "queued", status: "queued" },
      {
        execution_id: "exec_1",
        tree_id: "tree_1",
        branch_id: "main",
        phase: "preparing_environment",
        status: "running",
      },
      { treeId: "tree_1", branchId: "main" },
    ),
    {
      level: "info",
      kind: "execution",
      status: "preparing_environment",
      scope: {
        executionId: "exec_1",
        treeId: "tree_1",
        branchId: "main",
        nodeId: null,
        runtimeId: null,
      },
      message: "preparing environment…",
    },
  );
});

test("buildExecutionStatusEvent includes queue position changes", () => {
  assert.deepEqual(
    buildExecutionStatusEvent(
      { execution_id: "exec_1", phase: "queued", status: "queued", queue_position: 3 },
      { execution_id: "exec_1", phase: "queued", status: "queued", queue_position: 1 },
    ),
    {
      level: "info",
      kind: "execution",
      status: "queued",
      scope: {
        executionId: "exec_1",
        treeId: null,
        branchId: null,
        nodeId: null,
        runtimeId: null,
      },
      message: "queued. Position 1.",
    },
  );
});

test("executionStatusLevel maps execution phases to log severity", () => {
  assert.equal(executionStatusLevel({ phase: "preparing_environment" }), "info");
  assert.equal(executionStatusLevel({ phase: "cancellation_requested" }), "warn");
  assert.equal(executionStatusLevel({ phase: "failed" }), "error");
});

test("buildExecutionStatusEvent suppresses duplicate status logs", () => {
  assert.equal(
    buildExecutionStatusEvent(
      { execution_id: "exec_1", phase: "running", status: "running" },
      { execution_id: "exec_1", phase: "running", status: "running" },
      { treeId: "tree_1", branchId: "main" },
    ),
    null,
  );
});

test("buildExecutionStatusEvent marks queued branches skipped after earlier branch failure", () => {
  assert.deepEqual(
    buildExecutionStatusEvent(
      { execution_id: "exec_2", tree_id: "tree_1", branch_id: "branch_2", phase: "queued", status: "queued" },
      {
        execution_id: "exec_2",
        tree_id: "tree_1",
        branch_id: "branch_2",
        phase: "failed",
        status: "failed",
        node_statuses: {},
      },
      { treeId: "tree_1", branchId: "branch_2" },
    ),
    {
      level: "warn",
      kind: "execution",
      status: "rejected",
      scope: {
        executionId: "exec_2",
        treeId: "tree_1",
        branchId: "branch_2",
        nodeId: null,
        runtimeId: null,
      },
      message: "stopped after earlier branch failure",
    },
  );
});

test("shouldRenderStderr suppresses stderr when it duplicates structured traceback", () => {
  assert.equal(
    shouldRenderStderr({
      stderr:
        "Cell In[2], line 3\n  print i\n  ^\nSyntaxError: Missing parentheses in call to 'print'. Did you mean print(...)?",
      error: {
        ename: "SyntaxError",
        evalue: "Missing parentheses in call to 'print'. Did you mean print(...)?",
        traceback: [
          "(1093641009.py, line 3)",
          "",
          "Cell In[2], line 3",
          "  print i",
          "  ^",
          "SyntaxError: Missing parentheses in call to 'print'. Did you mean print(...)?",
        ],
      },
    }),
    false,
  );
});

test("shouldRenderStderr keeps stderr when it contains additional non-error output", () => {
  assert.equal(
    shouldRenderStderr({
      stderr: "warning: deprecated path\ncustom stderr line",
      error: {
        ename: "ValueError",
        evalue: "boom",
        traceback: ["ValueError: boom"],
      },
    }),
    true,
  );
});

test("shouldRequestExecutionResync requests recovery for node events missing target context", () => {
  assert.equal(
    shouldRequestExecutionResync({
      eventType: "NodeStarted",
      executionId: "exec_1",
      cellKey: null,
      executionTarget: null,
    }),
    true,
  );
  assert.equal(
    shouldRequestExecutionResync({
      eventType: "NodeStarted",
      executionId: "exec_1",
      cellKey: "tree_1_main_step1",
      executionTarget: null,
    }),
    false,
  );
  assert.equal(
    shouldRequestExecutionResync({
      eventType: "ExecutionStarted",
      executionId: "exec_1",
      cellKey: null,
      executionTarget: null,
    }),
    false,
  );
  assert.equal(
    shouldRequestExecutionResync({
      eventType: "NodeCompleted",
      executionId: "exec_1",
      cellKey: null,
      executionTarget: { treeId: "tree_1", branchId: "main" },
    }),
    false,
  );
});

test("shouldHydrateTerminalLogs only hydrates terminal logs once when no logs are loaded", () => {
  assert.equal(
    shouldHydrateTerminalLogs({
      status: "done",
      logs: null,
      hydrationKey: "tree_1:main:cell_1:done",
      loadedHydrationKey: null,
      isTreeExecutionRuntime: true,
      treeId: "tree_1",
      branchId: "main",
    }),
    true,
  );
  assert.equal(
    shouldHydrateTerminalLogs({
      status: "done",
      logs: { stdout: "ready", stderr: "", outputs: [], error: null, metrics: {} },
      hydrationKey: "tree_1:main:cell_1:done",
      loadedHydrationKey: null,
      isTreeExecutionRuntime: true,
      treeId: "tree_1",
      branchId: "main",
    }),
    false,
  );
  assert.equal(
    shouldHydrateTerminalLogs({
      status: "done",
      logs: null,
      hydrationKey: "tree_1:main:cell_1:done",
      loadedHydrationKey: "tree_1:main:cell_1:done",
      isTreeExecutionRuntime: true,
      treeId: "tree_1",
      branchId: "main",
    }),
    false,
  );
  assert.equal(
    shouldHydrateTerminalLogs({
      status: "running",
      logs: null,
      hydrationKey: "tree_1:main:cell_1:running",
      loadedHydrationKey: null,
      isTreeExecutionRuntime: true,
      treeId: "tree_1",
      branchId: "main",
    }),
    false,
  );
});

test("shouldCoalesceTerminalEvents matches equivalent terminal entries", () => {
  assert.equal(
    shouldCoalesceTerminalEvents(
      {
        kind: "node",
        status: "failed",
        level: "warn",
        scope: { executionId: "exec_1", treeId: "tree_1", branchId: "main", nodeId: "step1" },
        stream: null,
        message: null,
        error: { ename: "ValueError", evalue: "boom" },
      },
      {
        kind: "node",
        status: "failed",
        level: "error",
        scope: { executionId: "exec_1", treeId: "tree_1", branchId: "main", nodeId: "step1" },
        stream: null,
        message: null,
        error: { ename: "ValueError", evalue: "boom" },
      },
    ),
    true,
  );
});

test("appendTerminalEvent coalesces equivalent entries and keeps stronger severity", () => {
  const coalesced = appendTerminalEvent(
    [
      {
        id: "term-1",
        ts: 1,
        kind: "node",
        status: "failed",
        level: "warn",
        scope: { executionId: "exec_1", treeId: "tree_1", branchId: "main", nodeId: "step1" },
        stream: null,
        message: null,
        metrics: null,
        error: { ename: "ValueError", evalue: "boom", traceback: [] },
        duration_ms: null,
      },
    ],
    {
      id: "term-2",
      ts: 2,
      kind: "node",
      status: "failed",
      level: "error",
      scope: { executionId: "exec_1", treeId: "tree_1", branchId: "main", nodeId: "step1" },
      stream: null,
      message: null,
      metrics: null,
      error: { ename: "ValueError", evalue: "boom", traceback: [] },
      duration_ms: null,
    },
  );

  assert.equal(coalesced.length, 1);
  assert.equal(coalesced[0].level, "error");
  assert.equal(coalesced[0].id, "term-2");
});

test("shouldReplaceTerminalEvent matches warn-to-terminal-error escalation on same target", () => {
  assert.equal(
    shouldReplaceTerminalEvent(
      {
        kind: "node",
        status: "cancellation_requested",
        level: "warn",
        scope: { executionId: "exec_1", treeId: "tree_1", branchId: "main", nodeId: "step1" },
      },
      {
        kind: "node",
        status: "failed",
        level: "error",
        scope: { executionId: "exec_1", treeId: "tree_1", branchId: "main", nodeId: "step1" },
      },
    ),
    true,
  );
});

test("appendTerminalEvent replaces immediate warning with terminal error for same target", () => {
  const coalesced = appendTerminalEvent(
    [
      {
        id: "term-1",
        ts: 1,
        kind: "node",
        status: "cancellation_requested",
        level: "warn",
        scope: { executionId: "exec_1", treeId: "tree_1", branchId: "main", nodeId: "step1" },
        stream: null,
        message: "step1 cancellation requested",
        metrics: null,
        error: null,
        duration_ms: null,
      },
    ],
    {
      id: "term-2",
      ts: 2,
      kind: "node",
      status: "failed",
      level: "error",
      scope: { executionId: "exec_1", treeId: "tree_1", branchId: "main", nodeId: "step1" },
      stream: null,
      message: "step1 failed",
      metrics: null,
      error: { ename: "ValueError", evalue: "boom", traceback: [] },
      duration_ms: null,
    },
  );

  assert.equal(coalesced.length, 1);
  assert.equal(coalesced[0].level, "error");
  assert.equal(coalesced[0].status, "failed");
  assert.equal(coalesced[0].id, "term-2");
});

test("branchRequiresReplay returns true for replay-required runtime health or execution status", () => {
  assert.equal(
    branchRequiresReplay({
      treeId: "tree_1",
      branchId: "main",
      runtimeState: { kernel_state: "needs_replay" },
      executionStatuses: {},
    }),
    true,
  );

  assert.equal(
    branchRequiresReplay({
      treeId: "tree_1",
      branchId: "main",
      runtimeState: { kernel_state: "ready" },
      executionStatuses: {
        exec_1: {
          tree_id: "tree_1",
          branch_id: "main",
          runtime: { replay_required: true },
        },
      },
    }),
    true,
  );

  assert.equal(
    branchRequiresReplay({
      treeId: "tree_1",
      branchId: "main",
      runtimeState: { kernel_state: "ready" },
      executionStatuses: {
        exec_1: {
          tree_id: "tree_1",
          branch_id: "other",
          runtime: { replay_required: true },
        },
      },
    }),
    false,
  );
});

test("markReplayRequiredCellStatuses only marks completed and cached branch cells stale", () => {
  assert.deepEqual(
    markReplayRequiredCellStatuses(
      {
        tree_1_main_step1: "done",
        tree_1_main_step2: "cached",
        tree_1_main_step3: "running",
        tree_1_other_step1: "done",
      },
      {
        treeId: "tree_1",
        branchId: "main",
        nodeIds: ["step1", "step2", "step3"],
      },
    ),
    {
      tree_1_main_step1: "stale",
      tree_1_main_step2: "stale",
      tree_1_main_step3: "running",
      tree_1_other_step1: "done",
    },
  );
});

test("nextPollDelay backs off monotonically and caps at five seconds", () => {
  let delay = 1000;
  const observed = [];
  for (let i = 0; i < 10; i++) {
    const next = nextPollDelay(delay);
    assert.ok(next >= delay, `delay must not shrink: ${delay} -> ${next}`);
    assert.ok(next <= 5000, `delay must cap at 5000, got ${next}`);
    observed.push(next);
    delay = next;
  }
  assert.equal(observed[observed.length - 1], 5000);
});

test("nextPollDelay recovers from invalid previous delays", () => {
  assert.equal(nextPollDelay(undefined), 1500);
  assert.equal(nextPollDelay(0), 1500);
  assert.equal(nextPollDelay(-50), 1500);
});

test("reconnectResyncTargets returns only actively tracked executions with targets", () => {
  assert.deepEqual(
    reconnectResyncTargets({
      activePollIds: {
        exec_1: true,
        exec_2: false,
        exec_3: true,
      },
      executionTargets: {
        exec_1: { treeId: "tree_1", branchId: "main" },
        exec_3: { treeId: "tree_2", branchId: "alt" },
      },
    }),
    [
      {
        executionId: "exec_1",
        target: { treeId: "tree_1", branchId: "main" },
      },
      {
        executionId: "exec_3",
        target: { treeId: "tree_2", branchId: "alt" },
      },
    ],
  );
});


test("deriveRuntimeUi: busy execution wins over kernel state", () => {
  const ui = deriveRuntimeUi({
    kernelState: "ready",
    hasLiveKernel: true,
    isBusy: true,
  });
  assert.equal(ui.tone, "busy");
  assert.equal(ui.runBlocked, false);
  assert.deepEqual(ui.menuActions, []);
});

test("deriveRuntimeUi: kernel_lost surfaces restart-only", () => {
  const ui = deriveRuntimeUi({
    kernelState: "kernel_lost",
    hasLiveKernel: false,
    isBusy: false,
  });
  assert.equal(ui.tone, "error");
  assert.equal(ui.runBlocked, true);
  assert.deepEqual(
    ui.menuActions.map((a) => a.id),
    ["restart"],
  );
});

test("deriveRuntimeUi: switching has no actions and blocks runs", () => {
  const ui = deriveRuntimeUi({
    kernelState: "switching",
    hasLiveKernel: true,
    isBusy: false,
  });
  assert.equal(ui.tone, "busy");
  assert.equal(ui.runBlocked, true);
  assert.deepEqual(ui.menuActions, []);
});

test("deriveRuntimeUi: kernel-off (no live kernel) shows muted with no actions", () => {
  const ui = deriveRuntimeUi({
    kernelState: "ready",
    hasLiveKernel: false,
    isBusy: false,
  });
  assert.equal(ui.tone, "muted");
  assert.equal(ui.runBlocked, false);
  assert.deepEqual(ui.menuActions, []);
});

test("deriveRuntimeUi: needs_replay with live kernel offers restart and shutdown", () => {
  const ui = deriveRuntimeUi({
    kernelState: "needs_replay",
    hasLiveKernel: true,
    isBusy: false,
  });
  assert.equal(ui.tone, "warn");
  assert.equal(ui.runBlocked, true);
  assert.deepEqual(
    ui.menuActions.map((a) => a.id),
    ["restart", "shutdown"],
  );
  assert.equal(
    ui.menuActions.find((a) => a.id === "shutdown").kind,
    "danger",
  );
});

test("deriveRuntimeUi: ready kernel offers full menu", () => {
  const ui = deriveRuntimeUi({
    kernelState: "ready",
    hasLiveKernel: true,
    isBusy: false,
  });
  assert.equal(ui.tone, "ready");
  assert.equal(ui.runBlocked, false);
  assert.deepEqual(
    ui.menuActions.map((a) => a.id),
    ["restart", "shutdown"],
  );
});

test("deriveRuntimeUi: tolerates missing/garbage inputs", () => {
  const empty = deriveRuntimeUi();
  assert.equal(empty.tone, "muted");
  assert.deepEqual(empty.menuActions, []);
  const garbage = deriveRuntimeUi({
    kernelState: "  KERNEL_LOST  ",
    hasLiveKernel: false,
  });
  assert.equal(garbage.tone, "error");
  assert.deepEqual(
    garbage.menuActions.map((a) => a.id),
    ["restart"],
  );
});
