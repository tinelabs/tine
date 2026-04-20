import test from "node:test";
import assert from "node:assert/strict";

import {
  activeBranchPathCellIds,
  buildExecutionStatusEvent,
  describeExecutionProgress,
  executionStatusLevel,
  fileQuery,
  hasHttpOrigin,
  normalizeFileTreePath,
  normalizeSavedExperimentTreePayload,
  pickActiveBranchId,
  resolveApiBaseUrl,
  resolveApiUrl,
  resolveWebSocketUrl,
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
      message: "Branch main preparing environment…",
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
      message: "Execution exec_1 queued. Position 1.",
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
