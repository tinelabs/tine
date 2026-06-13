import test from "node:test";
import assert from "node:assert/strict";

import {
  activeBranchPathCellIds,
  applyExecutionSnapshotToState,
  branchRequiresReplay,
  finishTrackedExecutionState,
  markReplayRequiredCellStatuses,
  nextAsyncRequestId,
  nextPollDelay,
  reconnectResyncTargets,
  shouldApplyScopedRequestResult,
} from "./app-helpers.js";

test("scenario: tracked branch execution reconnect resync restores completion and clears tracking", () => {
  const initialState = {
    activePollIds: { exec_1: true },
    executionTargets: {
      exec_1: {
        treeId: "tree_1",
        branchId: "main",
        targetKind: "experiment_tree_branch",
      },
    },
    executionStatuses: {},
    activeCellExecutions: {
      tree_1_main_step1: "exec_1",
      tree_1_main_step2: "exec_1",
    },
    cellStatuses: {
      tree_1_main_step1: "running",
      tree_1_main_step2: "running",
    },
  };

  const reconnectTargets = reconnectResyncTargets(initialState);
  assert.deepEqual(reconnectTargets, [
    {
      executionId: "exec_1",
      target: {
        treeId: "tree_1",
        branchId: "main",
        targetKind: "experiment_tree_branch",
      },
    },
  ]);

  const resyncedState = applyExecutionSnapshotToState(initialState, {
    executionId: "exec_1",
    target: reconnectTargets[0].target,
    status: {
      execution_id: "exec_1",
      tree_id: "tree_1",
      branch_id: "main",
      status: "completed",
      phase: "completed",
      node_statuses: {
        step1: "completed",
        step2: "completed",
      },
    },
  });

  assert.equal(resyncedState.cellStatuses.tree_1_main_step1, "done");
  assert.equal(resyncedState.cellStatuses.tree_1_main_step2, "done");
  assert.equal(resyncedState.executionStatuses.exec_1.phase, "completed");

  const finishedState = finishTrackedExecutionState(resyncedState, "exec_1");
  assert.deepEqual(finishedState.activePollIds, {});
  assert.deepEqual(finishedState.executionTargets, {});
  assert.deepEqual(finishedState.executionStatuses, {});
  assert.deepEqual(finishedState.activeCellExecutions, {});
});

test("scenario: replay-required runtime invalidates visible completed branch cells", () => {
  const tree = {
    root_branch_id: "main",
    branches: [
      {
        id: "main",
        parent_branch_id: null,
        branch_point_cell_id: null,
        cell_order: ["step1", "step2"],
      },
      {
        id: "alt",
        parent_branch_id: "main",
        branch_point_cell_id: "step1",
        cell_order: ["branch_step"],
      },
    ],
  };
  const nodeIds = activeBranchPathCellIds(tree, "alt");
  const state = {
    executionStatuses: {
      exec_2: {
        tree_id: "tree_1",
        branch_id: "alt",
        runtime: { replay_required: true },
      },
    },
    cellStatuses: {
      tree_1_alt_step1: "done",
      tree_1_alt_branch_step: "cached",
    },
  };

  assert.equal(
    branchRequiresReplay({
      treeId: "tree_1",
      branchId: "alt",
      runtimeState: { kernel_state: "ready" },
      executionStatuses: state.executionStatuses,
    }),
    true,
  );

  const nextCellStatuses = markReplayRequiredCellStatuses(state.cellStatuses, {
    treeId: "tree_1",
    branchId: "alt",
    nodeIds,
  });
  assert.equal(nextCellStatuses.tree_1_alt_step1, "stale");
  assert.equal(nextCellStatuses.tree_1_alt_branch_step, "stale");
});

test("scenario: long-running disconnected poll keeps converging with bounded interval", () => {
  // Polling no longer has an iteration cap: a 10-minute execution with a
  // dropped WebSocket converges via status polls. The backoff bounds load —
  // after a handful of iterations the interval settles at 5s and never
  // exceeds it, no matter how long the execution runs.
  let delay = 1000;
  let elapsedFirstMinute = 0;
  let polls = 0;
  while (elapsedFirstMinute + delay <= 60_000) {
    elapsedFirstMinute += delay;
    polls += 1;
    delay = nextPollDelay(delay);
  }
  assert.ok(polls >= 12, `backoff too aggressive: only ${polls} polls in the first minute`);
  assert.equal(nextPollDelay(delay), 5000, "interval must settle at the 5s cap");
});

test("scenario: stale experiment refresh is rejected after project navigation", () => {
  const firstRequestId = nextAsyncRequestId(0);
  const secondRequestId = nextAsyncRequestId(firstRequestId);

  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: firstRequestId,
      latestRequestId: secondRequestId,
      requestScope: "project-a",
      currentScope: "project-b",
    }),
    false,
  );

  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: secondRequestId,
      latestRequestId: secondRequestId,
      requestScope: "project-b",
      currentScope: "project-b",
    }),
    true,
  );
});

test("scenario: stale runtime snapshot is rejected after tree selection changes", () => {
  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: 4,
      latestRequestId: 5,
      requestScope: "tree-old",
      currentScope: "tree-new",
    }),
    false,
  );

  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: 5,
      latestRequestId: 5,
      requestScope: "tree-new",
      currentScope: "tree-new",
    }),
    true,
  );
});
test("scenario: per-tree runtime snapshot counters do not clobber concurrent refreshes", () => {
  // Mirrors refreshTreeRuntimeSnapshot freshness check after the per-tree
  // counter migration: a slow response for tree-A must still apply even if
  // tree-B has issued a newer request in the meantime.
  const counters = new Map();
  const begin = (treeId) => {
    const previous = counters.get(treeId) || 0;
    const requestId = nextAsyncRequestId(previous);
    counters.set(treeId, requestId);
    return requestId;
  };

  const reqA = begin("tree-a");
  const reqB = begin("tree-b");
  // tree-A response arrives after tree-B issued a newer request — but tree-A
  // has its own counter, so its latest is still reqA.
  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: reqA,
      latestRequestId: counters.get("tree-a") || 0,
      requestScope: "tree-a",
      currentScope: "tree-a",
    }),
    true,
  );
  // tree-B response also applies under its own scope.
  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: reqB,
      latestRequestId: counters.get("tree-b") || 0,
      requestScope: "tree-b",
      currentScope: "tree-b",
    }),
    true,
  );

  // A second tree-A request still invalidates its own earlier in-flight one.
  const reqA2 = begin("tree-a");
  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: reqA,
      latestRequestId: counters.get("tree-a") || 0,
      requestScope: "tree-a",
      currentScope: "tree-a",
    }),
    false,
  );
  assert.equal(
    shouldApplyScopedRequestResult({
      requestId: reqA2,
      latestRequestId: counters.get("tree-a") || 0,
      requestScope: "tree-a",
      currentScope: "tree-a",
    }),
    true,
  );
});
