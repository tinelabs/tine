import test from "node:test";
import assert from "node:assert/strict";

import {
  activeBranchPathCellIds,
  fileQuery,
  normalizeFileTreePath,
  normalizeSavedExperimentTreePayload,
  pickActiveBranchId,
  watchedDirForPath,
} from "./app-helpers.js";

test("fileQuery includes project scope when present", () => {
  assert.equal(fileQuery("nested/file.txt", "project-1"), "path=nested%2Ffile.txt&project_id=project-1");
  assert.equal(fileQuery("", null), "path=");
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
