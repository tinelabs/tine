export function fileQuery(path, projectId) {
  const params = new URLSearchParams();
  params.set("path", path || "");
  if (projectId) params.set("project_id", projectId);
  return params.toString();
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
