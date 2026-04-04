#!/usr/bin/env python
"""Legacy tine SDK demo kept for historical reference.

Prerequisites:
    This example reflects the old SDK-shaped Python surface and is not the
    current public install story.

    For wrapper-package development:
    cd packaging/python && python -m pip install -e .

This script demonstrates what tine does for you:
  1. Declare pipelines — common deps (numpy, pandas, scikit-learn, etc.) are
     pre-installed automatically, just like conda's defaults channel.
  2. tine creates isolated venvs (via uv) and installs everything automatically
  3. Nodes run in real Jupyter kernels — full Python, not a sandbox
  4. Results are cached content-addressably (change code → only that node reruns)
  5. Fork a pipeline, swap one node, run both — shared data via zero-copy mmap
  6. Compare metrics across experiments in one call

No notebooks. No manual env management. No copy-pasting between experiments.

Default packages (always available, same as conda defaults):
  numpy, pandas, polars, scipy, scikit-learn, matplotlib, seaborn,
  tqdm, requests, pillow  — plus pyarrow and ipykernel for tine internals.

If you need something extra, just add it via deps=["xgboost", "lightgbm"].

Auto-wiring (pytest-fixture style):
  - Parameter names = upstream node names (no explicit inputs needed)
  - Function name = output variable (no explicit outputs needed)
  - Scalar/dict-of-scalar returns are auto-extracted as metrics for compare()
"""

from __future__ import annotations

import json
import tempfile
import time

import tine


def main():
    with tempfile.TemporaryDirectory(prefix="tine_demo_") as workspace_dir:
        print(f"workspace: {workspace_dir}\n")

        # ── Open workspace ─────────────────────────────────────────
        ws = tine.Workspace(workspace_dir)

        # ══════════════════════════════════════════════════════════
        # Pipeline 1: Linear regression on synthetic data
        # ══════════════════════════════════════════════════════════
        #
        # Notice: NO deps= needed for numpy, pandas, scikit-learn.
        # tine ships them by default — just like conda.
        # You only need deps= for packages outside the defaults.
        linear = tine.Pipeline("linear_regression")

        @linear.node()
        def generate_data():
            """Create a synthetic regression dataset."""
            import numpy as np
            import pandas as pd

            rng = np.random.default_rng(42)
            n = 500
            x1 = rng.normal(0, 1, n)
            x2 = rng.normal(0, 1, n)
            noise = rng.normal(0, 0.3, n)
            y = 3 * x1 + 1.5 * x2 + noise  # true relationship
            return pd.DataFrame({"x1": x1, "x2": x2, "y": y})

        @linear.node()
        def train_linear(generate_data):
            """Fit a linear regression and report metrics.

            Returns a dict of scalars — tine auto-extracts each key
            as a metric (r2, rmse) for compare(). No print() hacks.
            """
            import numpy as np
            from sklearn.linear_model import LinearRegression
            from sklearn.model_selection import train_test_split

            X = generate_data[["x1", "x2"]].values
            y = generate_data["y"].values
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            model = LinearRegression().fit(X_train, y_train)
            r2 = model.score(X_test, y_test)
            rmse = float(np.sqrt(np.mean((model.predict(X_test) - y_test) ** 2)))

            # Return dict of scalars → auto-extracted as metrics
            return {"r2": r2, "rmse": rmse, "model": "LinearRegression"}

        @linear.node()
        def summarize(train_linear):
            """Print a human-readable summary."""
            print(f"Model: {train_linear.get('model', '?')}")
            return "done"

        print("━━━ Step 1: Create pipeline ━━━")
        print("  (no deps declared — numpy, pandas, sklearn are defaults)")
        pid1 = ws.create_pipeline(linear)
        print(f"  pipeline id: {pid1}")
        print(f"  nodes: {[n.id for n in linear._nodes]}\n")

        # ── Execute ────────────────────────────────────────────────
        print("━━━ Step 2: Execute (venv created, defaults installed, kernels started) ━━━")
        t0 = time.time()
        eid1 = ws.execute(pid1)
        elapsed1 = time.time() - t0
        print(f"  execution id: {eid1}")
        print(f"  wall time: {elapsed1:.1f}s\n")

        status1 = ws.status(eid1)
        print("  node statuses:")
        for node_id, node_status in status1.get("node_statuses", {}).items():
            print(f"    {node_id}: {node_status}")
        print()

        # ══════════════════════════════════════════════════════════
        # Pipeline 2: Fork → swap linear for Ridge regression
        # ══════════════════════════════════════════════════════════
        #
        # Only the train node changes. generate_data is IDENTICAL,
        # so tine will cache-hit it — zero re-execution, zero-copy
        # mmap injection into the new kernel.

        ridge_train_code = '''
def train_linear(generate_data):
    """Fit a Ridge regression and report metrics."""
    import numpy as np
    from sklearn.linear_model import Ridge
    from sklearn.model_selection import train_test_split

    X = generate_data[["x1", "x2"]].values
    y = generate_data["y"].values
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    model = Ridge(alpha=1.0).fit(X_train, y_train)
    r2 = model.score(X_test, y_test)
    rmse = float(np.sqrt(np.mean((model.predict(X_test) - y_test) ** 2)))

    return {"r2": r2, "rmse": rmse, "model": "Ridge(alpha=1.0)"}
'''

        print("━━━ Step 3: Fork pipeline (swap train node) ━━━")
        pid2 = ws.fork_pipeline(pid1, "ridge_regression", replacements={
            "train_linear": ridge_train_code,
        })
        print(f"  forked pipeline id: {pid2}\n")

        print("━━━ Step 4: Execute fork (generate_data = cache hit) ━━━")
        t0 = time.time()
        eid2 = ws.execute(pid2)
        elapsed2 = time.time() - t0
        print(f"  execution id: {eid2}")
        print(f"  wall time: {elapsed2:.1f}s")
        print(f"  (compare to first run: {elapsed1:.1f}s — data gen was cached)\n")

        status2 = ws.status(eid2)
        print("  node statuses:")
        for node_id, node_status in status2.get("node_statuses", {}).items():
            print(f"    {node_id}: {node_status}")
        print()

        # ══════════════════════════════════════════════════════════
        # Pipeline 3: XGBoost — needs a non-default dep
        # ══════════════════════════════════════════════════════════
        #
        # xgboost is NOT in the defaults, so we declare it via deps=.
        # All the defaults (numpy, pandas, sklearn) are still available.

        xgb = tine.Pipeline("xgboost_regression", deps=["xgboost"])

        @xgb.node()
        def generate_data():
            """Same data generation — will cache-hit if same workspace venv."""
            import numpy as np
            import pandas as pd

            rng = np.random.default_rng(42)
            n = 500
            x1 = rng.normal(0, 1, n)
            x2 = rng.normal(0, 1, n)
            noise = rng.normal(0, 0.3, n)
            y = 3 * x1 + 1.5 * x2 + noise
            return pd.DataFrame({"x1": x1, "x2": x2, "y": y})

        @xgb.node()
        def train_xgb(generate_data):
            """Fit XGBoost and report metrics."""
            import numpy as np
            import xgboost as xgb_lib
            from sklearn.model_selection import train_test_split

            X = generate_data[["x1", "x2"]].values
            y = generate_data["y"].values
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            model = xgb_lib.XGBRegressor(
                n_estimators=100, max_depth=3, learning_rate=0.1
            ).fit(X_train, y_train)
            r2 = float(model.score(X_test, y_test))
            rmse = float(np.sqrt(np.mean((model.predict(X_test) - y_test) ** 2)))

            return {"r2": r2, "rmse": rmse, "model": "XGBRegressor"}

        print("━━━ Step 5: Create XGBoost pipeline (deps=[\"xgboost\"]) ━━━")
        print("  defaults (numpy, pandas, sklearn) still included automatically")
        pid3 = ws.create_pipeline(xgb)
        print(f"  pipeline id: {pid3}\n")

        print("━━━ Step 6: Execute XGBoost ━━━")
        t0 = time.time()
        eid3 = ws.execute(pid3)
        elapsed3 = time.time() - t0
        print(f"  execution id: {eid3}")
        print(f"  wall time: {elapsed3:.1f}s\n")

        # ══════════════════════════════════════════════════════════
        # Compare all three experiments
        # ══════════════════════════════════════════════════════════

        print("━━━ Step 7: Compare all experiments ━━━")
        comparison = ws.compare([pid1, pid2, pid3], ["r2", "rmse"])
        print(json.dumps(comparison, indent=2))
        print()

        # ── Diff: what changed between linear and ridge? ──────────
        print("━━━ Step 8: Diff (linear vs ridge) ━━━")
        diff = ws.diff(pid1, pid2)
        print(json.dumps(diff, indent=2))
        print()

        # ── Snapshot: save state for reproducibility ──────────────
        print("━━━ Step 9: Snapshot + rollback ━━━")
        snap = ws.snapshot(pid1)
        print(f"  snapshot id: {snap}")

        ws.rollback(pid1, snap)
        print(f"  rolled back to {snap}\n")

        # ── Extra dependency example ───────────────────────────────
        print("━━━ Bonus: Pipeline with an extra dependency ━━━")
        minimal = tine.Pipeline(
            "minimal",
            deps=["httpx"],
        )

        @minimal.node()
        def fetch():
            import httpx
            r = httpx.get("https://httpbin.org/get")
            return {"status": r.status_code}

        pid_min = ws.create_pipeline(minimal)
        print(f"  pipeline id: {pid_min}")
        print('  deps=["httpx"] adds one extra package on top of the uv defaults\n')

        # ── List everything ───────────────────────────────────────
        print("━━━ Pipelines in workspace ━━━")
        for pid in ws.list_pipelines():
            print(f"  {pid}")

        print("\n═══ Demo complete ═══")


if __name__ == "__main__":
    main()
