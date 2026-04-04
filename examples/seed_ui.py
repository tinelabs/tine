#!/usr/bin/env python
"""Seed the UI demo workspace with pipelines, execute them, and fork.

Usage:
    python examples/seed_ui.py

Expects the tine server to be running against /tmp/tine_ui_demo.
This script uses the Python SDK (local Workspace, same directory).
"""
from __future__ import annotations
import time
import tine

WORKSPACE = "/tmp/tine_ui_demo"


def main():
    ws = tine.Workspace(WORKSPACE)

    # ── Pipeline 1: Linear regression ──────────────────────────
    linear = tine.Pipeline("linear_regression")

    @linear.node()
    def generate_data():
        import numpy as np, pandas as pd
        rng = np.random.default_rng(42)
        n = 500
        x1, x2 = rng.normal(0, 1, n), rng.normal(0, 1, n)
        y = 3 * x1 + 1.5 * x2 + rng.normal(0, 0.3, n)
        return pd.DataFrame({"x1": x1, "x2": x2, "y": y})

    @linear.node()
    def train_linear(generate_data):
        import numpy as np
        from sklearn.linear_model import LinearRegression
        from sklearn.model_selection import train_test_split
        X = generate_data[["x1", "x2"]].values
        y = generate_data["y"].values
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        model = LinearRegression().fit(X_train, y_train)
        r2 = model.score(X_test, y_test)
        rmse = float(np.sqrt(np.mean((model.predict(X_test) - y_test) ** 2)))
        return {"r2": r2, "rmse": rmse, "model": "LinearRegression"}

    @linear.node()
    def summarize(train_linear):
        print(f"Model: {train_linear.get('model', '?')}")
        return "done"

    print("Creating linear_regression pipeline...")
    pid1 = ws.create_pipeline(linear)
    print(f"  id: {pid1}")

    print("Executing linear_regression...")
    t0 = time.time()
    eid1 = ws.execute(pid1)
    print(f"  done in {time.time() - t0:.1f}s  (exec: {eid1})")

    s1 = ws.status(eid1)
    for nid, ns in s1.get("node_statuses", {}).items():
        print(f"    {nid}: {ns}")

    # ── Pipeline 2: Fork → Ridge ───────────────────────────────
    ridge_code = '''
def train_linear(generate_data):
    import numpy as np
    from sklearn.linear_model import Ridge
    from sklearn.model_selection import train_test_split
    X = generate_data[["x1", "x2"]].values
    y = generate_data["y"].values
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = Ridge(alpha=1.0).fit(X_train, y_train)
    r2 = model.score(X_test, y_test)
    rmse = float(np.sqrt(np.mean((model.predict(X_test) - y_test) ** 2)))
    return {"r2": r2, "rmse": rmse, "model": "Ridge(alpha=1.0)"}
'''

    print("\nForking → ridge_regression...")
    pid2 = ws.fork_pipeline(pid1, "ridge_regression", replacements={
        "train_linear": ridge_code,
    })
    print(f"  id: {pid2}")

    print("Executing ridge_regression...")
    t0 = time.time()
    eid2 = ws.execute(pid2)
    print(f"  done in {time.time() - t0:.1f}s  (exec: {eid2})")

    s2 = ws.status(eid2)
    for nid, ns in s2.get("node_statuses", {}).items():
        print(f"    {nid}: {ns}")

    # ── Pipeline 3: XGBoost ────────────────────────────────────
    xgb = tine.Pipeline("xgboost_regression", deps=["xgboost"])

    @xgb.node()
    def generate_data():
        import numpy as np, pandas as pd
        rng = np.random.default_rng(42)
        n = 500
        x1, x2 = rng.normal(0, 1, n), rng.normal(0, 1, n)
        y = 3 * x1 + 1.5 * x2 + rng.normal(0, 0.3, n)
        return pd.DataFrame({"x1": x1, "x2": x2, "y": y})

    @xgb.node()
    def train_xgb(generate_data):
        import numpy as np
        import xgboost as xgb_lib
        from sklearn.model_selection import train_test_split
        X = generate_data[["x1", "x2"]].values
        y = generate_data["y"].values
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        dtrain = xgb_lib.DMatrix(X_train, label=y_train)
        dtest = xgb_lib.DMatrix(X_test, label=y_test)
        params = {"max_depth": 4, "eta": 0.1, "objective": "reg:squarederror", "eval_metric": "rmse"}
        model = xgb_lib.train(params, dtrain, num_boost_round=100, evals=[(dtest, "test")], verbose_eval=False)
        preds = model.predict(dtest)
        r2 = float(1 - np.sum((y_test - preds)**2) / np.sum((y_test - y_test.mean())**2))
        rmse = float(np.sqrt(np.mean((preds - y_test)**2)))
        return {"r2": r2, "rmse": rmse, "model": "XGBoost(depth=4)"}

    @xgb.node()
    def summarize_xgb(train_xgb):
        print(f"Model: {train_xgb.get('model', '?')}")
        return "done"

    print("\nCreating xgboost_regression pipeline...")
    pid3 = ws.create_pipeline(xgb)
    print(f"  id: {pid3}")

    print("Executing xgboost_regression...")
    t0 = time.time()
    eid3 = ws.execute(pid3)
    print(f"  done in {time.time() - t0:.1f}s  (exec: {eid3})")

    s3 = ws.status(eid3)
    for nid, ns in s3.get("node_statuses", {}).items():
        print(f"    {nid}: {ns}")

    # ── Done ───────────────────────────────────────────────────
    print("\n✓ Workspace seeded. Refresh the UI at http://127.0.0.1:9473")
    print(f"  Pipelines: {pid1}, {pid2}, {pid3}")


if __name__ == "__main__":
    main()
