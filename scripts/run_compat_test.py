#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0.
"""
Orchestrate a cross-version Hudi compatibility scenario.

Reads drogon/hudi-spark-integ-test/compat-tests.json, and for the named
scenario submits ONE Drogon job per step, selecting each step's Hudi runtime by
the bundle on --jars. State survives between steps only via the Hudi table on
HDFS + HMS (each step is a fresh Spark app / JVM).

Usage:
    scripts/run_compat_test.py <scenario-name> [--config PATH] [--db-prefix P] [--dry-run]

Env passthrough (see project drogon workflow): proxy_user, X_UBER_REGION_ROUTING.

Version selection: the runner is a single thin jar; each step runs against its
declared version by supplying that version's hudi-spark-bundle on --jars
(HUDI_SPARK_BUNDLE_VERSION) and the sync mode that runtime supports (HIVE_SYNC_MODE).
Both are set per step from VERSION_MAP below. Every step uses the SAME db.table so
the table one version writes is what the other version reads/evolves. Run with
--dry-run first to inspect commands.
"""
import argparse
import json
import os
import subprocess
import sys

APP = "hudi_spark_compat_test"
# Maps the config's per-step "version" label to the concrete hudi-spark-bundle version
# supplied on --jars and the hive sync mode that runtime supports. Update the bundle
# versions as needed.
VERSION_MAP = {
    "0.14": {"bundle": "0.14.171", "sync_mode": "HIVEQL", "binary_type": "PARQUET_10"},
    "1.2":  {"bundle": "0.16.19",  "sync_mode": "HMS",    "binary_type": "PARQUET_13"},
}
DEFAULT_CONFIG = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "drogon", "hudi-spark-integ-test", "compat-tests.json")


def load_scenario(config_path, name):
    with open(config_path) as f:
        cfg = json.load(f)
    for s in cfg.get("scenarios", []):
        if s["name"] == name:
            return s
    names = ", ".join(s["name"] for s in cfg.get("scenarios", []))
    sys.exit(f"scenario '{name}' not found in {config_path}. Available: {names}")


def build_command():
    # `drogon launch` reads config vars from the shell env (TEST_*, DB, STEP,
    # HUDI_SPARK_BUNDLE_VERSION, HIVE_SYNC_MODE, SPARK_BINARY_TYPE); see
    # hudi_spark_integ_test.drogon.json. The Hudi runtime is selected by the bundle
    # on --jars (HUDI_SPARK_BUNDLE_VERSION), not by a jar tag.
    return ["drogon", "launch", "--app", APP, "-c", "phx2", "-tb"]


def step_env(version, test_category, test_case, idx, db):
    v = VERSION_MAP.get(version)
    if v is None:
        sys.exit(f"unknown version '{version}' in config; known: {', '.join(VERSION_MAP)}")
    env = dict(os.environ)
    env["TEST_CATEGORY"] = test_category   # resolves the step-based test class
    env["TEST_NAME"] = test_case           # scenario (test case) within that class
    env["DB"] = db
    env["STEP"] = str(idx)
    # Single thin runner jar; the Hudi runtime is swapped per step by supplying that
    # version's hudi-spark-bundle on --jars, and the hive sync mode differs by version.
    env["HUDI_SPARK_BUNDLE_VERSION"] = v["bundle"]
    env["HIVE_SYNC_MODE"] = v["sync_mode"]
    env["SPARK_BINARY_TYPE"] = v["binary_type"]
    env.setdefault("proxy_user", "hudi")   # drogon var is lowercase proxy_user
    env.setdefault("X_UBER_REGION_ROUTING", "phx2")
    return env


def main():
    ap = argparse.ArgumentParser(description="Run a cross-version Hudi compat scenario.")
    ap.add_argument("scenario", help="scenario name from compat-tests.json")
    ap.add_argument("--config", default=DEFAULT_CONFIG)
    ap.add_argument("--db-prefix", default="huditmp",
                    help="HMS database used as-is for every step (no suffix); both "
                         "versions operate on the same db.table")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the drogon submissions instead of running them")
    args = ap.parse_args()

    scenario = load_scenario(args.config, args.scenario)
    test_category = scenario["test_category"]
    test_case = scenario["test_case"]
    steps = scenario["steps"]
    db = args.db_prefix

    print(f"scenario : {args.scenario}")
    print(f"category : {test_category}")
    print(f"case     : {test_case}")
    print(f"db       : {db}")
    print(f"steps    : {len(steps)}")
    print("-" * 68)

    for idx, step in enumerate(steps):
        version = step["version"]
        name = step["name"]
        cmd = build_command()
        env = step_env(version, test_category, test_case, idx, db)
        print(f"[step {idx}] v{version:<5} {name!r}")
        if args.dry_run:
            shown = " ".join(f"{k}={env[k]!r}" for k in
                             ("TEST_CATEGORY", "TEST_NAME", "DB", "STEP",
                              "HUDI_SPARK_BUNDLE_VERSION", "HIVE_SYNC_MODE", "SPARK_BINARY_TYPE"))
            print(f"          {shown}")
            print(f"          {' '.join(cmd)}")
            continue
        try:
            rc = subprocess.call(cmd, env=env)
        except OSError as e:
            sys.exit(f"\nFAILED at step {idx} ({name!r}): could not run {cmd[0]!r}: {e}")
        if rc != 0:
            print(f"\nFAILED at step {idx} ({name!r}) on v{version} [exit {rc}].")
            print(f"State left on HDFS/HMS under db '{db}'. Re-running the scenario "
                  f"re-cleans in step 0; or drop db '{db}' manually.")
            sys.exit(1)

    print("-" * 68)
    print(f"OK: all {len(steps)} steps passed for '{args.scenario}'.")


if __name__ == "__main__":
    main()
