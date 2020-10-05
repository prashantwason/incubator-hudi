#! /usr/bin/env python

import argparse
from copy import deepcopy
import json
import os
import sys

SCRIPT_DESCRIPTION = """
This script accepts a path to a test-suite-bundle jar and to two yaml 
files. The two yaml files represent one full workload dag that has been
split into a first and second part. The path to the test-suite-bundle jar
is jar that is built with a different version of hudi / avro / parquet /
spark / other dependency. 

The script launches spark jobs in the following order:  
1. current version jar with yaml_1, then provided version jar with yaml_2.
2. provided version jar with yaml_1, then current version jar with yaml_2.
"""

TEST_SUITE_JSON_FILE = "test-suite.drogon.json"

DROGON_EXTENSION = ".drogon.json"

JSON_RUN_1_PART_1 = "split_test_r1_p1.drogon.json"
JSON_RUN_1_PART_2 = "split_test_r1_p2.drogon.json"
JSON_RUN_2_PART_1 = "split_test_r2_p1.drogon.json"
JSON_RUN_2_PART_2 = "split_test_r2_p2.drogon.json"

drogon_jobs = [
    JSON_RUN_1_PART_1,
    JSON_RUN_1_PART_2,
    JSON_RUN_2_PART_1,
    JSON_RUN_2_PART_2,
]

generated_files = [*drogon_jobs]

def j_read(file_name):
    """
    Read from a json file path and return a dict.
    """
    with open(file_name, "r") as fp:
        return json.load(fp)


def j_write(obj, file_name):
    """
    Write a dict as a json file.
    """
    with open(file_name, "w") as fp:
        json.dump(obj, fp, indent=2)


def _prepare_job(dag_file, jar, skip_clean, output_file):
    """
    Prepare a .drogon.json file representing a
    """
    job = j_read(TEST_SUITE_JSON_FILE)
    if jar:
        job["deployment"]["artifacts"]["file"] = jar
    if skip_clean:
        job["launch"]["args"].append("--skip-cleanup")
    job["variables"]["dag_file"] = dag_file
    j_write(job, output_file)


def prepare_jobs(jar, dag1, dag2):
    """
    Prepare drogon job configs to run:
    - version_a with yaml_1 and then version_b with yaml_2
    followed by
    - version_b with yaml_2 and then version_a with yaml_2

    Here yaml_1 + yaml_2 represent one full dag workload.
    """
    _prepare_job(dag1, "", False, JSON_RUN_1_PART_1)
    _prepare_job(dag2, jar, True, JSON_RUN_1_PART_2)

    _prepare_job(dag1, jar, False, JSON_RUN_2_PART_1)
    _prepare_job(dag2, "", True, JSON_RUN_2_PART_2)


def clean_up():
    """
    Clean the local files generated as part of this execution.
    """
    for file_name in generated_files:
        os.remove(file_name)


def execute_drogon_jobs():
    """
    Sequentially launch the drogon jobs to run the dag.
    """
    for drogon_job in drogon_jobs:
        job_name = drogon_job[:-len(DROGON_EXTENSION)]
        exit_code = os.system("drogon launch -a {} -d".format(job_name))
        if exit_code != 0:
            print("Failed at {}".format(drogon_job))
            exit(1)


def run(jar, dag1, dag2):
    """
    Run this split-yaml test with two jar versions.
    """
    prepare_jobs(jar, dag1, dag2)
    execute_drogon_jobs()
    clean_up()


def main():
    """
    Parse arguments and run workload.
    """
    parsed_args = parse_args(sys.argv[1:])
    run(**parsed_args)


def parse_args(args):
    """
    Parse command line arguments and return a dict.
    """
    parser = argparse.ArgumentParser(description=SCRIPT_DESCRIPTION)
    parser.add_argument("--jar", help="Path to alt version test-suite jar.", required=True)
    parser.add_argument("--dag1", help="Path to yaml file: first part of workload dag.", required=True)
    parser.add_argument("--dag2", help="Path to yaml file: second part of workload dag.", required=True)
    return parser.parse_args(args).__dict__


if __name__ == "__main__":
    main()
