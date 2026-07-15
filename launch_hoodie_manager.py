#!/usr/bin/python3

import os
import sys

"""
format of input file
command=<name of HoodieManager command> (can be ; separated list of commands)
arg1=value1
arg2=value2
arg3=value3
<or any drogon environment variable you want to override>

ex. orphan files
command=orphan_files
--operation=view
--basepath=hdfs://ns-router-dca1/uber-data/tables/rawdata/schemaless/row/mezzanine_trips_12
--partition=2022
--archived-commit-start-ts=0
DRIVER_CORES=2
DRIVER_MEMORY=13g
EXECUTOR_MEMORY=24g
EXECUTOR_MEMORY_OVERHEAD=8g
NUM_EXECUTORS=400
"""

file = sys.argv[1]
if not file:
    print ("Please provide path to input file for command and arguments")

datacenter = sys.argv[2]
if not datacenter:
    print ("Please provide data center (dca1, phx2, cloudlake-dca, cloudlake-phx)")

# deploy drogon

routing = ""
if datacenter == "cloudlake-dca" or datacenter == "cloudlake-phx":
    routing = f"X_UBER_REGION_ROUTING={datacenter}"
    datacenter = "dca1" if "dca" in datacenter else "phx2"

with open(file) as r:
    # first line is always command
    command = r.readline().split("=")[1].strip()

    # arguments key=value next lines. 1 key value pair argument per line
    arguments = []
    drogon_overrides = []
    for arg in r.readlines():
        arg = arg.strip()
        if arg == "":
            continue
        if arg.split("=")[0].isupper():
            drogon_overrides.append(arg)
        else:
            arguments.append(arg)

    # launch drogon
    commands = [
        ' '.join(drogon_overrides),
        f"COMMAND={command}",
        f"ARGS={','.join(arguments)}",
        routing,
        f"drogon launch -a hudi_manager -tb -d -c {datacenter}"
    ]
    print(' '.join(commands))
    os.system(' '.join(commands))

