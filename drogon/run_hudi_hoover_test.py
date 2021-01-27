#!/usr/bin/env python3

"""
Run a HUDI Test on Hoover ingested production datasets.

Datasets are used in read-only mode and data is written to temporary directory.
"""

import json
import os
import select
import sys
import time
from argparse import ArgumentParser
from datetime import datetime, timedelta
from itertools import chain
from subprocess import Popen, PIPE, TimeoutExpired


try:
    import browser_cookie3
    import requests
except (ImportError):
    print("Please install the following extensions: browser_cookie3 requests")
    print("  pip3 install browser_cookie3 requests")
    sys.exit(-1)


def run(cmd, errstr, env=None, timeout=None, echo=False):
    """ Run the given command and return the stdout and stderr. 
        If the command does not exit cleanly, print the errstr and terminate the process 
    """
    if echo:
        stdout, stderr = sys.stdout, sys.stderr
    else:
        stdout, stderr = PIPE, PIPE

    proc = Popen(cmd, stdin=sys.stdin, stdout=stdout, stderr=stderr, shell=True, cwd=SOURCE_PATH, env=env)
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        if stdout:
            stdout = stdout.decode("utf-8")
        if stderr:
            stderr = stderr.decode("utf-8")
    except TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()
        print("ERROR: " + errstr)
        print("ERROR: command=" + cmd)
        sys.exit(-1)

    if proc.returncode != 0:
        print("ERROR: " + errstr)
        print("ERROR: command=" + cmd + ", retcode=" + str(proc.returncode))
        sys.exit(-1)

    return stdout, stderr


def get(url):
    #cookies = browser_cookie3.chrome(domain_name='.shs-dca1-secure.uberinternal.com')
    print(url)
    cookies = browser_cookie3.chrome()
    headers={"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_0_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.67 Safari/537.36"}
    response = requests.get(url, verify=True, headers=headers, cookies=cookies, timeout=10)
    return response.text


# Global config
USER = os.environ["USER"]
CWDIR = os.getcwd()
SCRIPT_DIR = os.path.dirname(__file__)
SOURCE_PATH = os.path.join(SCRIPT_DIR, "../")


# CLI arguments
parser = ArgumentParser(description=__doc__)
parser.add_argument("--baseuri", help="The URI of the production dataset", required=True)
parser.add_argument("--cluster", default="dca1", help="The cluster to use for testing (default=dca1)")
parser.add_argument("--tmpdir", default="/user/" + USER + "/hudi-test-suite", 
                    help="The temporary directory to use for writes")
parser.add_argument("--commits", action="append", nargs="+", required=False, help="Commits to replay")
parser.add_argument("--noprepped", default=False, action="store_true", help="Do not use prepped APIs for updated records")
parser.add_argument("--nomultiwrite", default=False, action="store_true", help="Do not use write API (which performs multiple operations in single commit). upsert API is used instead.")
parser.add_argument("--insert", default=False, action="store_true", help="Use insert API for inserted records (default: uses bulkInsert API)")
parser.add_argument("--rollback", default=False, action="store_true", help="Perform a Rollback operation 'after' the commit as a separate DAG node")
parser.add_argument("--clean", default=False, action="store_true", help="Perform a Clean operation 'after' the commit as a separate DAG node")
parser.add_argument("--parallel-clean", default=False, action="store_true", help="Perform a Clean operation in parallel during the commit as a separate DAG")
parser.add_argument("--auto-clean", default=False, action="store_true", help="Enable automatic cleaning (hudi.clean.automatic=true)")
parser.add_argument("--async-clean", default=False, action="store_true", help="Enable Async Clean operation during the replay (hudi.clean.async=true)")
parser.add_argument("--executors", default=256, help="Total executors to use")
parser.add_argument("--metadata", default=False, action="store_true", help="Add TableMetadataNode before other nodes")
parser.add_argument("-y", default=False, action="store_true", help="Do not prompt and answer 'y' for all choices.")
parser.add_argument("-x", default=False, action="store_true", help="Use existing DAG and properties rather than generating new ones")
args = parser.parse_args()


if "://" not in args.baseuri:
    print("Error: baseuri must include correct schema and authority")
    print("       Example: viewfs://ns-default/uber-data/tables/....")
    print("       Example: hdfs://ns-router-dca1/uber-data/tables/...")
    sys.exit(-1)

basepath = "/" + "/".join(args.baseuri.split("/")[3:])


# Get HUDI version
hudi_version, _ = run("mvn help:evaluate -Dexpression=project.version -q -DforceStdout", 
                      "Could not determine hudi version", timeout=60)


# Check for required JARs
bundle_path = "packaging/hudi-integ-test-bundle/target/hudi-integ-test-bundle-%s.jar" % hudi_version
bundle_abs_path = os.path.join(SOURCE_PATH, bundle_path)
if not os.path.exists(bundle_abs_path):
    print("ERROR: hudi-integ-test-bundle JAR not found at %s" % bundle_abs_path)
    print("ERROR: Did you create the package (mvn package)?")
    sys.exit(-1)


# Generate DAG if required
dag_header = """
dag_name: hudi-test-suite-replay
dag_content:"""

table_name = basepath.split("/")[-1]
replay_node = """
  commit_{INDEX}:
    config:
      instant_time: "{INSTANT_TIME}"
      use_bulk_insert: {USE_BULK_INSERT}
      use_prepped_api: {USE_PREPPED_API}
      use_multi_writes: {USE_MULTI_WRITES}
    type: CommitReplayNode
    deps: {DEPS}
"""
metadata_node = """
  metadata_0:
    config:
    type: TableMetadataNode
    deps: none
"""
clean_node = """
  clean_{INDEX}:
    config:
    type: CleanNode
    deps: {DEPS}
"""
rollback_node = """
  rollback_{INDEX}:
    config:
    type: RollbackNode
    deps: {DEPS}
"""

dag_filepath = "/tmp/%s.dag.yaml" % table_name
if os.path.exists(dag_filepath) and args.x:
    # Found existing. Do not generate as requested.
    pass
else:
    # generate new dag
    dag = dag_header
    deps = "none"
    
    if args.metadata:
        dag += metadata_node
        deps = "metadata_0"

    instant_time = datetime.now().strftime("%Y%m%d%H%M%S")
    if args.commits:
        for index, instant_time in enumerate(sorted(chain.from_iterable(args.commits))):
           dag += replay_node.format(INSTANT_TIME=instant_time, INDEX=index, DEPS=deps, 
                                     USE_BULK_INSERT=str(not args.insert).lower(),
                                     USE_PREPPED_API=str(not args.noprepped).lower(),
                                     USE_MULTI_WRITES=str(not args.nomultiwrite).lower())
           deps = "commit_" + str(index)

    if args.rollback:
        dag += rollback_node.format(DEPS=deps, INDEX="0")
        deps = "rollback_0"

    if args.clean or args.parallel_clean:
        deps = "none" if args.parallel_clean else deps
        dag += clean_node.format(DEPS=deps, INDEX="0")

    with open(dag_filepath, "w") as fout:
        fout.write(dag)


# Generate properties if required
properties_template = """
hoodie.datasource.write.recordkey.field=_row_key
hoodie.datasource.write.partitionpath.field=_hoodie_partition_path

hoodie.fs.union.baseuri={BASEURI}
hoodie.fs.union.deltauri={DELTAURI}
hoodie.fs.union.max.file.modification.timestamp={FILE_MOD_TS}
hoodie.fs.union.max.datafile.modification.timestamp={DATAFILE_MOD_TS}

hoodie.deltastreamer.source.dfs.root={BASEPATH}
hoodie.deltastreamer.schemaprovider.source.basepath={BASEURI}
hoodie.deltastreamer.schemaprovider.target.basepath={BASEURI}
hoodie.deltastreamer.schemaprovider.include.commit.files=true

hoodie.clean.automatic={CLEAN}
hoodie.clean.async={ASYNC_CLEAN}
hoodie.cleaner.commits.retained=2
hoodie.cleaner.policy=KEEP_LATEST_FILE_VERSIONS
hoodie.cleaner.fileversions.retained=1

hoodie.parquet.small.file.limit=0
hoodie.upsert.shuffle.parallelism=4096
hoodie.memory.merge.max.size=104857600
hoodie.insert.shuffle.parallelism=1024
hoodie.upsert.shuffle.parallelism=1024
hoodie.bulkinsert.shuffle.parallelism=1024
hoodie.write.buffer.limit.bytes=2147483647
hoodie.dataloss.check.enabled=true
hoodie.copyonwrite.insert.auto.split=true
hoodie.parquet.max.file.size=4294967296
hoodie.write.status.storage.level=DISK_ONLY
hoodie.bloom.index.parallelism=1024
hoodie.bloom.index.bucketized.checking=true

hoodie.metrics.on=true
hoodie.metrics.graphite.metric.prefix=stats.dca1.gauges.hoodie.dev.{USER}
hoodie.metrics.executor.enable=true

hoodie.embed.timeline.server=true
hoodie.embed.timeline.server.threads=4
hoodie.embed.timeline.server.compress=true
hoodie.embed.timeline.server.async=true

hoodie.metadata.enable=true
hoodie.metadata.validate=false

"""

props_filepath = "/tmp/%s.properties" % table_name
if os.path.exists(props_filepath) and args.x:
    # Found existing. Do not generate as requested.
    pass
else:
    # generate properties
    deltauri = "viewfs://ns-default/user/%s/hudi_test_suite/%s/input/.fs.union.delta" % (USER, table_name)
    file_mod_ts = instant_time
    # datefile mod should be less than file_mod so that the data files created as part of the 
    # instant being replayed are not visible.
    ts = datetime.strptime(instant_time, "%Y%m%d%H%M%S")
    ts = ts - timedelta(seconds=1)
    datafile_mod_ts = ts.strftime("%Y%m%d%H%M%S")
    properties = properties_template.format(BASEPATH=basepath, BASEURI=args.baseuri, DELTAURI=deltauri, USER=USER,
                                            CLEAN=str(args.auto_clean or args.async_clean).lower(), 
                                            ASYNC_CLEAN=str(args.async_clean).lower(),
                                            FILE_MOD_TS=file_mod_ts, DATAFILE_MOD_TS=datafile_mod_ts)
    with open(props_filepath, "w") as fout:
        fout.write(properties)


# Confirm user is ok
if args.x:
    print("\nUsing the following test suite files: ")
else:
    print("\nGenerated the following test suite files: ")
print("    DAG: " + dag_filepath)
print("    Properties: " + props_filepath)
#print("    Schema: " + schema_filepath)

if not args.y:
    choice = input("\nRun the test [y/N]? You can edit the above test files if needed. ") or "N"
    if choice.lower() != "y":
        sys.exit(0)


# Generate the environment to use while running drogon
drogon_env = {"UBER_LDAP_UID": USER,
              "DAG_FILEPATH": dag_filepath,
              "DAG_FILENAME": os.path.basename(dag_filepath),
              "PROPERTIES_FILEPATH": props_filepath,
              "PROPERTIES_FILENAME": os.path.basename(props_filepath),
              "HUDI_VERSION": hudi_version,
              "TABLE_NAME": table_name,
              "TABLE_TYPE": "COPY_ON_WRITE",
              "BASE_PATH": basepath,
              "NUM_EXECUTORS": str(args.executors)}
drogon_env.update(os.environ)

# drogon deploy
print("\n=========== Deploying to %s using drogon..." % args.cluster)
run("drogon deploy -c %s --app hudi_hoover_test" % args.cluster, "Could not deploy application using drogon", 
    env=drogon_env, timeout=1800, echo=True)


# drogon launch
print("\n=========== Launching application in %s using drogon..." % args.cluster)
output = run("drogon launch -c %s --app hudi_hoover_test" % args.cluster, "Could not launch application using drogon",
             env=drogon_env, timeout=7200, echo=True)

print("\n=========== Fetching application status using drogon...")
output, _ = run("drogon apps -a hudi_hoover_test", "Could not get application status using drogon", env=drogon_env, timeout=60)

# process the latest application
apps = output.split("\n")
apps.reverse()
sid = None
for app in apps:
    if app.startswith("Session: "):
        try:
            sid = app[9:].split(",")[0]
            break
        except:
            pass

if sid is None:
    print("Could not find drogon Session status. Please parse the logs manually")

output, _ = run("drogon status %s" % sid, "Could not get application status using drogon", env=drogon_env, timeout=60)
status = output.split("\n")
for line in status:
    if "Driver Logs:" in line:
        url = line[line.index("https:"):]
        print("Driver logs URL: " + url + "/stderr/?start=0")
 
