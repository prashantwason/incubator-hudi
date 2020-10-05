import argparse
import json
import os
import sys

SCRIPT_DESCRIPTION = """
This script accepts a path containing a list of schema files (named {name}.{NN}.avsc).
From the sorted list of schema files (sorted by version number NN), it generates two
dag nodes (one insert and one upsert) for every version of the schema.

Schema is expected to contain "timestamp" field, if it does not have the "timestamp",
it will be added to the test version of the schema. (original file remains unaffected).

"""

# For validating schema evolution, use 10 records for insert/upsert
num_to_insert = 0
num_to_upsert = 0
num_per_batch = 10

user = os.environ["USER"]

TEST_SUITE_JSON_FILE = "test-suite.drogon.json"

SCHEMA_VALIDATION_JSON_FILE = "schema-validation.drogon.json"
HIVE_CONFIG_FILE = "hive-site.xml"
CORE_CONFIG_FILE = "core-site.xml"
SECURITY_FILE = "hdrone-security.xml"

DROGON_EXTENSION = ".drogon.json"

INSERT_NODE = """{node_name}:
  config:
    record_size: 7000
    num_partitions_insert: 1
    start_partition: {start_partition}
    num_records_insert: {num_per_batch}
    reinitialize_context: true
    hoodie.deltastreamer.schemaprovider.source.schema.file: {evolved_schem_path}
  type: InsertNode
  deps: {dep}

"""

UPSERT_NODE = """{node_name}:
  config:
    record_size: 7000
    num_records_insert: {num_records_insert}
    num_records_upsert: {num_per_batch}
    num_partitions_upsert: {num_upsert_partitions}
    reinitialize_context: true
    hoodie.deltastreamer.schemaprovider.source.schema.file: {evolved_schem_path}
  type: UpsertNode
  deps: {dep}

"""

INSERT_VERIFY_NODE = """{node_name}:
  config:
    record_size: 7000

  type: ValidateInsertNode
  deps: {dep}

"""

HIVE_QUERY = """first_hive_sync:
  config:
    queue_name: "adhoc"
    engine: "mr"
  type: HiveSyncNode
  deps: {dep}

first_hive_query:
  config:
    hive_props:
      prop1: "set spark.yarn.queue=hadoop-platform-adhoc"
    hive_queries:
      query1: "select count(*) from rawdata_test.{user}_hudi_test_suite_table group by `_row_key` having count(*) > 1"
      result1: 0
      query2: "select count(*) from rawdata_test.{user}_hudi_test_suite_table"
      result2: {expected_count}
  type: HiveQueryNode
  deps: first_hive_sync

"""

PROPERTIES = """
hoodie.datasource.write.recordkey.field=_row_key
hoodie.datasource.write.keygenerator.class=org.apache.hudi.utilities.keygen.TimestampBasedKeyGenerator
hoodie.datasource.write.partitionpath.field=timestamp
hoodie.datasource.write.keytranslator.class=org.apache.hudi.DayBasedPartitionPathKeyTranslator

hoodie.deltastreamer.source.dfs.root=/tmp/{user}/hudi-test-suite/input
hoodie.deltastreamer.schemaprovider.source.schema.file=complex-source.avsc
hoodie.deltastreamer.schemaprovider.target.schema.file=complex-source.avsc
hoodie.deltastreamer.keygen.timebased.output.dateformat=yyyy/MM/dd
hoodie.deltastreamer.keygen.timebased.timestamp.type=UNIX_TIMESTAMP

hoodie.datasource.hive_sync.partition_fields=_hoodie_partition_path
hoodie.datasource.hive_sync.use_jdbc=false
hoodie.datasource.hive_sync.database=rawdata_test
hoodie.datasource.hive_sync.table={user}_hudi_test_suite_table
hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
hoodie.datasource.hive_sync.assume_date_partitioning=true
hoodie.datasource.hive_sync.use_pre_apache_input_format=true

hoodie.metrics.on=true
hoodie.metrics.graphite.metric.prefix=stats.dca1.gauges.hoodie.dev.{user}
hoodie.embed.timeline.server=true

hoodie.insert.shuffle.parallelism=10
hoodie.upsert.shuffle.parallelism=10
hoodie.bulkinsert.shuffle.parallelism=10

"""


def copy_and_check_schema_files(basepath, file):
    with open(os.path.join(basepath, file), "r") as orig_schema_file:
        contents = orig_schema_file.read()
        # if avro schema does not contain "_row_key" field add it.
        if not "_row_key" in contents:
            contents = contents.replace("\"fields\":[",
             "\"fields\":[{\"default\":null,\"type\":[\"null\",\"string\"],\"name\":\"_row_key\"},", 1)
        # add "timestamp" field.
        contents = contents.replace("\"fields\":[",
             "\"fields\":[{\"default\":null,\"type\":[\"null\",\"long\"],\"name\":\"timestamp\"},", 1)
        # create a local copy of the file.
        with open(file, "w") as schema_file:
            schema_file.write(contents)

def generate_drogon_json_file(jar, schema_files, freshstart):
    with open(TEST_SUITE_JSON_FILE, "r") as rfp:
        job = json.load(rfp)
        # update jar if provided
        if jar:
            job["deployment"]["artifacts"]["file"] = jar
        # add schema files to artifacts files.
        for schema in schema_files:
           if schema.endswith(".avsc"):
               job["deployment"]["artifacts"]["files"].append("drogon/" + schema)

        # add hive/core configuration files
        job["deployment"]["artifacts"]["files"].append("drogon/phx2/" + HIVE_CONFIG_FILE)
        job["launch"]["args"].extend(["--hive-config-file", HIVE_CONFIG_FILE])
        job["deployment"]["artifacts"]["files"].append("drogon/phx2/" + CORE_CONFIG_FILE)
        job["launch"]["args"].extend(["--core-config-file", CORE_CONFIG_FILE])
        job["deployment"]["artifacts"]["files"].append("drogon/phx2/" + SECURITY_FILE)
        job["launch"]["args"].extend(["--security-file", SECURITY_FILE])

        # COPY_ON_WRITE table,
        job["launch"]["args"] = ["COPY_ON_WRITE" if x == "MERGE_ON_READ" else x for x in job["launch"]["args"]]

        # dag location
        job["launch"]["args"] = ["/user/${UBER_LDAP_UID}/hudi_test_suite/dev_${UBER_LDAP_UID}/${dag_file}" if x == "${dag_file}" else x for x in job["launch"]["args"]]

        # skip-cleanup
        if freshstart == False:
            job["launch"]["args"].append("--skip-cleanup")

        # write updated job info back to the file
        with open(SCHEMA_VALIDATION_JSON_FILE, "w") as wfp:
            json.dump(job, wfp, indent=2)

def generate_dag(basepath, freshstart, maxversions):
    maxVer = 1000
    if not (maxversions is None):
        maxVer = eval(maxversions)
    num_to_insert_with_upsert_op = 10
    total_inserts = 0
    num_to_upsert = 0
    start_partition = 0
    with open("dag.yaml", "w") as dag_file:
        insert_name_fmt = "insert_{}"
        upsert_name_fmt = "upsert_{}"
        insert_verify_name_fmt = "insert_verify_{}"
        upsert_verify_name_fmt = "upsert_verify_{}"
        previous_node_name = "none"

        first_insert = True
        version = 0
        dir_files = os.listdir(basepath)
        dir_files.sort(key=lambda f: int(filter(str.isdigit, f)), reverse=True)
        latest_schema = dir_files[0]
        schemas_for_eval = []
        for schema in dir_files:
            if schema.endswith(".avsc"):
                # create a local copy of the schema file. Make sure timestamp field exists
                copy_and_check_schema_files(basepath, schema)
                schemas_for_eval.append(schema)

                # if freshstart is requested, add an insert node for every schema.
                # otherwise, add the insert node only for the last version of schema.
                if freshstart == 'True' or maxVer > 0:
                    # insert node
                    node_name = insert_name_fmt.format(version)
                    formatted_node = INSERT_NODE.format(
                        node_name=node_name,
                        num_per_batch=num_per_batch,
                        start_partition=start_partition,
                        dep=previous_node_name,
                        evolved_schem_path=schema
                    )
                    previous_node_name = node_name
                    dag_file.write(formatted_node)

                start_partition += 1
                total_inserts += num_per_batch
                version += 1

                # requested number of schema versions have been evaluated
                maxVer -= 1
                if maxVer <= 0:
                    break

        # upsert node
        node_name = upsert_name_fmt.format(version)
        formatted_node = UPSERT_NODE.format(
            node_name=node_name,
            num_per_batch=total_inserts,
            dep=previous_node_name,
            num_upsert_partitions=version,
            evolved_schem_path=latest_schema,
            num_records_insert=num_to_insert_with_upsert_op
        )
        previous_node_name = node_name
        dag_file.write(formatted_node)

        # query node
        #dag_file.write(HIVE_QUERY.format(expected_count=num_to_insert + num_to_upsert, dep=previous_node_name, user=user))
        return schemas_for_eval


def execute_schema_validation_job():
    job_name = SCHEMA_VALIDATION_JSON_FILE[:-len(DROGON_EXTENSION)]
    exit_code = os.system("drogon launch -a {} -d ".format(job_name))
    if exit_code != 0:
        print("Failed at {}".format(SCHEMA_VALIDATION_JSON_FILE))
        exit(1)

def run(jar, basepath, freshstart, maxversions):
    schema_files = generate_dag(basepath, freshstart, maxversions)
    # generate the json file, used for drogon launch.
    generate_drogon_json_file(jar, schema_files, freshstart)
    with open("test.properties", "w") as properties_file:
        properties_file.write(PROPERTIES.format(user=user))
    execute_schema_validation_job()
    # cleanup the generated json file
    os.remove(SCHEMA_VALIDATION_JSON_FILE)

def parse_args(args):
    """
    Parse command line arguments and return a dict.
    """
    parser = argparse.ArgumentParser(description=SCRIPT_DESCRIPTION)
    parser.add_argument("--freshstart", help="DAG is setup for fresh start", required=False)
    parser.add_argument("--maxversions", help="maximum number of versions to be validated", required=False)
    parser.add_argument("--jar", help="Path to alt version test-suite jar.", required=False)
    parser.add_argument("--basepath", help="Path to schema versions to be evaluated.", required=True)
    return parser.parse_args(args).__dict__

def main():
    """
    Parse arguments, generate dag.yaml, after updating schema (if needed).
    """
    parsed_args = parse_args(sys.argv[1:])
    run(**parsed_args)

if __name__ == "__main__":
    main()
