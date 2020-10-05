import os


num_to_insert = 5000000
num_to_upsert = 50000
num_per_batch = 5000

user = os.environ["USER"]

INSERT_NODE = """{node_name}:
  config:
    record_size: 70000
    num_insert_partitions: 1
    num_records_insert: {num_per_batch}
  type: InsertNode
  deps: {dep}

"""

UPSERT_NODE = """{node_name}:
  config:
    record_size: 70000
    num_insert_partitions: 1
    num_records_insert: {num_per_batch}
    num_records_upsert: {num_per_batch}
    num_upsert_partitions: 1
  type: UpsertNode
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

with open("dag.yaml", "w") as dag_file:
    insert_name_fmt = "insert_{}"
    upsert_name_fmt = "upsert_{}"
    previous_node_name = "none"

    for i in range(int(num_to_insert / num_per_batch)):
        node_name = insert_name_fmt.format(i)
        formatted_node = INSERT_NODE.format(
            node_name=node_name,
            num_per_batch=num_per_batch,
            dep=previous_node_name
        )
        previous_node_name = node_name
        dag_file.write(formatted_node)

    for i in range(int(num_to_upsert / num_per_batch)):
        node_name = upsert_name_fmt.format(i)
        formatted_node = UPSERT_NODE.format(
            node_name=node_name,
            num_per_batch=num_per_batch,
            dep=previous_node_name
        )
        previous_node_name = node_name
        dag_file.write(formatted_node)

    dag_file.write(HIVE_QUERY.format(expected_count=num_to_upsert+num_to_upsert, dep=previous_node_name, user=user))



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
hoodie.datasource.hive_sync.jdbcurl=jdbc:hive2://hadoopzk04-dca1:2181,hadoopzk05-dca1:2181,hadoopzk06-dca1:2181/;serviceDiscoveryMode=zookeeper;zooKeeperNamespace=hiveserver2_prod_binary_2_X
hoodie.datasource.hive_sync.database=rawdata_test
hoodie.datasource.hive_sync.table={user}_hudi_test_suite_table
hoodie.datasource.hive_sync.partition_extractor_class=org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor
hoodie.datasource.hive_sync.assume_date_partitioning=true
hoodie.datasource.hive_sync.use_pre_apache_input_format=true

hoodie.metrics.on=true
hoodie.metrics.graphite.metric.prefix=stats.dca1.gauges.hoodie.dev.{user}
hoodie.embed.timeline.server=true

"""

with open("test.properties", "w") as properties_file:
    properties_file.write(PROPERTIES.format(user=user))