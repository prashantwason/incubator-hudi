-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE EXTERNAL TABLE IF NOT EXISTS ${DATABASE}.hudi_trips_partitioned_empty_table (
    `_hoodie_commit_time`    string,
    `_hoodie_commit_seqno`   string,
    `_hoodie_record_key`     string,
    `_hoodie_partition_path` string,
    `_hoodie_file_name`      string,
    `uuid`                   string,
    `trip_uuid`              string
)
    LOCATION "/user/hudi/integration_tests/${DATABASE}/hudi_trips_partitioned_empty_table"
    ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT 'com.uber.hoodie.hadoop.HoodieInputFormat'
        OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    PARTITIONED BY (
        datestr string
    )
    TBLPROPERTIES (
        "parquet.compression" = "ZSTD"
    );

-- CREATE EXTERNAL TABLE IF NOT EXISTS ${DATABASE}.hudi_trips_nonpartitioned_empty_table (
--     `_hoodie_commit_time`    string,
--     `_hoodie_commit_seqno`   string,
--     `_hoodie_record_key`     string,
--     `_hoodie_partition_path` string,
--     `_hoodie_file_name`      string,
--     `uuid`                   string,
--     `trip_uuid`              string
-- )
--     LOCATION "/user/hudi/integration_tests/${DATABASE}/hudi_trips_nonpartitioned_empty_table"
--     ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
--     STORED AS INPUTFORMAT 'com.uber.hoodie.hadoop.HoodieInputFormat'
--         OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
--     TBLPROPERTIES (
--         "parquet.compression" = "ZSTD"
--     );
