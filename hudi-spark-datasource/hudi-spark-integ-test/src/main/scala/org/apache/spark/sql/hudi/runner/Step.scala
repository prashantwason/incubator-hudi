/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.runner

import org.apache.spark.sql.SparkSession

/**
 * A single unit of a cross-version integration test.
 *
 * A step is just a named closure over a [[SparkSession]] — the same Scala you
 * would write in a normal integ test, carved into an independently runnable
 * chunk. A test case is a public no-arg method returning `Seq[Step]`; the runner
 * runs one step at a time.
 * There is deliberately no "operation" vocabulary and no Write/Operate typing:
 * which Hudi version a step runs on is decided *externally* by the orchestrator
 * (see scripts/run_compat_test.py), not by the step itself.
 *
 * Every step runs in its own Spark application (a separate Drogon submission,
 * a fresh JVM with a single Hudi JAR on the classpath). Nothing in memory
 * survives between steps — state is carried only by the Hudi table on
 * HDFS and its HMS entries. A step body must therefore reconstruct anything
 * it needs (DataFrames, metaclients, options) from scratch.
 *
 * @param name stable, human-readable label. The orchestrator references steps
 *             by name (not index), and the runner validates the name at
 *             dispatch, so this string is part of the contract: renaming a
 *             step is a breaking change to the yaml/json config.
 */
case class Step(name: String)(val body: SparkSession => Unit)


