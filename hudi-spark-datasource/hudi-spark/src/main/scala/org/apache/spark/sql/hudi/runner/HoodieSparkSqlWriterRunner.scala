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

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.util.HoodieTimer
import org.apache.hudi.config.metrics.{HoodieMetricsConfig, HoodieMetricsM3Config}
import org.apache.hudi.metrics.{Metrics, MetricsReporterType}
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object HoodieSparkSqlWriterRunner {
  private val log = LoggerFactory.getLogger(getClass)
  var sparkSession: SparkSession = _
  var sparkEngineContext: HoodieSparkEngineContext = _
  private val metricsConfig = HoodieMetricsConfig.newBuilder()
    .withReporterType(MetricsReporterType.M3.name())
    .withPath("/tmp/hudi_spark_integ_test")
    .on(true)
    .build()
  // Metrics instance is initialized lazily after SparkSession is available
  var metrics: Metrics = _

  def main(args: Array[String]): Unit = {
    log.info(s"Args: ${args.mkString(",")}")
    val category = args(1)   // Category name (e.g., "hudi", "hive")
    val testName = args(3)   // Specific test name (e.g., "testHudiDFInsertTable")
    val db = args(5)         // Database name

    val jobTimer = HoodieTimer.start()

    // Initialize Spark session and engine context
    val sparkConf = new SparkConf()
    sparkConf.set("spark.hadoop.hive.cbo.enable", "false")
    sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate
    sparkEngineContext = new HoodieSparkEngineContext(new JavaSparkContext(sparkSession.sparkContext))

    // Initialize metrics with HoodieStorage
    val tmpPath = new Path("/tmp")
    val fs = tmpPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
    val storage = new HoodieHadoopStorage(fs)
    metrics = new Metrics(metricsConfig, storage)

    // Run tests
    if (testName.isEmpty) {
      runAllTests(category, db)
    } else {
      runTest(category, testName, db)
    }

    log.info("Tests completed, flushing metrics.")
    metrics.registerGauge("runtime", jobTimer.endTimer())
    metrics.flush()
  }

  def runAllTests(category: String, db: String): Unit = {
    val className = classMapping.get(category)
      .getOrElse(throw new UnsupportedOperationException(s"Unsupported test category: $category"))

    val runtimeUniverse = scala.reflect.runtime.universe
    val runtimeMirror = runtimeUniverse.runtimeMirror(getClass.getClassLoader)
    val classSymbol = runtimeMirror.staticClass(className)

    // Discover all test methods (public, no-arg methods starting with "test")
    val testMethods = classSymbol.toType.members
      .filter(m => m.isMethod && m.isPublic && m.name.toString.startsWith("test"))
      .map(_.asMethod)
      .filter(m => m.paramLists.isEmpty || m.paramLists == List(List()))
      .toList
      .sortBy(_.name.toString)

    log.info(s"Discovered ${testMethods.size} test methods in $category: ${testMethods.map(_.name).mkString(", ")}")

    val failures = scala.collection.mutable.ListBuffer[(String, Exception)]()

    for (method <- testMethods) {
      val methodName = method.name.toString
      val timer = HoodieTimer.start()
      try {
        log.info(s"Running test: $category.$methodName")
        executeTest(className, methodName, sparkSession, db)
        val durationInMs = timer.endTimer()
        log.info(s"Test $category.$methodName PASSED in $durationInMs ms")
        reportStatusMetrics(className, methodName, status = true)
      } catch {
        case e: Exception if isNotImplemented(e) =>
          val durationInMs = timer.endTimer()
          log.warn(s"Test $category.$methodName SKIPPED (not implemented) in $durationInMs ms")
        case e: Exception =>
          val durationInMs = timer.endTimer()
          log.error(s"Test $category.$methodName FAILED in $durationInMs ms", e)
          reportStatusMetrics(className, methodName, status = false)
          failures += ((methodName, e))
      }
    }

    if (failures.nonEmpty) {
      val failedNames = failures.map(_._1).mkString(", ")
      throw new RuntimeException(s"${failures.size}/${testMethods.size} tests failed in $category: $failedNames")
    }
  }

  def runTest(category: String, testName: String, db: String): Unit = {
    val className = classMapping.get(category)
      .getOrElse(throw new UnsupportedOperationException(s"Unsupported test category: $category"))

    val timer = HoodieTimer.start()
    var status = false
    var testException: Option[Exception] = None

    try {
      // Execute test with global session
      executeTest(className, testName, sparkSession, db)

      status = true
    } catch {
      case e: Exception if isNotImplemented(e) =>
        log.warn(s"Test $category.$testName SKIPPED (not implemented)")
        status = true
      case e: Exception =>
        log.error(s"Failed to execute test: $category.$testName", e)
        testException = Some(e)
    } finally {
      val durationInMs = timer.endTimer()
      log.info(s"Test $category.$testName completed with status: $status in $durationInMs ms")
      reportStatusMetrics(className, testName, status)
    }

    testException.foreach { e =>
      throw new RuntimeException(s"Test $category.$testName failed", e)
    }
  }

  private def executeTest(className: String, testName: String, sparkSession: SparkSession, db: String): Unit = {
    val runtimeUniverse = scala.reflect.runtime.universe
    val appClassLoader = getClass.getClassLoader
    val runtimeMirror = runtimeUniverse.runtimeMirror(appClassLoader)

    val classSymbol = runtimeMirror.staticClass(className)
    val classMirror = runtimeMirror.reflectClass(classSymbol)
    val constructorSymbol = classSymbol.primaryConstructor.asMethod
    val constructorMirror = classMirror.reflectConstructor(constructorSymbol)

    val instance = constructorMirror()
    val instanceMirror = runtimeMirror.reflect(instance)

    // Initialize with global Spark session
    val initializeMethodSymbol = classSymbol.toType.member(runtimeUniverse.TermName("initialize")).asMethod
    val initializeMethodMirror = instanceMirror.reflectMethod(initializeMethodSymbol)
    initializeMethodMirror(sparkSession, db)

    // Reset thread context classloader to the app classloader.
    // Hive SQL operations (e.g., DROP TABLE in cleanup/initialize) set the thread's context
    // classloader to UDFClassLoader, which causes ClassCastExceptions when Hudi's
    // engineContext.map() deserializes objects (e.g., DirectoryInfo) using a different classloader.
    Thread.currentThread().setContextClassLoader(appClassLoader)

    // Execute the specific test method
    val testMethodSymbol = classSymbol.toType.member(runtimeUniverse.TermName(testName)).asMethod
    val testMethodMirror = instanceMirror.reflectMethod(testMethodSymbol)
    testMethodMirror()
  }

  // Keep existing class mapping using classOf syntax
  private val classMapping = Map(
    "hudi" -> classOf[RunHudiBasicOperations].getName,
    "hive" -> classOf[RunHiveBasicOperations].getName,
    "hudi-hive" -> classOf[RunHudiHiveJoinSQLs].getName,
    "hudi-crud" -> classOf[RunHudiCRUDOperations].getName,
    "hudi-procedures" -> classOf[RunHudiProcedures].getName,
    "hudi-comparison" -> classOf[RunHudiComparisonTests].getName,
    "hudi-spark-hive" -> classOf[RunHudiSparkHiveApiTests].getName,
    "hudi-support-non-hoodie" -> classOf[RunNonHoodiePartitionsTest].getName,
    "hudi-random-production-sqls" -> classOf[RunRandomProductionSQLsTest].getName,
    "hudi-schema-evolution" -> classOf[RunSchemaEvolutionTest].getName,
    "hudi-error-handling" -> classOf[RunErrorHandlingTests].getName,
    "hudi-table-ddl" -> classOf[RunHudiTableDDLOperations].getName,
    "hudi-legacy-table-compat" -> classOf[RunLegacyTableCompatTests].getName,
    "hudi-index-ddl" -> classOf[RunHudiIndexDDL].getName,
    "hudi-table-version-six" -> classOf[RunTableVersionSixTests].getName
  )

  private def isNotImplemented(e: Exception): Boolean = {
    val cause = if (e.getCause != null) e.getCause else e
    cause.isInstanceOf[UnsupportedOperationException] && cause.getMessage != null && cause.getMessage.contains("Not implemented")
  }

  /**
   * This method is used to publish metrics.
   */
  private def reportStatusMetrics(testClassName: String, testMethodName: String, status: Boolean): Unit = synchronized {
    val simpleClassName = Class.forName(testClassName).getSimpleName
    metrics.registerGauge(s"$simpleClassName.$testMethodName.status", if (status) 1 else 0)
  }
}
