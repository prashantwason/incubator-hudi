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

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable


object HoodieSparkSqlWriterRunner {
  private val log = LoggerFactory.getLogger(getClass)
  var sparkSession: SparkSession = _
  var sparkEngineContext: HoodieSparkEngineContext = _

  def main(args: Array[String]): Unit = {
    log.info(s"Args: ${args.mkString(",")}")
    val argMap = parseArgs(args)

    // --list-steps: print the step manifest for step-based test cases and exit.
    // With --test-name it lists that case's steps; without, all step-based cases.
    if (argMap.contains("list-steps")) {
      val category = requireArg(argMap, "test-category")
      val className = classMapping.getOrElse(category,
        throw new UnsupportedOperationException(s"Unsupported test category: $category"))
      val instance = newInstance(className)
      argMap.get("test-name").filter(_.nonEmpty) match {
        case Some(testCase) =>
          stepsOf(instance, className, testCase).zipWithIndex.foreach { case (s, i) => println(s"$i\t${s.name}") }
        case None =>
          testCaseMethods(className).filter(m => returnsSteps(className, m)).foreach { m =>
            stepsOf(instance, className, m).zipWithIndex.foreach { case (s, i) => println(s"$m\t$i\t${s.name}") }
          }
      }
    } else {
      runWithSpark(argMap)
    }
  }

  private def runWithSpark(argMap: Map[String, String]): Unit = {
    val jobTimer = HoodieTimer.start()

    // Initialize Spark session and engine context
    val sparkConf = new SparkConf()
    sparkConf.set("spark.hadoop.hive.cbo.enable", "false")
    sparkSession = SparkSession.builder.config(sparkConf).enableHiveSupport.getOrCreate
    sparkEngineContext = new HoodieSparkEngineContext(new JavaSparkContext(sparkSession.sparkContext))

    // Dispatch a category -> test class. A test case is a public no-arg method:
    //   - if it returns Seq[Step]: run one step (--step N) or the whole sequence in
    //     this job (no --step); the runtime is set by the hudi-spark-bundle on --jars.
    //   - otherwise: invoke it as-is (executes inline).
    // --test-name selects one case; without it, all test* cases run in this job.
    val category = requireArg(argMap, "test-category")
    val db = requireArg(argMap, "db")
    val testName = argMap.getOrElse("test-name", "")
    val stepOpt = argMap.get("step").filter(_.nonEmpty).map(_.toInt)

    val className = classMapping.getOrElse(category,
      throw new UnsupportedOperationException(s"Unsupported test category: $category"))
    val instance = newInstance(className)
    instance.asInstanceOf[RunOperationsBase].initialize(sparkSession, db)
    Thread.currentThread().setContextClassLoader(getClass.getClassLoader)

    if (testName.nonEmpty) {
      try runCase(instance, className, testName, stepOpt)
      catch {
        case e: Exception if isNotImplemented(e) =>
          log.warn(s"Test $category.$testName SKIPPED (not implemented)")
      }
    } else {
      val methods = testCaseMethods(className)
      val failed = methods.filterNot { m =>
        instance.asInstanceOf[RunOperationsBase].cleanup()
        try { runCase(instance, className, m, None); true }
        catch {
          case e: Exception if isNotImplemented(e) =>
            log.warn(s"Test $category.$m SKIPPED (not implemented)"); true
          case e: Exception =>
            log.error(s"Test $category.$m FAILED", e); false
        }
      }
      if (failed.nonEmpty) {
        throw new RuntimeException(s"${failed.size}/${methods.size} cases failed in $category: ${failed.mkString(", ")}")
      }
    }

    log.info(s"Tests completed in ${jobTimer.endTimer()} ms.")
  }

  /** Run one test case: step-based cases run their steps, others execute inline. */
  private def runCase(instance: AnyRef, className: String, methodName: String, stepOpt: Option[Int]): Unit = {
    if (returnsSteps(className, methodName)) {
      val steps = stepsOf(instance, className, methodName)
      stepOpt match {
        case Some(idx) => runOneStep(steps, className, methodName, idx)
        case None      => runAllSteps(steps, className, methodName)
      }
    } else {
      runReporting(className, methodName) { invokeCase(instance, className, methodName) }
    }
  }

  /**
   * Flag parser supporting `--key value` and boolean `--flag`. A flag whose
   * next token is another `--flag` (or end of args) is boolean. Empty values
   * (e.g. `--step ""` from an unset Drogon env var) are kept as empty strings
   * and filtered by callers, so the single Drogon arg list can carry the new
   * flags with empty defaults without changing legacy behavior.
   */
  private[runner] def parseArgs(args: Array[String]): Map[String, String] = {
    val m = mutable.Map[String, String]()
    var i = 0
    while (i < args.length) {
      val a = args(i)
      if (a.startsWith("--")) {
        val key = a.substring(2)
        if (i + 1 < args.length && !args(i + 1).startsWith("--")) {
          m(key) = args(i + 1); i += 2
        } else {
          m(key) = "true"; i += 1
        }
      } else {
        i += 1
      }
    }
    m.toMap
  }

  private def requireArg(m: Map[String, String], key: String): String =
    m.get(key).filter(_.nonEmpty)
      .getOrElse(throw new IllegalArgumentException(s"Missing required arg --$key"))

  private def newInstance(className: String): AnyRef =
    Class.forName(className).getDeclaredConstructor().newInstance().asInstanceOf[AnyRef]

  /** True if the test-case method's declared return type is Seq[Step]. */
  private def returnsSteps(className: String, methodName: String): Boolean = {
    val ru = scala.reflect.runtime.universe
    val classSymbol = ru.runtimeMirror(getClass.getClassLoader).staticClass(className)
    classSymbol.toType.member(ru.TermName(methodName)).asMethod.returnType <:< ru.typeOf[Seq[Step]]
  }

  /** Reflectively invoke a no-arg test-case method and return its Seq[Step]. */
  private def stepsOf(instance: AnyRef, className: String, methodName: String): Seq[Step] =
    invokeCase(instance, className, methodName).asInstanceOf[Seq[Step]]

  /** Reflectively invoke a no-arg test-case method; returns its result (Unit for inline cases). */
  private def invokeCase(instance: AnyRef, className: String, methodName: String): Any = {
    val ru = scala.reflect.runtime.universe
    val mirror = ru.runtimeMirror(getClass.getClassLoader)
    val classSymbol = mirror.staticClass(className)
    val methodSymbol = classSymbol.toType.member(ru.TermName(methodName)).asMethod
    mirror.reflect(instance).reflectMethod(methodSymbol)()
  }

  /** Public no-arg methods named test* — the test cases of a class. */
  private def testCaseMethods(className: String): List[String] = {
    val ru = scala.reflect.runtime.universe
    val classSymbol = ru.runtimeMirror(getClass.getClassLoader).staticClass(className)
    classSymbol.toType.members
      .filter(m => m.isMethod && m.isPublic && m.name.toString.startsWith("test"))
      .map(_.asMethod)
      .filter(m => m.paramLists.isEmpty || m.paramLists == List(List()))
      .toList
      .map(_.name.toString)
      .sorted
  }

  /** Run one labeled unit of work, reporting a pass/fail gauge and re-throwing
    * on failure so the Spark job (and thus the Drogon step) fails. */
  private def runReporting(className: String, label: String)(body: => Unit): Unit = {
    val timer = HoodieTimer.start()
    var status = false
    var testException: Option[Exception] = None
    try {
      body
      status = true
    } catch {
      case e: Exception =>
        log.error(s"$label failed", e)
        testException = Some(e)
    } finally {
      log.info(s"$label completed with status: $status in ${timer.endTimer()} ms")
      reportStatusMetrics(className, label, status)
    }
    testException.foreach(e => throw new RuntimeException(s"$label failed", e))
  }

  /** Run a single step of a test case by index. */
  private def runOneStep(steps: Seq[Step], className: String, testCase: String, idx: Int): Unit = {
    require(idx >= 0 && idx < steps.size,
      s"step index $idx out of range [0, ${steps.size}) for $testCase")
    val step = steps(idx)
    runReporting(className, s"$testCase.step_$idx") { step.body(sparkSession) }
  }

  /** Run all steps of one test case in order (single Spark app). Throws if any
    * step failed, after attempting every step and reporting per-step metrics. */
  private def runAllSteps(steps: Seq[Step], className: String, testCase: String): Unit = {
    log.info(s"Running $testCase (${steps.size} steps): ${steps.map(_.name).mkString(", ")}")
    val failures = mutable.ListBuffer[String]()
    steps.indices.foreach { idx =>
      val name = steps(idx).name
      val timer = HoodieTimer.start()
      var status = false
      try {
        steps(idx).body(sparkSession)
        status = true
      } catch {
        case e: Exception =>
          log.error(s"Step $idx ($name) of $testCase failed", e)
          failures += name
      } finally {
        log.info(s"Step $idx ($name) of $testCase completed with status: $status in ${timer.endTimer()} ms")
        reportStatusMetrics(className, s"$testCase.step_$idx", status)
      }
    }
    if (failures.nonEmpty) {
      throw new RuntimeException(
        s"${failures.size}/${steps.size} steps failed in $testCase: ${failures.mkString(", ")}")
    }
  }

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

  private def reportStatusMetrics(testClassName: String, testMethodName: String, status: Boolean): Unit = synchronized {
    val simpleClassName = Class.forName(testClassName).getSimpleName
    log.info(s"STATUS $simpleClassName.$testMethodName = ${if (status) "PASS" else "FAIL"}")
  }
}
