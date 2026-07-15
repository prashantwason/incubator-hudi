package com.uber.hudi.tools.manager;

import org.apache.spark.SparkEnv;

import java.io.Serializable;

public class Result implements Serializable {
  public String message;
  public StatusCode statusCode;
  public String executorId;
  public long operationTimeMs;

  public Result(StatusCode statusCode, String message) {
    this(statusCode, message, 0);
  }

  public Result(StatusCode statusCode, String message, long operationTimeMs) {
    this.statusCode = statusCode;
    this.message = message;
    this.executorId = SparkEnv.get() == null ? "" : SparkEnv.get().executorId();
    this.operationTimeMs = operationTimeMs;
  }

  public String toString() {
    long seconds = operationTimeMs / 1000;
    long hours = seconds / 3600;
    seconds %= 3600;
    long mins = seconds / 60;
    seconds %= 60;
    return String.format("ExecutorID[%s] Status[%s] Message[%s] Time[%s]",
        executorId, statusCode.name(), message,
        String.format("%02d:%02d:%02d", hours, mins, seconds));
  }
}
