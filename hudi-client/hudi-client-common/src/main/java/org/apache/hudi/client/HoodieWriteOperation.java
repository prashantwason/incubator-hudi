/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.client;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.table.BulkInsertPartitioner;

/**
 * Represents one of the write operations supported in HUDI.
 */
public class HoodieWriteOperation<I, K> {
  private final WriteOperationType operationType;
  private I records;
  private K deleteKeys;
  private Option<BulkInsertPartitioner<I>> bulkInsertPartitioner;

  private HoodieWriteOperation(WriteOperationType operationType, I records,
      Option<BulkInsertPartitioner<I>> bulkInsertPartitioner) {
    this.operationType = operationType;
    this.records = records;
    this.bulkInsertPartitioner = bulkInsertPartitioner;
  }

  private HoodieWriteOperation(K deleteKeys) {
    this.operationType = WriteOperationType.DELETE;
    this.deleteKeys = deleteKeys;
  }

  public static <I> HoodieWriteOperation bulkInsert(I records,
      Option<BulkInsertPartitioner<I>> bulkInsertPartitioner) {
    return new HoodieWriteOperation<>(WriteOperationType.BULK_INSERT, records, bulkInsertPartitioner);
  }

  public static <I> HoodieWriteOperation bulkInsertPrepped(I records,
      Option<BulkInsertPartitioner<I>> bulkInsertPartitioner) {
    return new HoodieWriteOperation<>(WriteOperationType.BULK_INSERT_PREPPED, records, bulkInsertPartitioner);
  }

  public static <I> HoodieWriteOperation insert(I records) {
    return new HoodieWriteOperation<>(WriteOperationType.INSERT, records, Option.empty());
  }

  public static <I> HoodieWriteOperation insertPrepped(I records) {
    return new HoodieWriteOperation<>(WriteOperationType.INSERT_PREPPED, records, Option.empty());
  }

  public static <I> HoodieWriteOperation insertOverwrite(I records) {
    return new HoodieWriteOperation<>(WriteOperationType.INSERT_OVERWRITE, records, Option.empty());
  }

  public static <I> HoodieWriteOperation upsert(I records) {
    return new HoodieWriteOperation<>(WriteOperationType.UPSERT, records, Option.empty());
  }

  public static <I> HoodieWriteOperation upsertPrepped(I records) {
    return new HoodieWriteOperation<>(WriteOperationType.UPSERT_PREPPED, records, Option.empty());
  }

  public static <K> HoodieWriteOperation delete(K deleteKeys) {
    return new HoodieWriteOperation(deleteKeys);
  }

  /**
   * Return the type of the operation.
   * @return {WriteOperationType}
   */
  public WriteOperationType operationType() {
    return operationType;
  }

  /**
   * Return the records that are part of this operation.
   * @return {JavaRDD}
   */
  public I getRecords() {
    ValidationUtils.checkArgument(operationType != WriteOperationType.DELETE);
    return records;
  }

  /**
   * If this is a DELETE operation, return the keys that are to be deleted.
   * @return {JavaRDD}
   */
  public K getDeleteKeys() {
    ValidationUtils.checkArgument(operationType == WriteOperationType.DELETE);
    return deleteKeys;
  }

  public Option<BulkInsertPartitioner<I>> getBulkInsertPartitioner() {
    ValidationUtils.checkArgument(operationType == WriteOperationType.BULK_INSERT);
    return bulkInsertPartitioner;
  }
}