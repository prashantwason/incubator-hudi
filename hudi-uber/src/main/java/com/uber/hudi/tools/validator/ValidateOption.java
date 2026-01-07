package com.uber.hudi.tools.validator;

/**
 * This enum represents validate option
 */
public enum ValidateOption {
  NOOP("noop"),
  // Validate Metadata Table file listings
  METADATA("Metadata"),
  // Validate Metadata Table record index
  RECORD_INDEX("RecordIndex"),
  // Validate requested/inflight instants are within accepted duration
  INFLIGHT_TIMELINE("Inflight"),
  // Validate HUDI configs and Config Store
  CONFIG("Config"),
  // Validate partitions read by query engines
  PARTITION("Partition"),
  // Validate commit timeline does not have archival lag
  COMMIT_TIMELINE("Commit"),
  // Validate last compaction time. Requires Metadata validation to be enabled
  COMPACTION("Compaction"),
  // Validate staging datasets - count, duplication
  STAGING("Staging"),
  // Validate detect duplicates from record index
  DETECT_RECORD_INDEX_DUPLICATE("DetectDuplicatesRI"),
  // Validate replication enabled flag
  REPLICATION("Replication"),
  // Validate number of commit, delta_commit, compaction, log_compaction, replace_commit.
  WRITE_TIMELINE("WriteTimeline"),
  // Validate file size in record index hfiles and data parquet files
  LARGE_SIZED_FILES("LargeSizedFiles"),
  // Validate hive-sync replication
  REPLICATION_CONSISTENCY("ReplicationConsistency"),
  // Validate for recent clean
  CLEAN("Clean");

  public final String label;

  private ValidateOption(String label) {
    this.label = label;
  }

}