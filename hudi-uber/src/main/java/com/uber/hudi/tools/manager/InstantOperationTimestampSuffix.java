package com.uber.hudi.tools.manager;

public enum InstantOperationTimestampSuffix {
  // Suffix to use for bootstrapping additional indexes. Should be less than other suffixes.
  INDEX_BOOTSTRAP_TIMESTAMP_SUFFIX("001"),
  // Suffix to use for compaction
  COMPACTION_TIMESTAMP_SUFFIX("002"),
  // Suffix to use for clean
  CLEAN_TIMESTAMP_SUFFIX("003"),
  // Suffix to use for log compaction
  LOG_COMPACTION_TIMESTAMP_SUFFIX("004"),
  // Suffix to use for metadata file operation - add/delete files
  METADATA_FILE_OPERATION_TIMESTAMP_SUFFIX("005");

  public final String label;

  private InstantOperationTimestampSuffix(String label) {
    this.label = label;
  }
}