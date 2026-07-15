package com.uber.hudi.tools.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.storage.StorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Stub for the Uber-internal HoodieFileCopyUtils.
 * TODO: Port the real implementation from the internal monorepo.
 */
public class HoodieFileCopyUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieFileCopyUtils.class);

  public static boolean copyFileWithRetry(StorageConfiguration<?> serConf, Path src, Path dest, int maxRetries) {
    Configuration conf = (Configuration) serConf.unwrap();
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        FileSystem srcFs = src.getFileSystem(conf);
        FileSystem destFs = dest.getFileSystem(conf);
        return FileUtil.copy(srcFs, src, destFs, dest, false, conf);
      } catch (IOException e) {
        LOG.warn("Copy attempt {} of {} failed for {} -> {}: {}", attempt, maxRetries, src, dest, e.getMessage());
        if (attempt == maxRetries) {
          LOG.error("All {} copy attempts exhausted for {} -> {}", maxRetries, src, dest, e);
          return false;
        }
      }
    }
    return false;
  }
}
