package com.uber.hudi.tools.manager;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;

/**
 * Stub: real implementation lives in the internal monorepo.
 * TODO: Port the full implementation from 0.14.x.
 */
public class HoodieOperationRestore implements HoodieOperation {

  @Override
  public Result execute(HoodieSparkEngineContext context, String[] args) throws Exception {
    throw new UnsupportedOperationException("HoodieOperationRestore is not yet ported to 1.x");
  }

  public boolean auditPostRestore(HoodieSparkEngineContext context, HoodieTableMetaClient metaClient,
      String basePath, HoodieInstant restoreInstant, int numExecutors) {
    throw new UnsupportedOperationException("HoodieOperationRestore.auditPostRestore is not yet ported to 1.x");
  }
}
