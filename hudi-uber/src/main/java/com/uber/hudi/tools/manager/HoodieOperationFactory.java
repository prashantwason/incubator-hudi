package com.uber.hudi.tools.manager;

/**
 * This class returns proper hoodie management operation class based on provided command and arguments
 */
public class HoodieOperationFactory {
  static HoodieOperation create(String commandLabel) throws IllegalArgumentException {
    HoodieOperation.Command command = HoodieOperation.Command.valueOfLabel(commandLabel);
    switch (command) {
      case REPLICATION:
        return new HoodieOperationReplication();
      case REPLICATION_INTEG_TESTS:
        return new HoodieOperationReplicationIntegTests();
      default:
        throw new IllegalArgumentException("Invalid command:" + commandLabel);
    }
  }
}
