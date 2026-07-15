package com.uber.hudi.tools.manager;

import java.util.Map;
import java.util.HashMap;

import org.apache.hudi.client.common.HoodieSparkEngineContext;

interface HoodieOperation {
  enum Command {
    REPLICATION("replication"),
    REPLICATION_INTEG_TESTS("replication_integ_tests");

    public final String label;
    private static final Map<String, Command> LABEL_TO_COMMAND = new HashMap<>();

    static {
      for (Command command : values()) {
        LABEL_TO_COMMAND.put(command.label, command);
      }
    }

    private Command(String label) {
      this.label = label;
    }

    public static Command valueOfLabel(String label) {
      return LABEL_TO_COMMAND.get(label);
    }
  }

  Result execute(HoodieSparkEngineContext context, String[] args) throws Exception;
}
