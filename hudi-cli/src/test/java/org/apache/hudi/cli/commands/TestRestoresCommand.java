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

package org.apache.hudi.cli.commands;

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.ShellEvaluationResultUtil;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.shell.Shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
@SpringBootTest(properties = {"spring.shell.interactive.enabled=false", "spring.shell.command.script.enabled=false"})
public class TestRestoresCommand extends CLIFunctionalTestHarness {

  @Autowired
  private Shell shell;

  @BeforeEach
  public void init() throws Exception {
    String tableName = tableName();
    String tablePath = tablePath(tableName);
    new TableCommand().createTable(
            tablePath, tableName, HoodieTableType.MERGE_ON_READ.name(),
            "", HoodieTableVersion.SIX.versionCode(), "org.apache.hudi.common.model.HoodieAvroPayload");
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(HoodieCLI.getTableMetaClient());
    //Create some commits files and base files
    Map<String, String> partitionAndFileId = new HashMap<String, String>() {
      {
        put(DEFAULT_FIRST_PARTITION_PATH, "file-1");
        put(DEFAULT_SECOND_PARTITION_PATH, "file-2");
        put(DEFAULT_THIRD_PARTITION_PATH, "file-3");
      }
    };

    HoodieTestTable hoodieTestTable = HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(DEFAULT_PARTITION_PATHS)
        .addCommit("100")
        .withBaseFilesInPartitions(partitionAndFileId).getLeft()
        .addCommit("101");

    hoodieTestTable.addCommit("102").withBaseFilesInPartitions(partitionAndFileId);
    HoodieSavepointMetadata savepointMetadata2 = hoodieTestTable.doSavepoint("102");
    hoodieTestTable.addSavepoint("102", savepointMetadata2);

    hoodieTestTable.addCommit("103").withBaseFilesInPartitions(partitionAndFileId);

    // Create restore instants properly using the active timeline
    // First restore: restoring to savepoint "102", which rolls back "103"
    createRestoreInstant(metaClient, "10000001", "103");

    hoodieTestTable.addCommit("105").withBaseFilesInPartitions(partitionAndFileId);
    HoodieSavepointMetadata savepointMetadata = hoodieTestTable.doSavepoint("105");
    hoodieTestTable.addSavepoint("105", savepointMetadata);

    hoodieTestTable.addCommit("106").withBaseFilesInPartitions(partitionAndFileId);

    // Second restore: restoring to savepoint "105", which rolls back "106"
    createRestoreInstant(metaClient, "10000002", "106");
  }

  /**
   * Creates a restore instant using the active timeline.
   */
  private void createRestoreInstant(HoodieTableMetaClient metaClient, String restoreTime, String... instantsToRollback) throws IOException {
    HoodieRestoreMetadata restoreMetadata = createRestoreMetadata(restoreTime, instantsToRollback);

    // Create restore instant using the active timeline
    HoodieInstant restoreInstant = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.RESTORE_ACTION, restoreTime);
    metaClient.getActiveTimeline().createNewInstant(restoreInstant);
    metaClient.getActiveTimeline().saveAsComplete(restoreInstant, Option.of(restoreMetadata));
  }

  /**
   * Creates a HoodieRestoreMetadata for testing purposes.
   */
  private HoodieRestoreMetadata createRestoreMetadata(String restoreTime, String... instantsToRollback) {
    List<String> instants = new ArrayList<>();
    Collections.addAll(instants, instantsToRollback);

    List<HoodieInstantInfo> instantInfoList = new ArrayList<>();
    Map<String, List<HoodieRollbackMetadata>> instantToRollbackMetadata = new HashMap<>();

    for (String instant : instantsToRollback) {
      instantInfoList.add(new HoodieInstantInfo(instant, HoodieTimeline.DELTA_COMMIT_ACTION));
      // Create empty rollback metadata for the instant
      HoodieRollbackMetadata rollbackMetadata = new HoodieRollbackMetadata();
      rollbackMetadata.setStartRollbackTime(restoreTime);
      rollbackMetadata.setTimeTakenInMillis(100L);
      rollbackMetadata.setTotalFilesDeleted(0);
      rollbackMetadata.setCommitsRollback(Collections.singletonList(instant));
      rollbackMetadata.setPartitionMetadata(new HashMap<>());
      rollbackMetadata.setInstantsRollback(Collections.singletonList(new HoodieInstantInfo(instant, HoodieTimeline.DELTA_COMMIT_ACTION)));
      instantToRollbackMetadata.put(instant, Collections.singletonList(rollbackMetadata));
    }

    HoodieRestoreMetadata restoreMetadata = new HoodieRestoreMetadata();
    restoreMetadata.setStartRestoreTime(restoreTime);
    restoreMetadata.setTimeTakenInMillis(1000L);
    restoreMetadata.setInstantsToRollback(instants);
    restoreMetadata.setHoodieRestoreMetadata(instantToRollbackMetadata);
    restoreMetadata.setVersion(2);
    restoreMetadata.setRestoreInstantInfo(instantInfoList);

    return restoreMetadata;
  }

  @Test
  public void testShowRestores() {
    Object result = shell.evaluate(() -> "show restores");
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // get restored instants
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    Stream<HoodieInstant> restores = activeTimeline.getRestoreTimeline().filterCompletedInstants().getInstantsAsStream();

    List<Comparable[]> rows = new ArrayList<>();
    restores.sorted().forEach(instant -> {
      try {
        HoodieRestoreMetadata metadata = activeTimeline.readRestoreMetadata(instant);
        metadata.getInstantsToRollback().forEach(c -> {
          Comparable[] row = new Comparable[4];
          row[0] = metadata.getStartRestoreTime();
          row[1] = c;
          row[2] = metadata.getTimeTakenInMillis();
          row[3] = HoodieInstant.State.COMPLETED.toString();
          rows.add(row);
        });
      } catch (IOException e) {
        e.printStackTrace();
      }
    });

    TableHeader header = new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_STATE);
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false,
            -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

  @Test
  public void testShowRestore() throws IOException {
    // get instant
    HoodieActiveTimeline activeTimeline = HoodieCLI.getTableMetaClient().getActiveTimeline();
    Stream<HoodieInstant> restores = activeTimeline.getRestoreTimeline().filterCompletedInstants().getInstantsAsStream();
    HoodieInstant instant = restores.findFirst().orElse(null);
    assertNotNull(instant, "The instant can not be null.");

    Object result = shell.evaluate(() -> "show restore --instant " + instant.requestedTime());
    assertTrue(ShellEvaluationResultUtil.isSuccess(result));

    // get metadata of instant
    HoodieRestoreMetadata instantMetadata = activeTimeline.readRestoreMetadata(instant);

    // generate expected result
    TableHeader header = new TableHeader()
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_INSTANT)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_TIME_TOKEN_MILLIS)
            .addTableHeaderField(HoodieTableHeaderFields.HEADER_RESTORE_STATE);

    List<Comparable[]> rows = new ArrayList<>();
    instantMetadata.getInstantsToRollback().forEach((String rolledbackInstant) -> {
      Comparable[] row = new Comparable[4];
      row[0] = instantMetadata.getStartRestoreTime();
      row[1] = rolledbackInstant;
      row[2] = instantMetadata.getTimeTakenInMillis();
      row[3] = HoodieInstant.State.COMPLETED.toString();
      rows.add(row);
    });
    String expected = HoodiePrintHelper.print(header, new HashMap<>(), "", false, -1,
            false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(result.toString());
    assertEquals(expected, got);
  }

}
