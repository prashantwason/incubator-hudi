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

package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.SchemaTestUtil;
import org.codehaus.jackson.JsonNode;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * Tests hoodie avro utilities.
 */
public class TestHoodieAvroUtils {

  private static String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}]}";

  @Test
  public void testPropsPresent() {
    Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    boolean piiPresent = false;
    for (Schema.Field field : schema.getFields()) {
      if (HoodieAvroUtils.isMetadataField(field.name())) {
        continue;
      }

      Assert.assertNotNull("field name is null", field.name());
      Map<String, JsonNode> props = field.getJsonProps();
      Assert.assertNotNull("The property is null", props);

      if (field.name().equals("pii_col")) {
        piiPresent = true;
        Assert.assertTrue("sensitivity_level is removed in field 'pii_col'", props.containsKey("column_category"));
      } else {
        Assert.assertEquals("The property shows up but not set", 0, props.size());
      }
    }
    Assert.assertTrue("column pii_col doesn't show up", piiPresent);
  }

  public void testRewriteRecordPerformance() throws IOException {
    Schema origSchema = new Schema.Parser().parse(TestHoodieAvroUtils.class.getResourceAsStream("/simple-large.avsc"));
    Schema writerSchema = HoodieAvroUtils.addMetadataFields(origSchema);

    // Construct a record from the origSchema
    GenericRecord origRecord = new GenericData.Record(origSchema);
    for (int i = 1; i <= 10; ++i) {
      origRecord.put("s" + i, "string_value");
      origRecord.put("i" + i, i);
      origRecord.put("l" + i, (long)i);
      origRecord.put("d" + i, (double)i);
      origRecord.put("b" + i, i%2 == 0);
    }

    int runs = 50*1000*1000;
    long t1 = System.currentTimeMillis();
    for (int i = 0; i < runs; ++i) {
      GenericRecord newRecord = HoodieAvroUtils.rewriteRecord(origRecord, writerSchema);
    }
    long t2 = System.currentTimeMillis();

    System.out.format("Conversion takes total %d msec (%.3f usec / record)%n", t2 - t1, (float)(1000 * (t2 - t1)) / runs);
  }
}
