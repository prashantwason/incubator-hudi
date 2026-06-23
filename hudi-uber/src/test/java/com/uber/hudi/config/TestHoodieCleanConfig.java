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

package com.uber.hudi.config;

import java.util.Properties;
import org.apache.hudi.config.HoodieCleanConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestHoodieCleanConfig {

  @Test
  void maxCommitsToClean_acceptsUber014AlternativePropertyName() {
    Properties props = new Properties();
    props.setProperty("hoodie.cleaner.max.commits.clean", "42");

    HoodieCleanConfig cleanConfig = HoodieCleanConfig.newBuilder().fromProperties(props).build();

    assertEquals(42L, cleanConfig.getLong(HoodieCleanConfig.MAX_COMMITS_TO_CLEAN));
  }

  @Test
  void maxCommitsToClean_onePointTwoKeyStillWorks() {
    Properties props = new Properties();
    props.setProperty("hoodie.clean.max.commits.to.clean", "99");

    HoodieCleanConfig cleanConfig = HoodieCleanConfig.newBuilder().fromProperties(props).build();

    assertEquals(99L, cleanConfig.getLong(HoodieCleanConfig.MAX_COMMITS_TO_CLEAN));
  }
}
