/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.gradle.hadoop;

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the HadoopPlugin class.
 */
class HadoopPluginTest {
  HadoopPlugin plugin;
  Project project;

  @Before
  public void setup() {
    plugin = new HadoopPlugin();
    project = ProjectBuilder.builder().build();
  }

  /**
   * Simple unit test to make sure applying the plugin succeeds and that the hadoopRuntime
   * configuration was created.
   */
  @Test
  public void testApplyHadoopPlugin() {
    plugin.apply(project);

    // Check that the hadoopRuntime configuration was created successfully.
    Assert.assertNotNull(project.configurations["hadoopRuntime"]);
  }
}
