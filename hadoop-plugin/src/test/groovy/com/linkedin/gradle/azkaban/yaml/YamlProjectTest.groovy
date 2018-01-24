/*
 * Copyright 2017 LinkedIn Corp.
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
package com.linkedin.gradle.azkaban.yaml;

import org.junit.Test;

import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_FLOW_VERSION;
import static org.junit.Assert.assertEquals;

class YamlProjectTest {
  @Test
  public void TestYamlProject() {
    YamlProject yamlProject = new YamlProject("testProject");
    Map yamlizedProject = yamlProject.yamlize();

    assertEquals("testProject", yamlProject.name);
    // The third argument - 0 - refers to the possible difference between two doubles.
    // There should be no difference between AZK_FLOW_VERSION and the yamlProject flow version.
    assertEquals(AZK_FLOW_VERSION, (double) yamlizedProject["azkaban-flow-version"], 0);
  }
}
