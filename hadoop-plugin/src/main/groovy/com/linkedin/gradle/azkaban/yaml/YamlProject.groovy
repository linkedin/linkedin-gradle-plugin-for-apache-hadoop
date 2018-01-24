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

import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_FLOW_VERSION;

/**
 * Representation of a project in flow.project file for Azkaban Flow 2.0
 */
class YamlProject implements YamlObject {
  String name;

  /**
   * Constructor for YamlProject
   */
  YamlProject(String name) {
    this.name = name;
  }

  /**
   * @return Map detailing exactly what should be printed in Yaml
   */
  @Override
  Map yamlize() {
    Map result = [:];
    result.put("azkaban-flow-version", AZK_FLOW_VERSION);
    return result;
  }
}
