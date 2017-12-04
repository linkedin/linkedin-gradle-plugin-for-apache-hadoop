package com.linkedin.gradle.azkaban.yaml;

import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_FLOW_VERSION;

/**
 * Representation of a project in flow.project file for Azkaban Flow 2.0
 */
class YamlProject {
  /**
   * Constructor for YamlProject
   */
  YamlProject() {
  }

  /**
   * @return String Map detailing exactly what should be printed in Yaml
   */
  Map yamlize() {
    Map result = [:];
    result.put("Azkaban-Flow-Version", AZK_FLOW_VERSION);
    return result;
  }
}
