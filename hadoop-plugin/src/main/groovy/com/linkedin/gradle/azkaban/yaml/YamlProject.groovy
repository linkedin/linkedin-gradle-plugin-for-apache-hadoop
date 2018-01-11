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
  Map yamlize() {
    Map result = [:];
    result.put("azkaban-flow-version", AZK_FLOW_VERSION);
    return result;
  }
}
