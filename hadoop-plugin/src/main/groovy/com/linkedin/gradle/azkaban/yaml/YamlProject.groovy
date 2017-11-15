package com.linkedin.gradle.azkaban.yaml;

/**
 * Representation of a project in flow.project file for Azkaban Flow 2.0
 */
class YamlProject {
  Double azkabanFlowVersion;

  /**
   * Constructor for YamlProject
   */
  YamlProject() {
    azkabanFlowVersion = 2.0;
  }

  /**
   * @return String Map detailing exactly what should be printed in Yaml
   */
  Map yamlize() {
    Map result = [:];
    result.put("Azkaban-Flow-Version", azkabanFlowVersion);
    return result;
  }
}
