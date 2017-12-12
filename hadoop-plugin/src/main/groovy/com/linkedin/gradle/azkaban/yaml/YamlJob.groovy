package com.linkedin.gradle.azkaban.yaml;

import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.job.Job;

/**
 * Representation of a job in .flow file for Azkaban Flow 2.0
 */
class YamlJob implements YamlObject {
  String name;
  String type;
  List<String> dependsOn;
  Map<String, String> config;

  /**
   * Construct YamlJob from Job and the Job's parent scope (the workflow's scope)
   *
   * @param job The job to be converted into Yaml
   * @param parentScope The parent scope of the job
   */
  YamlJob(Job job, NamedScope parentScope) {
    name = job.name;
    type = job.jobProperties["type"];
    dependsOn = job.dependencyNames.toList();
    config = job.buildProperties(parentScope);
    // Remove type and dependencies from config because they're represented elsewhere
    config.remove("type");
    config.remove("dependencies");
  }

  /**
   * @return Map detailing exactly what should be printed in Yaml
   * will not include name, type, dependsOn, or config if it is false (i.e. dependsOn not defined)
   */
  Map yamlize() {
    Map result = [:];
    def addToMapIfNotNull = { val, valName ->
      if (val) {
        result.put(valName, val);
      }
    };
    addToMapIfNotNull(name, "name");
    addToMapIfNotNull(type, "type");
    addToMapIfNotNull(dependsOn, "dependsOn");
    addToMapIfNotNull(config, "config");
    return result;
  }
}
