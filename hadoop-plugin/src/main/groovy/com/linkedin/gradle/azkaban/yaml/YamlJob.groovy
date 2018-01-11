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
  @Override
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
