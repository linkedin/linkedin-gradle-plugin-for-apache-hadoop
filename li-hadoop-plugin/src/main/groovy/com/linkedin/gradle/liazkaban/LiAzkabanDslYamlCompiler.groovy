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
package com.linkedin.gradle.liazkaban

import com.linkedin.gradle.azkaban.AzkabanDslYamlCompiler;
import com.linkedin.gradle.lihadoopdsl.lijob.LiPigBangBangJob;
import org.gradle.api.Project

import static com.linkedin.gradle.liazkaban.LiAzkabanCompilerUtils.addBangBangProperties
import static com.linkedin.gradle.liazkaban.LiAzkabanCompilerUtils.writeGradleForBangBangJob;

/**
 * Simple class that wraps the YamlCompiler to specify LiYamlWorkflows instead of YamlWorkflows to
 * handle LI specific behavior.
 */
class LiAzkabanDslYamlCompiler extends AzkabanDslYamlCompiler {

  /**
   * Constructor for the YamlCompiler.
   *
   * @param project The Gradle project
   */
  LiAzkabanDslYamlCompiler(Project project) {
    super(project);
  }

  /**
   * Handle LiPigBangBangJob as a special case to introduce LI specific functionality.
   * This includes generate the required BangBang gradle file.
   *
   * @param job LiPigBangBangJob whose config will be modified
   * @return Filtered string map of properties to be output in Yaml
   */
  Map<String,String> yamlizeJob(LiPigBangBangJob job) {
    Map yamlizedJob = [:];

    // Add job name
    yamlizedJob["name"] = job.name;
    // Add job configs if there are any
    Map<String, String> filteredConfig = [:];
    Map<String, String> config = job.buildProperties(this.parentScope);
    // Add job type after test to pick up LiBangBangJob type switch from pig -> hadoopShell
    yamlizedJob["type"] = config["type"];
    // Add job dependencies if there are any
    if (!job.dependencyNames.isEmpty()) {
      yamlizedJob["dependsOn"] = job.dependencyNames.toList();
    }
    // Remove type and dependencies from config because they're represented elsewhere
    config.remove("type");
    config.remove("dependencies");
    writeGradleForBangBangJob(job, this.project, this.parentScope, this.parentDirectory);
    List<String> filteredKeys = addBangBangProperties(config);
    filteredKeys.each { key ->
      filteredConfig[key] = config[key];
    }
    if (!filteredConfig.isEmpty()) {
      yamlizedJob["config"] = filteredConfig;
    }

    return yamlizedJob;
  }

}
