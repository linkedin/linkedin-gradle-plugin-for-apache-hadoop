/*
 * Copyright 2014 LinkedIn Corp.
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
package com.linkedin.gradle.azkaban;

import com.linkedin.gradle.hadoopdsl.HadoopDslExtension;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * AzkabanPlugin implements features for Azkaban, including building the Hadoop DSL for Azkaban.
 */
class AzkabanPlugin implements Plugin<Project> {

  @Override
  void apply(Project project) {
    // Add the Gradle task that builds the Hadoop DSL for Azkaban. Plugin users should have their
    // build tasks depend on this task.
    project.tasks.create("buildAzkabanFlows") {
      dependsOn "checkHadoopDsl"
      description = "Builds the Hadoop DSL for Azkaban. Have your build task depend on this task.";
      group = "Hadoop Plugin";

      doLast {
        HadoopDslExtension extension = project.extensions.hadoop;
        extension.build();
      }
    }
  }
}