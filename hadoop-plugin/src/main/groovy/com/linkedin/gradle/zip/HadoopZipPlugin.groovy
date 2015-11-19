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
package com.linkedin.gradle.zip;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;

/**
 * HadoopZipPlugin implements features that build Hadoop zip artifacts.
 */
class HadoopZipPlugin implements Plugin<Project> {
  Configuration hadoopConfiguration;
  HadoopZipExtension hadoopZipExtension;

  /**
   * Applies the HadoopZipPlugin.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    // Create the HadoopZipExtension for the project.
    hadoopConfiguration = project.configurations["hadoopRuntime"];
    hadoopZipExtension = createZipExtension(project);

    // Create top-level tasks to build the Hadoop zips. The individual Hadoop zip tasks will
    // depend on startHadoopZips and be dependencies of buildHadoopZips. This enables the user's
    // build task to depend on buildHadoopZips and then have startHadoopZips depend on other tasks.
    project.tasks.create("startHadoopZips") {
      description = "Container task on which all the Hadoop zip tasks depend";
      group = "Hadoop Plugin";
    }

    project.tasks.create("buildHadoopZips") {
      dependsOn "startHadoopZips"
      description = "Builds all of the Hadoop zip archives. Tasks that depend on Hadoop zips should depend on this task";
      group = "Hadoop Plugin";
    }
  }

  /**
   * Helper method to create the Hadoop zip extension. Having this method allows for the unit tests
   * to override it.
   *
   * @param project The Gradle project
   * @return The Hadoop zip extension
   */
  HadoopZipExtension createZipExtension(Project project) {
    HadoopZipExtension extension = new HadoopZipExtension(project);
    project.extensions.add("hadoopZip", extension);
    return extension;
  }
}
