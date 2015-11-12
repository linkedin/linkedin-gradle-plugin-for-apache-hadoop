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
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.bundling.Zip;

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
    // Enable users to skip the plugin
    if (project.hasProperty("disableHadoopZipPlugin")) {
      println("HadoopZipPlugin disabled");
      return;
    }

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

    // Create task in afterEvaluate so that the 'main' in hadoopZip extension is resolved first,
    // otherwise the getContents() method of HadoopZipExtension returns null.
    project.afterEvaluate {
      for (String cluster : hadoopZipExtension.getZipMap().keySet()) {
        createZipTask(project, cluster);
      }
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

  /**
   * Method to create the Hadoop Zip task for the given named zip.
   *
   * @param project The Gradle project
   * @param zipName The zip name
   * @return The zip task
   */
  Task createZipTask(Project project, String zipName) {
    return project.tasks.create(name: "${zipName}HadoopZip", type: Zip) { task ->
      classifier = zipName.equals("main") ? "" : zipName;
      description = "Creates a Hadoop zip archive for ${zipName}";
      group = "Hadoop Plugin";

      // This task is a dependency of buildHadoopZips and depends on the startHadoopZips
      project.tasks["buildHadoopZips"].dependsOn task;
      dependsOn "startHadoopZips";

      // Include files specified by the user through hadoopZip extension. If there is a base
      // CopySpec, add it as a child of the cluster specific CopySpec.
      if (hadoopZipExtension.getBaseCopySpec() != null) {
        task.with(hadoopZipExtension.getZipCopySpec(zipName).with(hadoopZipExtension.getBaseCopySpec()));
      }
      else {
        task.with(hadoopZipExtension.getZipCopySpec(zipName));
      }

      // For Java projects, include the project jar into the libPath directory in the zip by default
      project.getPlugins().withType(JavaPlugin) {
        includeLibs(project, task, project.tasks.getByName("jar"));
      }

      // Include hadoopRuntime dependencies into the libPath directory in the zip
      includeLibs(project, task, hadoopConfiguration);

      // Include any additional paths added to the hadoopZip extension
      for (String path : hadoopZipExtension.additionalPaths) {
        from(path) { };
      }

      // Add the task to project artifacts
      if (project.configurations.findByName("archives") != null) {
        project.artifacts.add("archives", task);
      }

      // When everything is done, print out a message
      doLast {
        project.logger.lifecycle("Prepared Hadoop zip archive at: ${archivePath}");
      }
    }
  }

  /**
   * Includes libs in the directory specified by azkaban.ZipLibDir property if present.
   *
   * @param project
   * @param spec
   * @param target
   */
  void includeLibs(Project project, CopySpec spec, Object target) {
    spec.from(target) {
      into hadoopZipExtension.libPath;
    }
  }
}
