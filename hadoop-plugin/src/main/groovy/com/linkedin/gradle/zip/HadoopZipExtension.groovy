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

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.CopySpec;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.bundling.Zip;

/**
 * A hadoopZip makes it convenient for the user to make specific choices about how their content
 * goes inside the zip. In the hadoopZip, the user can specify files and folders which should go
 * inside which folder in the zip.
 * <p>
 * The hadoopZip can be specified with:
 * <pre>
 *    hadoopZip {
 *      libPath = "."
 *      main {
 *        from ("src/") {
 *          into "src"
 *        }
 *      }
 *    }
 * </pre>
 */
class HadoopZipExtension {
  Project project;
  List<String> additionalPaths;
  CopySpec baseCopySpec;
  String libPath;
  Map<String, CopySpec> zipMap;

  /**
   * Constructor for the HadoopZipExtension.
   *
   * @param project The Gradle project
   */
  HadoopZipExtension(Project project) {
    this.project = project;
    additionalPaths = new ArrayList<String>();
    libPath = ".";
    zipMap = new HashMap<String, CopySpec>();
  }

  /**
   * The files specified by the base copySpec will be added to all the zips including main.
   * The base spec is added as a child of the specific zip specs.
   * <pre>
   *   hadoopZip {
   *     base {
   *       from("common resources") {  // add the files common to all the zips.
   *         into "common"
   *       }
   *     }
   *     zip("holdem") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *     zip("war") {
   *       from ("azkaban/") {
   *         into "."
   *       }
   *     }
   *   }
   * </pre>
   * The DSL inside the {@code base\{} } block is the same DSL used for Copy tasks.
   */
  void base(Closure closure) {
    if (baseCopySpec != null) {
      throw new RuntimeException("base is already defined");
    }

    baseCopySpec = project.copySpec(closure);

    // Add the baseCopySpec to any zips that have already been declared
    zipMap.each { String zipName, CopySpec copySpec ->
      getZipCopySpec(zipName).with(getBaseCopySpec());
    }
  }

  /**
   * <pre>
   *   hadoopZip {
   *     main {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *   }
   * </pre>
   * The DSL inside the {@code main\{} } block is the same DSL used for Copy tasks.
   */
  void main(Closure closure) {
    zip("main", closure);
  }

  /**
   * <pre>
   *   hadoopZip {
   *     zip("holdem") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *     zip("war") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *   }
   * </pre>
   * The DSL inside the {@code zip(zipName)\{} } block is the same DSL used for Copy tasks.
   */
  void zip(String zipName, Closure closure) {
    if (zipMap.containsKey(zipName)){
      throw new RuntimeException("${zipName} is already defined");
    }
    zipMap.put(zipName, project.copySpec(closure));
    createZipTask(project, zipName);
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
      // CopySpec, add it as a child of the zip-specific CopySpec.
      if (getBaseCopySpec() != null) {
        task.with(getZipCopySpec(zipName).with(getBaseCopySpec()));
      }
      else {
        task.with(getZipCopySpec(zipName));
      }

      // For Java projects, include the project jar into the libPath directory in the zip by default
      project.getPlugins().withType(JavaPlugin) {
        from(project.tasks.getByName("jar")) { into libPath; }
      }

      // Include hadoopRuntime dependencies into the libPath directory in the zip
      from(project.configurations["hadoopRuntime"]) { into libPath; }

      // Include any additional paths added to the extension
      additionalPaths.each { String path -> from(path); }

      // Add the task to project artifacts
      if (project.configurations.findByName("archives") != null) {
        project.artifacts.add("archives", task);
      }

      // When everything is done, print out a message
      doLast {
        project.logger.lifecycle("Prepared archive for Hadoop zip '${zipName}' at: ${archivePath}");
      }
    }
  }

  /**
   * Utility method to return baseCopySpec.
   *
   * @return baseCopySpec
   */
  CopySpec getBaseCopySpec() {
    return baseCopySpec;
  }

  /**
   * Utility method to return the CopySpec for the given named zip.
   *
   * @param zipName
   * @return Returns the CopySpec for the given zip name
   */
  CopySpec getZipCopySpec(String zipName) {
    return zipMap.get(zipName);
  }

  /**
   * Utility method to return the zipMap.
   *
   * @return Returns the zipMap
   */
  Map<String, CopySpec> getZipMap() {
    return zipMap;
  }
}
