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
import org.gradle.api.file.FileVisitDetails;
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
 *      libPath = "lib"
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
  AutomaticBuild automaticBuild;
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
    automaticBuild = null;
    libPath = "";
    zipMap = new LinkedHashMap<String, CopySpec>();
  }

  /**
   * Configures "automatic" mode for building your Hadoop zip artifacts.
   *
   * @param closure The configuration closure
   * @return The AutomaticBuild extension
   */
  AutomaticBuild automatic(Closure closure) {
    automaticBuild = new AutomaticBuild(project, this);
    automaticBuild.automaticMode = true;

    // Configure the automatic build properties according to the user specifications
    project.configure(automaticBuild, closure);

    // Automatically configure the build, including the Hadoop zip artifacts (unless the user
    // disabled automatic mode)
    return automaticBuild.automaticMode ? automaticBuild.setup() : automaticBuild;
  }

  /**
   * The files specified by the base copySpec will be added to all the zips including main.
   * The base spec is added as a child of the specific zip specs.
   * <pre>
   *   hadoopZip {
   *     base {
   *       from("common_resources") {  // add the files common to all the zips.
   *         into "common"
   *       }
   *     }
   *     zip("gridA") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *     zip("gridB") {
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
   *     zip("gridA") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *     zip("gridB") {
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
    return project.tasks.create(name: "${zipName}HadoopZip", type: Zip) { Task task ->
      classifier = zipName.equals("main") ? "" : zipName;
      description = "Creates a Hadoop zip archive for ${zipName}";
      group = "Hadoop Plugin";

      // This task is a dependency of buildHadoopZips and depends on the startHadoopZips
      project.tasks["buildHadoopZips"].dependsOn task;
      dependsOn "startHadoopZips";

      doFirst {
        mergeTempPropsWithFlowFiles(task, zipName);
      }

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
   * This enables "Emergent Flows" in Flow 2.0 - so users can define properties in a separate
   * namespace then merge them into the resulting yaml workflows at Zip time.
   *
   * This method retrieves all .tempprops source files in the task and turns them into Maps, then
   * merges those maps into the root level config of all of the .flow file (to ensure backward
   * compatibility). The resulting object is then saved in a new .flow file.
   *
   * @param task Task where merging may occur
   * @param zipName Name of the zip being generated - used to create new .flow file names
   */
  void mergeTempPropsWithFlowFiles(Task task, String zipName) {
    List<Map<String, String>> tempPropsList = [];
    List<String> excludeList = [];
    List<String> includeList = [];
    Map<String, FileVisitDetails> flowsToMerge = [:];

    task.source.visit { FileVisitDetails tempProps ->
      String tempPropsPath = tempProps.path;
      if (tempPropsPath.matches(/.*\.tempprops/)) {
        Map<String, String> tempPropsMap = YamlMerge.readInYaml(tempProps);
        tempPropsList.add(tempPropsMap);
        excludeList.add(tempPropsPath);
      }
    }

    // Merge all .tempprops files into the .flow files and create new .flow files with the results
    if (!tempPropsList.isEmpty()) {
      task.source.visit { FileVisitDetails fileDetails ->
        String file = fileDetails.path;
        if (file.matches(/.*\.flow/) && !file.matches(/.*_.*/)) {
          // Consider all .flow files to be flows except for those that have underscores
          // Those that have underscores are files that were already merged
          excludeList.add(file);
          flowsToMerge.put(file, fileDetails);
        }
        else if (file.matches(/.*_.*/) && file.matches(/.*\.flow/) &&
                !file.matches(/.*${zipName}.*/)) {
          // Remove all flow files with underscores that do not have the zipName included
          // These are merged flow files from another namespace
          excludeList.add(file);
        }
      }
      for (entry in flowsToMerge) {
        String newFlowFile = YamlMerge.merge(entry.value, tempPropsList, zipName);
        includeList.add(newFlowFile);
      }

      // Remove all .tempprops files and old .flow files from the zip and include the new .flow files
      task.exclude excludeList;
      includeList.each { String path -> task.from(path); }
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
