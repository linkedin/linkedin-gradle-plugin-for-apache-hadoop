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
package com.linkedin.gradle.azkaban;

import com.linkedin.gradle.hadoopdsl.HadoopDslChecker;
import com.linkedin.gradle.hadoopdsl.HadoopDslFactory;
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin;

import groovy.json.JsonBuilder;

import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.bundling.Zip;


/**
 * AzkabanPlugin implements features for Azkaban, including building the Hadoop DSL for Azkaban.
 */
class AzkabanPlugin implements Plugin<Project> {

  static boolean interactive = true;
  protected final Logger logger = Logging.getLogger(AzkabanPlugin);

  /**
   * Applies the AzkabanPlugin. This adds the Gradle task that builds the Hadoop DSL for Azkaban.
   * Plugin users should have their build tasks depend on this task.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    createBuildFlowsTask(project);
    createCancelFlowTask(project);
    createCreateProjectTask(project);
    createExecuteFlowTask(project);
    createFlowStatusTask(project);
    createUploadTask(project);
    createWritePluginJsonTask(project);
  }

  /**
   * Creates the task to build the Hadoop DSL for Azkaban.
   *
   * @param project The Gradle project
   * @returns The created task
   */
  Task createBuildFlowsTask(Project project) {
    return project.tasks.create("buildAzkabanFlows") {
      description = "Builds the Hadoop DSL for Azkaban. Have your build task depend on this task.";
      group = "Hadoop Plugin";

      doLast {
        HadoopDslPlugin plugin = project.extensions.hadoopDslPlugin;
        if (plugin == null) {
          throw new GradleException("The Hadoop DSL Plugin has been disabled. You cannot run the buildAzkabanFlows task when the plugin is disabled.");
        }

        // Run the static checker on the DSL
        HadoopDslFactory factory = project.extensions.hadoopDslFactory;
        HadoopDslChecker checker = factory.makeChecker(project);
        checker.check(plugin);

        if (checker.failedCheck()) {
          throw new GradleException("Hadoop DSL static checker FAILED");
        }
        else {
          logger.lifecycle("Hadoop DSL static checker PASSED");
        }

        AzkabanDslCompiler compiler = makeCompiler(project);
        compiler.compile(plugin);
      }
    }
  }

  /**
   * Creates the task to Cancel a running flow in Azkaban.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createCancelFlowTask(Project project) {
    return project.task("azkabanCancelFlow", type: AzkabanCancelFlowTask) { task ->
      description = "Kills a running flows in Azkaban";
      group = "Hadoop's Azkaban Plugin";

      doFirst {
        azkProject = readAzkabanProject(project, false);

        if (project.hasProperty("execId")) {
          logger.lifecycle("Skipping interactive mode");
          interactive = false;
        }
      }

      doLast {
        interactive = true;
      }
    }
  }

  /**
   * Creates the task to create a new project in Azkaban.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createCreateProjectTask(Project project) {
    return project.task("azkabanCreateProject", type: AzkabanCreateProjectTask) { task ->
      description = "Creates a new Project in Azkaban";
      group = "Hadoop's Azkaban Plugin";

      doFirst {
        azkProject = readAzkabanProject(project, false);
      }
    }
  }

  /**
   * Creates the task to execute flows in Azkaban.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createExecuteFlowTask(Project project) {
    return project.task("azkabanExecuteFlow", type: AzkabanExecuteFlowTask) { task ->
      description = "Executes flows in Azkaban";
      group = "Hadoop's Azkaban Plugin";

      doFirst {
        azkProject = readAzkabanProject(project, false);

        if (project.hasProperty("flow")) {
          logger.lifecycle("Skipping interactive mode");
          interactive = false;
        }
      }

      doLast {
        if (interactive) {
          logger.lifecycle("\nTo skip interactive mode use -Pflow command line parameter and enter a comma separated list of flows.");
        }
        interactive = true;
      }
    }
  }

  /**
   * Creates the task to get flow status from Azkaban.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createFlowStatusTask(Project project) {
    return project.task("azkabanFlowStatus", type: getAzkabanFlowStatusTaskClass()) { task ->
      description = "Gets the status of all the flows in Azkaban";
      group = "Hadoop's Azkaban Plugin";

      doFirst {
        azkProject = readAzkabanProject(project, false);

        if (project.hasProperty("flow")) {
          logger.lifecycle("Displaying Job level Status");
          interactive = false;
        }
      }

      doLast {
        if (interactive) {
          logger.lifecycle("\nTo get job status use -Pflow command line parameter and provide a comma delimited list of flow names.");
        }
        interactive = true;
      }
    }
  }

  /**
   * Creates the task to upload to Azkaban.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createUploadTask(Project project) {
    return project.task("azkabanUpload", type: AzkabanUploadTask) { task ->
      description = "Uploads Hadoop zip archive to Azkaban. Use -PskipInteractive command line " +
          "parameter to skip interactive mode and ONLY read from the .azkabanPlugin.json file.";
      group = "Hadoop Plugin";

      doFirst {
        // Enable users to skip the interactive mode
        if (project.hasProperty("skipInteractive")) {
          logger.lifecycle("Skipping interactive mode");
          interactive = false;
        }

        azkProject = readAzkabanProject(project, interactive);

        String zipTaskName = azkProject.azkabanZipTask;
        if (!zipTaskName) {
          throw new GradleException("\nPlease set the property 'azkabanZipTask' in the .azkabanPlugin.json file");
        }

        def zipTaskCont = project.getProject().tasks[zipTaskName];
        if (zipTaskCont == null) {
          throw new GradleException("\nThe task ${zipTaskName} doesn't exist. Please specify a Gradle Zip task after configuring it in your build.gradle file.");
        }

        if (!zipTaskCont instanceof Zip) {
          throw new GradleException("\nThe task ${zipTaskName} is not a Zip task. Please specify a Gradle Zip task after configuring it in your build.gradle file.");
        }

        archivePath = zipTaskCont.archivePath;
      }

      doLast {
        if (interactive) {
          logger.lifecycle("\nUse -PskipInteractive command line parameter to skip interactive mode and ONLY read from the .azkabanPlugin.json file.");
          printUploadMessage();
        }
        interactive = true;
      }
    }
  }

  /**
   * Creates the task to write the plugin json file.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createWritePluginJsonTask(Project project) {
    return project.tasks.create("writeAzkabanPluginJson") {
      description = "Writes a default .azkabanPlugin.json file in the project directory";
      group = "Hadoop Plugin";

      doLast {
        String pluginJsonPath = getPluginJsonPath(project);
        if (!new File(pluginJsonPath).exists()) {
          AzkabanProject azkabanProject = makeDefaultAzkabanProject();
          String pluginJson = new JsonBuilder(azkabanProject).toPrettyString();
          new File(pluginJsonPath).write(pluginJson);
        }
      }
    }
  }

  /**
   * Factory method to return the AzkabanFlowStatusTask class. Subclasses can override this method to
   * return their own AzkabanFlowStatusTask class.
   *
   * @return Class that implements the AzkabanFlowStatusTask
   */
  Class<? extends AzkabanFlowStatusTask> getAzkabanFlowStatusTaskClass() {
    return AzkabanFlowStatusTask.class;
  }

  /**
   * Helper method to determine the location of the plugin json file. This helper method will make
   * it easy for subclasses to get (or customize) the file location.
   *
   * @param project The Gradle project
   * @return The path to the plugin json file
   */
  String getPluginJsonPath(Project project) {
    return "${project.projectDir}/.azkabanPlugin.json";
  }

  /**
   * Factory method to build an AzkabanProject. Can be overridden by subclasses.
   *
   * @param project The Gradle project
   * @return The AzkabanProject object
   */
  AzkabanProject makeAzkabanProject(Project project) {
    return new AzkabanProject();
  }

  /**
   * Factory method to build the Hadoop DSL compiler for Azkaban. Subclasses can override this
   * method to provide their own compiler.
   *
   * @param project The Gradle project
   * @return The AzkabanDslCompiler
   */
  AzkabanDslCompiler makeCompiler(Project project) {
    return new AzkabanDslCompiler(project);
  }

  /**
   * Factory method to build a default AzkabanProject for use with the writePluginJson method. Can
   * be overridden by subclasses.
   *
   * @param project The Gradle project
   * @return The AzkabanProject object
   */
  AzkabanProject makeDefaultAzkabanProject(Project project) {
    return makeAzkabanProject();
  }

  /**
   * Prints a message after upload to Azkaban is done. Subclasses can override this method.
   */
  void printUploadMessage() {
  }

  /**
   * Reads the AzkabanProject from the .azkabanPlugin.json file or the interactive console
   * @param project The Gradle project
   * @return The created AzkabanProject
   */
  AzkabanProject readAzkabanProject(Project project, boolean interactive) {
    AzkabanProject azkabanProject = AzkabanHelper.
        readAzkabanProjectFromJson(project, getPluginJsonPath(project));
    if (azkabanProject == null) {
      azkabanProject = AzkabanHelper.
          readAzkabanProjectFromInteractiveConsole(project, makeDefaultAzkabanProject(project),
              getPluginJsonPath(project));
    } else if (interactive) {
      azkabanProject = AzkabanHelper.readAzkabanProjectFromInteractiveConsole(project, azkabanProject, getPluginJsonPath(project));
    }
    return azkabanProject;
  }

}
