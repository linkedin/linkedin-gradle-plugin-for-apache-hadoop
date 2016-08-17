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
import groovy.json.JsonSlurper;

import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.bundling.Zip;

import static com.linkedin.gradle.azkaban.AzkabanConstants.*;

/**
 * AzkabanPlugin implements features for Azkaban, including building the Hadoop DSL for Azkaban.
 */
class AzkabanPlugin implements Plugin<Project> {

  boolean interactive = true;
  private final static Logger logger = Logging.getLogger(AzkabanPlugin);

  /**
   * Applies the AzkabanPlugin. This adds the Gradle task that builds the Hadoop DSL for Azkaban.
   * Plugin users should have their build tasks depend on this task.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    createBuildFlowsTask(project);
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
   * Creates the task to upload to Azkaban.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createUploadTask(Project project) {
    return project.task("azkabanUpload", type: AzkabanUploadTask) { task ->
      description = "Uploads Hadoop zip archive to Azkaban. Use -PskipInteractive command line parameter to" +
          " skip asking for confirmation and ONLY read from the .azkabanPlugin.json file.";
      group = "Hadoop Plugin";

      doFirst {
        // Enable users to skip the interactive mode
        if (project.hasProperty("skipInteractive")) {
          logger.lifecycle("Skipping Interactive mode");
          interactive = false;
        }

        if (project.hasProperty("showProgress")) {
          AzkabanUploadTask.showProgress = true;
        }

        azkProject = readAzkabanProject(project);
        String zipTaskName = azkProject.azkabanZipTask;
        if (!zipTaskName) {
          throw new GradleException("\nPlease set the property 'azkabanZipTask' in the .azkabanPlugin.json file");
        }

        def zipTaskCont = project.getProject().tasks[zipTaskName];
        if (zipTaskCont == null) {
          throw new GradleException("\nThe task " + zipTaskName + " doesn't exist. Please specify a Zip task after configuring it in your build.gradle file.");
        }

        if (!zipTaskCont instanceof Zip) {
          throw new GradleException("\nThe task " + zipTaskName + " is not a Zip task. Please specify a Zip task after configuring it in your build.gradle file.");
        }

        archivePath = zipTaskCont.archivePath;
      }

      doLast {
        if (interactive) {
          logger.lifecycle("\nUse -PskipInteractive command line parameter to skip asking for confirmation and ONLY read from the .azkabanPlugin.json file.");
        }

        if (!AzkabanUploadTask.showProgress) {
          logger.lifecycle("Use -PshowProgress command line parameter to show the Zip Upload status.");
        }
      }
    }
  }

  /**
   * Creates the task to write the plugin json.
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
   * Helper method to read the plugin json file as a JSON object. For backwards compatibility
   * reasons, we should read it as a JSON object instead of coercing it into a domain object.
   *
   * @param project The Gradle project
   * @return A JSON object or null if the file does not exist
   */
  def readAzkabanPluginJson(Project project) {
    String pluginJsonPath = getPluginJsonPath(project);
    if (!new File(pluginJsonPath).exists()) {
      return null;
    }

    def reader = null;
    try {
      reader = new BufferedReader(new FileReader(pluginJsonPath));
      def slurper = new JsonSlurper();
      def pluginJson = slurper.parse(reader);
      return pluginJson;
    }
    catch (Exception ex) {
      throw new GradleException("\nError parsing ${pluginJsonPath}.\n" + ex.toString());
    }
    finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Load the Azkaban project properties defined in the .azkabanPlugin.json file.
   *
   * @return An AzkabanProject object with the properties set
   */
  AzkabanProject readAzkabanProject(Project project) {
    def pluginJson = readAzkabanPluginJson(project);
    AzkabanProject azkProject = makeDefaultAzkabanProject(); //changed makeAzkabanProject here to makeDefaultAzkabanProject()

    if (pluginJson != null) {
      // If the file exists, the task should use this information, but give the user the chance
      // to confirm or change the Azkaban URL / project / user / ZipTask. If the user changes this information,
      // ask them if they want to save the changes (to the .azkabanPlugin.json file).

      azkProject.azkabanProjName = pluginJson[AZK_PROJ_NAME];
      azkProject.azkabanUrl = pluginJson[AZK_URL];
      azkProject.azkabanUserName = pluginJson[AZK_USER_NAME];
      azkProject.azkabanValidatorAutoFix = pluginJson[AZK_VAL_AUTO_FIX];
      azkProject.azkabanZipTask = pluginJson[AZK_ZIP_TASK];
    } else {
      // The file doesn't exist, the azkabanUpload task should ask the user for this information.
      logger.lifecycle("File .azkabanPlugin.json not found. Automatically switching to Interactive mode");
      interactive = true;
    }

    // When specifying this command line parameter, the task should fail if the file does not exist or is not completely filled out.
    if(interactive && AzkabanHelper.configureTask(azkProject)) { //configureTask is called only if interactive is true
      String updatedPluginJson = new JsonBuilder(azkProject).toPrettyString();
      new File(getPluginJsonPath(project)).write(updatedPluginJson);
    }

    return azkProject;
  }

}
