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
package com.linkedin.gradle.oozie;

import com.linkedin.gradle.hadoopdsl.HadoopDslChecker;
import com.linkedin.gradle.hadoopdsl.HadoopDslFactory;
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin;

import groovy.json.JsonBuilder;
import groovy.json.JsonSlurper;

import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.bundling.Zip;

/**
 * OoziePlugin implements features for Apache Oozie, including building the Hadoop DSL for Oozie.
 */
class OoziePlugin implements Plugin<Project> {
  /**
   *  Applies the OoziePlugin.
   *
   *  @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    // Enable users to skip the plugin
    if (project.hasProperty("disableOoziePlugin")) {
      println("OoziePlugin disabled");
      return;
    }

    createBuildFlowsTask(project);
    createUploadTask(project);
    createWritePluginJsonTask(project);
    createOozieCommandTask(project);
  }

  /**
   * Creates the task to run oozieCommands
   * @param project The gradle project
   * @return The created task
   */
  Task createOozieCommandTask(Project project) {
    return project.tasks.create(name: "oozieCommand", type: getOozieCommandTaskClass()) { task ->
      description = "Runs the oozieCommand specified by -Pcommand=CommandName"
      group = "Hadoop Plugin";
      doFirst {
        oozieProject = readOozieProject(project);
      }
    }
  }


  /**
   * Creates the task to build the Hadoop DSL for Oozie.
   *
   * @param project The Gradle project
   * @returns The created task
   */
  Task createBuildFlowsTask(Project project) {
    return project.tasks.create("buildOozieFlows") {
      description = "Builds the Hadoop DSL for Apache Oozie. Have your build task depend on this task.";
      group = "Hadoop Plugin";

      doLast {
        HadoopDslPlugin plugin = project.extensions.hadoopDslPlugin;
        HadoopDslFactory factory = project.extensions.hadoopDslFactory;

        // Run the static checker on the DSL
        HadoopDslChecker checker = factory.makeChecker(project);
        checker.check(plugin);

        if (checker.failedCheck()) {
          throw new Exception("Hadoop DSL static checker FAILED");
        }
        else {
          logger.lifecycle("Hadoop DSL static checker PASSED");
        }

        OozieDslCompiler compiler = makeCompiler(project);
        compiler.compile(plugin);
      }
    }
  }

  /**
   * Creates the task to upload to HDFS for Oozie.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createUploadTask(Project project) {
    return project.tasks.create(name: "oozieUpload", type: getOozieUploadTaskClass()) { task ->
      dependsOn "buildHadoopZips";
      description = "Uploads the Oozie project folder to HDFS";
      group = "Hadoop Plugin";

      doFirst {
        oozieProject = readOozieProject(project);
        String zipTaskName = oozieProject.oozieZipTask;
        if (!zipTaskName) {
          throw new GradleException("\nPlease set the property 'oozieZipTask' in the .ooziePlugin.json file");
        }

        def zipTaskOozie = project.getProject().tasks[zipTaskName];
        if (zipTaskOozie == null) {
          throw new GradleException("\nThe task " + zipTaskName + " doesn't exist. Please specify a Zip task after configuring it in your build.gradle file.");
        }
        if (!zipTaskOozie instanceof Zip) {
          throw new GradleException("\nThe task " + zipTaskName + " is not a Zip task. Please specify a Zip task after configuring it in your build.gradle file.");
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
    return project.tasks.create("writeOoziePluginJson") {
      description = "Writes a default .ooziePlugin.json file in the project directory";
      group = "Hadoop Plugin";

      doLast {
        def ooziePluginFilePath = "${project.getProjectDir()}/.ooziePlugin.json";
        if (!new File(ooziePluginFilePath).exists()) {
          OozieProject oozieProject = makeDefaultOozieProject();
          String oozieData = new JsonBuilder(oozieProject).toPrettyString();
          new File(ooziePluginFilePath).write(oozieData);
        }
      }
    }
  }

  /**
   * Factory method to return the OozieUploadTask class. Subclasses can override this method to
   * return their own OozieUploadTask class.
   *
   * @return Class that implements the OozieUploadTask
   */
  Class<? extends OozieUploadTask> getOozieUploadTaskClass() {
    return OozieUploadTask.class;
  }

  /**
   * Factory method to return the OozieCommandTask class. Subclasses can override this method to
   * return their own OozieUploadTask class;
   *
   * @return Class that implements the OozieCommandTask
   */
  Class<? extends OozieCommandTask> getOozieCommandTaskClass() {
    return OozieCommandTask.class;
  }

  /**
   * Helper method to determine the location of the plugin json file. This helper method will make
   * it easy for subclasses to get (or customize) the file location.
   *
   * @param project The Gradle project
   * @return The path to the plugin json file
   */
  String getPluginJsonPath(Project project) {
    return "${project.getProjectDir()}/.ooziePlugin.json";
  }

  /**
   * Factory method to build the Hadoop DSL compiler for Apache Oozie. Subclasses can override this
   * method to provide their own compiler.
   *
   * @param project The Gradle project
   * @return The OozieDslCompiler
   */
  OozieDslCompiler makeCompiler(Project project) {
    return new OozieDslCompiler(project);
  }

  /**
   * Factory method to build a default OozieProject for use with the writePluginJson method. Can be
   * overridden by subclasses.
   *
   * @param project The Gradle project
   * @return The OozieProject object
   */
  OozieProject makeDefaultOozieProject(Project project) {
    return makeOozieProject();
  }

  /**
   * Factory method to build an OozieProject. Can be overridden by subclasses.
   *
   * @param project The Gradle project
   * @return The OozieProject object
   */
  OozieProject makeOozieProject(Project project) {
    return new OozieProject();
  }

  /**
   * Helper method to read the plugin json file as a JSON object.
   *
   * @param project The Gradle project
   * @return A JSON object or null if the file does not exist
   */
  def readOoziePluginJson(Project project) {
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
      throw new Exception("\nError parsing ${pluginJsonPath}.\n" + ex.toString());
    }
    finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Loads the Oozie project properties defined in the .ooziePlugin.json file.
   *
   * @return An OozieProject object with the properties set
   */
  OozieProject readOozieProject(Project project) {
    def pluginJson = readOoziePluginJson(project);
    if (pluginJson == null) {
      throw new GradleException("\n\nPlease run \"gradle writeOoziePluginJson\" to create a default .ooziePlugin.json file in your project directory which you can then edit.\n")
    }

    OozieProject oozieProject = makeOozieProject(project);
    oozieProject.clusterURI = pluginJson[OozieConstants.OOZIE_CLUSTER_URI]
    oozieProject.oozieURI = pluginJson[OozieConstants.OOZIE_SYSTEM_URI]
    oozieProject.oozieZipTask = pluginJson[OozieConstants.OOZIE_ZIP_TASK]
    oozieProject.projectName = pluginJson[OozieConstants.OOZIE_PROJECT_NAME]
    oozieProject.uploadPath = pluginJson[OozieConstants.PATH_TO_UPLOAD]
    return oozieProject;
  }
}