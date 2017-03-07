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

package com.linkedin.gradle.test;


import com.linkedin.gradle.azkaban.AzkabanDslCompiler;
import com.linkedin.gradle.azkaban.AzkabanHelper;
import com.linkedin.gradle.azkaban.AzkabanProject;
import com.linkedin.gradle.azkaban.AzkabanUploadTask;
import com.linkedin.gradle.hadoopdsl.HadoopDslChecker;
import com.linkedin.gradle.hadoopdsl.HadoopDslExtension;
import com.linkedin.gradle.hadoopdsl.HadoopDslFactory;
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.bundling.Zip;


/**
 * TestPlugin is used to test the workflows by override certain parameters such as data.
 * It provides a way to automatically deploy the generated
 * artifacts to the azkaban webserver and run it.
 **/
public class TestPlugin extends HadoopDslPlugin implements Plugin<Project> {
  private static testDirectory = null;
  private final static Logger logger = Logging.getLogger(TestPlugin);
  static boolean interactive = true;

  /**
   * Base constructor for the TestPlugin class
   **/
  TestPlugin() {
    super();
  }

  @Override
  public void apply(Project project) {
    addTestExtension(project);
    createPrintWorkflowTask(project);
    createBuildFlowsForTestingTask(project);
    createZipForTestingTask(project);
    createTestDeployTask(project);
  }

  /**
   * Add the plugin to testExtension
   * @param project The Gradle project
   */
  void addTestExtension(Project project) {
    project.extensions.add("testPlugin", this);
  }

  /**
   * Prints the tests, utility function for debugging
   * @param project The Gradle project
   * @return the task created
   */
  Task createPrintWorkflowTask(Project project) {
    return project.tasks.create("printTests") {
      description = "Prints all the tests added to the project";
      group = "Hadoop Plugin";

      doLast {
        HadoopDslExtension extension = project.extensions.getByName("hadoop");
        extension.tests.each {
          logger.lifecycle("--------------------------------------")
          logger.lifecycle(it.name + ": ");
          it.workflows.each {
            logger.lifecycle("\t* workflow : ${it.name}");
            it.jobs.each {
              logger.lifecycle("\t\t--> ${it.name}");
            }
          }
        }
      }
    }
  }

  /**
   * This task creates the flows for testing. It will substitute the parameters overriden in the tests block inside the hadoop construct
   * @param project The hadoop project
   * @return Return the created task
   */
  Task createBuildFlowsForTestingTask(Project project) {
    return project.tasks.create("buildFlowsForTesting") {
      description = "Builds the hadoop DSL for testing";
      group = "Hadoop Plugin";

      doLast {

        if (!project.hasProperty("testname")) {
          logger.error("You must specify the name of the test using -Ptestname=name");
          throw new RuntimeException("Test not found");
        }

        TestPlugin plugin = project.extensions.testPlugin;
        if (plugin == null) {
          throw new GradleException(
              "The Hadoop DSL Plugin has been disabled. You cannot run the buildAzkabanFlows task when the plugin is disabled.");
        }

        String testName;
        String buildDir;
        boolean foundTest = false;

        testName = project.testname;
        HadoopDslExtension testExtension = project.extensions.getByName("hadoop");
        buildDir = testExtension.buildDirectory;
        testExtension.tests.each {
          if (it.name.equals(testName)) {
            plugin.extension = it;
            foundTest = true;
            return;
          }
        }

        if (!foundTest) {
          logger.error("Test ${testName} not found")
          throw new RuntimeException("Specified test ${testName} is not found");
        }

        // delete the DSL build directory
        def deleted = new File(buildDir).deleteDir();

        File testDirFile = new File(new File(buildDir, "tests").getAbsolutePath(), testName);
        testDirFile.deleteDir();
        testDirectory = testDirFile.getAbsolutePath();

        plugin.extension.buildDirectory = testDirectory;
        HadoopDslFactory factory = project.extensions.hadoopDslFactory;
        HadoopDslChecker checker = factory.makeChecker(project);
        checker.check(plugin);

        if (checker.failedCheck()) {
          throw new GradleException("Hadoop DSL static checker FAILED");
        } else {
          logger.lifecycle("Hadoop DSL static checker PASSED");
        }

        AzkabanDslCompiler compiler = makeCompiler(project);
        compiler.compile(plugin);
      }
    }
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
   * This task will take all the job files along with the hadoopZip extension and create a separate zip for testing of the project. Technically we
   * want to retain the original zip but for each test a new zip has to be generated.
   * @param project The hadoop project
   * @return Return the created task
   */
  Task createZipForTestingTask(Project project) {
    return project.tasks.create("buildZipForTesting") {
      dependsOn project.tasks["buildHadoopZips"]
      dependsOn project.tasks["buildFlowsForTesting"]
      project.tasks["buildFlowsForTesting"].shouldRunAfter project.tasks["buildAzkabanFlows"]
      project.tasks["buildHadoopZips"].shouldRunAfter project.tasks["buildFlowsForTesting"]
    }
  }

  /**
   * Creates the testDeploy task. This task will automatically build and upload the zip to the test machines
   * @param project The Gradle project
   * @return The created task
   */
  Task createTestDeployTask(Project project) {
    return project.task("testDeploy", type: AzkabanUploadTask) { task ->
      description = "Deploys the test provided by -Ptestname=name"
      group = "Hadoop Plugin";
      dependsOn project.tasks["buildZipForTesting"]

      doFirst {

        if (!project.hasProperty("testname")) {
          logger.error("You must specify the name of the test using -Ptestname=name");
          throw new RuntimeException("Test not found");
        }

        if (project.hasProperty("skipInteractive")) {
          logger.lifecycle("Skipping interactive mode");
          interactive = false;
        }

        azkProject = readAzkabanProject(project);

        String zipTaskName = azkProject.azkabanZipTask;
        if (!zipTaskName) {
          throw new GradleException("\nPlease set the property 'azkabanZipTask' in the .azkabanPlugin.json file");
        }

        def zipTaskCont = project.getProject().tasks[zipTaskName];
        if (zipTaskCont == null) {
          throw new GradleException(
              "\nThe task ${zipTaskName} doesn't exist. Please specify a Gradle Zip task after configuring it in your build.gradle file.");
        }

        if (!zipTaskCont instanceof Zip) {
          throw new GradleException(
              "\nThe task ${zipTaskName} is not a Zip task. Please specify a Gradle Zip task after configuring it in your build.gradle file.");
        }

        archivePath = zipTaskCont.archivePath;
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
   * Creates a default AzkabanProject
   * @param project The Gradle project
   * @return The default AzkabanProject
   */
  AzkabanProject makeDefaultAzkabanProject(Project project) {
    return new AzkabanProject();
  }

  /**
   * Reads the AzkabanProject from the .azkabanPlugin.json and the interactive console
   * @param project The Gradle project
   * @return The azkaban project
   */
  AzkabanProject readAzkabanProject(Project project) {
    AzkabanProject azkabanProject = AzkabanHelper.
        readAzkabanProjectFromJson(project, getPluginJsonPath(project));
    if (azkabanProject == null) {
      azkabanProject = AzkabanHelper.
          readAzkabanProjectFromInteractiveConsole(project, makeDefaultAzkabanProject(project),
              getPluginJsonPath(project));
    } else if (interactive) {
      azkabanProject.azkabanProjName = azkabanProject.azkabanProjName + "-" + project.testname
      azkabanProject = AzkabanHelper.
          readAzkabanProjectFromInteractiveConsole(project, azkabanProject, getPluginJsonPath(project));
    } else {
      azkabanProject.azkabanProjName = azkabanProject.azkabanProjName + "-" + project.testname
    }

    return azkabanProject;
  }
}
