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
import com.linkedin.gradle.azkaban.AzkabanFlowStatusTask;
import com.linkedin.gradle.azkaban.AzkabanBlockedFlowStatusTask;
import com.linkedin.gradle.azkaban.AzkabanHelper;
import com.linkedin.gradle.azkaban.AzkabanProject;
import com.linkedin.gradle.azkaban.AzkabanUploadTask;
import com.linkedin.gradle.azkaban.client.AzkabanClient;
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

import org.json.JSONObject;


/**
 * TestPlugin is used to test the workflows by overriding certain parameters such as data. It
 * provides a way to automatically deploy the generated artifacts to Azkaban and run them.
 **/
class TestPlugin extends HadoopDslPlugin implements Plugin<Project> {
  private final static Logger logger = Logging.getLogger(TestPlugin);
  private static testDirectory = null;
  static boolean interactive = true;

  /**
   * Base constructor for the TestPlugin class.
   **/
  TestPlugin() {
    super();
  }

  @Override
  void apply(Project project) {
    addTestExtension(project);
    createPrintWorkflowTask(project);
    createBuildFlowsForTestingTask(project);
    createZipForTestingTask(project);
    createTestDeployTask(project);
    createRunTestTask(project);
    createGetTestStatusTask(project);
    createRunAssertionsTask(project);
    createGetAssertionStatusTask(project);
    createHadoopTestTask(project);
  }

  /**
   * Adds the plugin as a Gradle project extension.
   *
   * @param project The Gradle project
   */
  void addTestExtension(Project project) {
    project.extensions.add("workflowTestSuite", this);
  }

  /**
   * Prints the tests, utility function for debugging.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createPrintWorkflowTask(Project project) {
    return project.tasks.create("printAzkabanTests") {
      description = "Prints all the Hadoop tests added to the project";
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
   * This task creates the flows for testing. It will substitute the parameters overridden in the
   * tests block inside the hadoop construct.
   *
   * @param project The hadoop project
   * @return Return the created task
   */
  Task createBuildFlowsForTestingTask(Project project) {
    return project.tasks.create("buildFlowsForTesting") {
      description = "Builds the hadoop DSL for testing";
      group = "Hadoop Plugin";

      doLast {
        validateTestnameProperty(project);

        TestPlugin plugin = project.extensions.workflowTestSuite;
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

        // Delete the DSL build directory
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
   * This task will take all the job files along with the hadoopZip extension and create a separate
   * zip for testing of the project. Technically we want to retain the original zip but for each
   * test a new zip has to be generated.
   *
   * @param project The Hadoop project
   * @return Return the created task
   */
  Task createZipForTestingTask(Project project) {
    return project.tasks.create("buildZipForTesting") {
      dependsOn project.tasks["buildHadoopZips"]
      dependsOn project.tasks["buildFlowsForTesting"]
      project.tasks["buildFlowsForTesting"].mustRunAfter project.tasks["buildAzkabanFlows"]
      project.tasks["buildHadoopZips"].mustRunAfter project.tasks["buildFlowsForTesting"]
    }
  }

  /**
   * Creates the testDeploy task. This task will automatically build and upload the zip to the test
   * machines.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createTestDeployTask(Project project) {
    return project.task("testDeploy", type: AzkabanUploadTask) { task ->
      description = "Deploys the test provided by -Ptestname=name"
      group = "Hadoop Plugin";
      dependsOn project.tasks["buildZipForTesting"]

      doFirst {
        validateTestnameProperty(project);

        if (project.hasProperty("skipInteractive")) {
          logger.lifecycle("Skipping interactive mode");
          interactive = false;
        }

        azkProject = getTestProjectName(project, interactive);
        String message = "The test project on Azkaban is ${azkProject.azkabanProjName}. Your test will be deployed to ${azkProject.azkabanProjName}"
        prettyPrintMessage(message);

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
   * Creates the runTest task. This will run all the workflows defined in the test specified by
   * testname. If a flow is specified -Pflow=flowname, then only flow with name flowname will be
   * run.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createRunTestTask(Project project) {
    return project.task("runTest") { task ->
      description = "Runs the test provided by -Ptestname=name, Alternatively a flow can be specified with the test using -Pflow=flowname"
      group = "Hadoop Plugin";

      doFirst {
        validateTestnameProperty(project);

        def azkProject = getTestProjectName(project, false);

        String message = " The test project on Azkaban is ${azkProject.azkabanProjName}. Your test will be run in ${azkProject.azkabanProjName}";
        prettyPrintMessage(message);
        List<String> testWorkflows = getTestWorkflows(project, AzkabanHelper.resumeOrGetSession(AzkabanHelper.readSession(), azkProject), azkProject);
        executeAzkabanFlow(project, AzkabanHelper.readSession(), testWorkflows, azkProject);
      }
    }
  }


  /**
   * Creates the runTest task. This will run all the workflows defined in the test specified by
   * testname. If a flow is specified -Pflow=flowname, then only flow with name flowname will be
   * run.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createRunAssertionsTask(Project project) {
    return project.task("runAssertions") { task ->
      description = "Runs the assertions for the test provided by -Ptestname=name, Alternatively a flow can be specified with the test using -Pflow=flowname"
      group = "Hadoop Plugin";
      dependsOn project.tasks["runTest"]
      dependsOn project.tasks["getTestStatus"]

      doFirst {
        validateTestnameProperty(project);

        def azkProject = getTestProjectName(project, false);

        String message = " The test project on Azkaban is ${azkProject.azkabanProjName}. Your assertions will be run in ${azkProject.azkabanProjName}";
        prettyPrintMessage(message);
        List<String> testWorkflows = getAssertionWorkflows(project, AzkabanHelper.resumeOrGetSession(AzkabanHelper.readSession(), azkProject), azkProject);
        executeAzkabanFlow(project, AzkabanHelper.readSession(), testWorkflows, azkProject);
      }
    }
  }

  /**
   * The getWorkflowsFromFlowProperty returns the list of flows from the 'flow' property. It throws
   * and error if the workflow mentioned in flow property is not defined
   * @param project The Gradle project
   * @param flows The defined flows
   * @return The list of flows
   */
  List<String> getWorkflowsFromFlowProperty(Project project, List<String> flows) {
    List<String> inputFlows = project.getProperties().get("flow").toString().split(",+");
    for (String flowArg : inputFlows) {
      if (!flows.contains(flowArg)) {
        logger.error("ERROR: The flow name ${flowArg} doesn't exist");
        throw new RuntimeException("The flow name ${flowArg} doesn't exist!");
      }
    }
    return inputFlows;
  }


  /**
   * The getTestWorkflows method returns the list of test workflows defined in the test
   * @param project The Gradle project
   * @param sessionId The session id of Azkaban session
   * @param azkProject The azkaban project
   * @return THe list of test workflows
   */
  List<String> getTestWorkflows(Project project, String sessionId, AzkabanProject azkProject) {
    List<String> flows = getFlowsOrThrowError(project, sessionId, azkProject);
    List<String> inputFlows;
    if (project.getProperties().get("flow") == null) {
      inputFlows = filterTestFlows(flows, project);
    } else {
      inputFlows = getWorkflowsFromFlowProperty(project, flows);
    }
    return inputFlows;
  }

  /**
   * The getAssertionWorkflows method returns the list of the assertion workflows defined in the test
   * @param project The Gradle project
   * @param sessionId The session id of the Azkaban session
   * @param azkProject The azkaban project
   * @return The list of assertion workflows
   */
  List<String> getAssertionWorkflows(Project project, String sessionId, AzkabanProject azkProject) {
    List<String> flows = getFlowsOrThrowError(project, sessionId, azkProject);
    List<String> inputFlows;
    if (project.getProperties().get("flow") == null) {
      inputFlows = filterAssertionFlows(flows, project);
    } else {
      inputFlows = getWorkflowsFromFlowProperty(project, flows);
    }
    return inputFlows;
  }

  /**
   * filterAssertionFlows gives the list of all the assertion flows that are defined in the test block
   * @param flows The list of all the flows
   * @param project The Gradle project
   * @return The list of all the assertion flows that are defined in the tewst block
   */
  List<String> filterAssertionFlows(List<String> flows, Project project) {
    HadoopDslExtension testExtension = project.extensions.getByName("hadoop");
    TestExtension test = testExtension.tests.find {
      it.name.equals(project.getProperties().get("testname"));
    }

    return flows.findAll {
      test.assertionWorkflows.contains(it.substring(it.indexOf("_")+1));
    }
  }

  /**
   * filterTestFlows gives the list of test flows that are defined in the test block
   * @param flows List of all the flows that are defined in the test block
   * @param project The Gradle project
   * @return List of all the test flows that are defined in the test block
   */
  List<String> filterTestFlows(List<String> flows, Project project) {
    HadoopDslExtension testExtension = project.extensions.getByName("hadoop");
    TestExtension test = testExtension.tests.find {
      it.name.equals(project.getProperties().get("testname"));
    }

    return flows.findAll {
      !test.assertionWorkflows.contains(it.substring(it.indexOf("_")+1));
    }
  }

  /**
   * Executes the flows on Azkaban.
   *
   * @param project The Gradle project
   * @param sessionId The sessionId for the project
   * @param azkProject The Azkaban Project
   */
  void executeAzkabanFlow(Project project, String sessionId, List<String> inputFlows, AzkabanProject azkProject) {
    sessionId = AzkabanHelper.resumeOrGetSession(sessionId, azkProject);

    List<String> responseList = AzkabanClient.
        batchFlowExecution(azkProject.azkabanUrl, azkProject.azkabanProjName, inputFlows, sessionId);
    AzkabanHelper.printFlowExecutionResponses(responseList);
  }



  /**
   * Returns a list of all flows in project azkProject.azkabanProjName.
   *
   * @param project The Gradle project
   * @param sessionId The session id for the Azkaban session
   * @param azkProject The Azkaban Project
   * @return List of all flows in project azkProject.azkabanProjName. Returns an empty list if no flows are defined in the current project.
   */
  List<String> getFlowsOrThrowError(Project project, String sessionId, AzkabanProject azkProject) {
    // Fetch flows of the project
    String fetchFlowsResponse = AzkabanClient.
        fetchProjectFlows(azkProject.azkabanUrl, azkProject.azkabanProjName, sessionId);

    if (fetchFlowsResponse.toLowerCase().contains("error")) {
      // Check if session has expired. If so, re-login.
      if (fetchFlowsResponse.toLowerCase().contains("session")) {
        logger.lifecycle("\nPrevious Azkaban session expired. Please re-login.");
        return getFlowsOrThrowError(project, AzkabanHelper.resumeOrGetSession(null, azkProject), azkProject);
      } else {
        // If response contains other than session error
        logger.error(
            "Fetching flows from ${azkProject.azkabanUrl} failed. Reason: ${new JSONObject(fetchFlowsResponse).get("error")}");
        throw new RuntimeException(
            "Error fetching flows from ${azkProject.azkabanUrl}. Reason ${new JSONObject(fetchFlowsResponse).get("error")}");
      }
    }

    List<String> flows = AzkabanHelper.fetchSortedFlows(new JSONObject(fetchFlowsResponse));

    if (flows.isEmpty()) {
      logger.lifecycle("No flows defined in current project");
    }

    return flows;
  }

  /**
   * Creates the getTestStatus task which gets the status of the flows in the project.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createGetTestStatusTask(Project project) {
    return project.task("getTestStatus", type: AzkabanBlockedFlowStatusTask) { task ->
      description = "Gets the status of the test specified by -Ptestname=testname. Alternatively a flowname can be specified using -Pflow=flowname";
      group = "Hadoop Plugin";

      doFirst {
        validateTestnameProperty(project);

        azkProject = getTestProjectName(project, false);
        List<String> testflows = filterTestFlows(getFlowsOrThrowError(project, AzkabanHelper.readSession(), azkProject), project);
        flowsToExecute = testflows;

        String message = " The test project on Azkaban is ${azkProject.azkabanProjName}. Your test status will be fetched from ${azkProject.azkabanProjName}";
        prettyPrintMessage(message);

        if (project.hasProperty("flow")) {
          logger.lifecycle("Displaying Job level Status");
          interactive = false;
        } else {
          logger.lifecycle("Displaying flow level Status");
        }
      }
    }
  }

  /**
   * Creates the getTestStatus task which gets the status of the flows in the project.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createGetAssertionStatusTask(Project project) {
    return project.task("getAssertionStatus", type: AzkabanBlockedFlowStatusTask) { task ->
      description = "Gets the status of the test specified by -Ptestname=testname. Alternatively a flowname can be specified using -Pflow=flowname";
      group = "Hadoop Plugin";

      doFirst {
        validateTestnameProperty(project);

        azkProject = getTestProjectName(project, false);
        List<String> testflows = filterAssertionFlows(getFlowsOrThrowError(project, AzkabanHelper.readSession(), azkProject), project);
        flowsToExecute = testflows;

        if(flowsToExecute.empty) {
          logger.error("No assertions defined in the workflow");
          return;
        }

        String message = " The test project on Azkaban is ${azkProject.azkabanProjName}. Your test status will be fetched from ${azkProject.azkabanProjName}";
        prettyPrintMessage(message);

        if (project.hasProperty("flow")) {
          logger.lifecycle("Displaying Job level Status");
          interactive = false;
        } else {
          logger.lifecycle("Displaying flow level Status");
        }
      }
    }
  }

  /**
   * Creates the workflowTestSuite task. This task depends on testDeploy, runTest and getTestStatus tasks. This is a single task to deploy
   * run and get status of the test.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createHadoopTestTask(Project project) {
    return project.task("azkabanTest") { task ->
      description = "Runs the test specified by -Ptestname=test";
      group = "Hadoop Plugin";
      dependsOn project.tasks["testDeploy"]
      dependsOn project.tasks["runTest"]
      dependsOn project.tasks["getTestStatus"]
      dependsOn project.tasks["runAssertions"]
      dependsOn project.tasks["getAssertionStatus"]
      project.tasks["getTestStatus"].mustRunAfter project.tasks["runTest"]
      project.tasks["runTest"].mustRunAfter project.tasks["testDeploy"]
      project.tasks["getAssertionStatus"].mustRunAfter project.tasks["runAssertions"]
      project.tasks["runAssertions"].mustRunAfter project.tasks["getTestStatus"]
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
   *
   * @param project The Gradle project
   * @return The default AzkabanProject
   */
  AzkabanProject makeDefaultAzkabanProject(Project project) {
    return new AzkabanProject();
  }

  /**
   * Utility method to pretty print the message
   *
   * @param message The message to print
   */
  void prettyPrintMessage(String message) {
    logger.lifecycle("${"-" * (message.length() + 8)}\n\n\t${message}\n\n${"-" * (message.length() + 8)}");
  }

  /**
   * Checks for testname property. If it is not presents, throws an exception.
   *
   * @param project The Gradle project
   */
  void validateTestnameProperty(Project project) {
    if (!project.hasProperty("testname")) {
      String errorMessage = "ERROR: You must specify the name of the test using -Ptestname=test"
      logger.error(errorMessage);
      throw new GradleException(
          "${"-" * (errorMessage.length() + 8)}\n\n\t${errorMessage}\n\n${"-" * (errorMessage.length() + 8)}");
    }
  }

  /**
   * Reads the AzkabanProject from the .azkabanPlugin.json and the interactive console
   *
   * @param project The Gradle project
   * @return The azkaban project
   */
  AzkabanProject readAzkabanProject(Project project, boolean interactive) {
    AzkabanProject azkabanProject = AzkabanHelper.
        readAzkabanProjectFromJson(project, getPluginJsonPath(project));
    if (azkabanProject == null) {
      azkabanProject = AzkabanHelper.
          readAzkabanProjectFromInteractiveConsole(project, makeDefaultAzkabanProject(project),
              getPluginJsonPath(project));
    } else if (interactive) {
      azkabanProject.azkabanProjName = azkabanProject.azkabanProjName;
      azkabanProject = AzkabanHelper.
          readAzkabanProjectFromInteractiveConsole(project, azkabanProject, getPluginJsonPath(project));
    } else {
      azkabanProject.azkabanProjName = azkabanProject.azkabanProjName;
    }

    return azkabanProject;
  }

  /**
   * Modifies the name of the project according to the test.
   *
   * @param project The Gradle project
   * @param interactive enable interactive input
   * @return The modified Azkaban project
   */
  AzkabanProject getTestProjectName(Project project, boolean interactive) {
    AzkabanProject azkProject = readAzkabanProject(project, interactive)
    azkProject.azkabanProjName = azkProject.azkabanProjName + "_" + project.getProperties().get("testname");
    return azkProject;
  }
}
