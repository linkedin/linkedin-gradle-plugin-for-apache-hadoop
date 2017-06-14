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

import org.apache.oozie.client.WorkflowJob.Status;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;

/**
 * Task used to run Oozie commands such as run, submit, start, resume, kill and status.
 */
class OozieCommandTask extends DefaultTask {
  OozieService oozieService;
  OozieProject oozieProject;
  String oozieAppDataDir;
  String oozieAppDataFile;

  // Allowed values for property "command"
  static String SUBMIT_COMMAND = "submit";
  static String RUN_COMMAND = "run";
  static String SUSPEND_COMMAND = "suspend";
  static String RESUME_COMMAND = "resume";
  static String START_COMMAND = "start";
  static String STATUS_COMMAND = "status";
  static String KILL_COMMAND = "kill";

  // Other property keys
  static String JOB_ID = "jobId";
  static String JOB_CONFIG = "config";
  static String OOZIE_COMMAND = "command";

  @TaskAction
  void runCommand() {

    // Check if the user has specified the command to run
    if (!project.hasProperty(OOZIE_COMMAND)) {
      throw new GradleException("The user must specify command as -Pcommand=commandName. The commandName can be run, submit, suspend, resume, kill, start and status");
    }

    // Set oozieAppDataDir and oozieAppDataFile. We need them to write and get the job id of last execution.
    oozieAppDataDir = "${System.properties['user.home']}/.ooziePlugin/${project.getName()}/${project.getVersion()}";
    oozieAppDataFile = "oozieAppData";

    // Initialize the OozieService with given url
    oozieService = makeOozieService(oozieProject.oozieURI);

    String oozieCommand = project."${OOZIE_COMMAND}";

    // Handle different commands
    switch (oozieCommand) {
      case SUBMIT_COMMAND:
        handleSubmit();
        break;
      case RUN_COMMAND:
        handleRun();
        break;
      case SUSPEND_COMMAND:
        handleSuspend();
        break;
      case RESUME_COMMAND:
        handleResume();
        break;
      case KILL_COMMAND:
        handleKill();
        break;
      case START_COMMAND:
        handleStart();
        break;
      case STATUS_COMMAND:
        handleStatus();
        break;
      default:
        throw new GradleException("Invalid command. Accepted commands are run, start, submit, suspend, resume, kill and status");
    }
  }

  /**
   * Handles submit command
   */
  void handleSubmit() {
    if (!project.hasProperty(JOB_CONFIG)) {
      throw new GradleException("The user must specify the configuration file as -Pconfig=configuration_file");
    }
    String jobId = oozieService.submitJob(project, project."${JOB_CONFIG}");
    writeOozieJobId(jobId);
    logger.lifecycle("Successfully submitted job with id ${jobId}")
  }

  /**
   * Handles run command
   */
  void handleRun() {
    if (!project.hasProperty(JOB_CONFIG)) {
      throw new GradleException("The user must specify the configuration file as -Pconfig=configuration_file");
    }
    String jobId = oozieService.runJob(project, project."${JOB_CONFIG}");
    writeOozieJobId(jobId);
    logger.lifecycle("Running job with id ${jobId}")
  }

  /**
   * Handles suspend command
   */
  void handleSuspend() {
    String jobId = getOozieJobId();
    oozieService.suspendJob(project, jobId);
    logger.lifecycle("suspending job with id ${jobId}");
  }

  /**
   * Handles resume command
   */
  void handleResume() {
    String jobId = getOozieJobId();
    oozieService.resumeJob(project, jobId);
    logger.lifecycle("Resuming job with id ${jobId}")
  }

  /**
   * Handles kill command
   */
  void handleKill() {
    String jobId = getOozieJobId();
    oozieService.killJob(project, jobId);
    logger.lifecycle("Killing job with id ${jobId}")
  }

  /**
   * Handles start command
   */
  void handleStart() {
    String jobId = getOozieJobId();
    oozieService.start(project, jobId);
    logger.lifecycle("Starting job with id ${jobId}")
  }

  /**
   * Handles status command
   */
  void handleStatus() {
    String jobId = getOozieJobId();
    Status status = oozieService.getJobStatus(project, jobId);
    logger.lifecycle("Current job status: ${status.toString()}")
  }

  /**
   * Returns a new OozieService
   * @param url The url of the oozie server
   * @return a new OozieService
   */
  OozieService makeOozieService(String url) {
    return new OozieService(project, url);
  }

  /**
   * Writes the oozieJob id to <user.home>/.ooziePlugin/<projectName>/<projectVersion>/oozieAppData
   * @param jobId The id of the job to write
   */
  void writeOozieJobId(String jobId) throws IOException {
    File file = new File(oozieAppDataDir);
    if (!file.exists()) {
      file.mkdirs();
    }
    File ooziePlugin = new File(file, oozieAppDataFile);
    ooziePlugin.withWriter { out ->
      out.writeLine("jobId=${jobId}");
    }
  }

  /**
   * Returns the jobId of the specified job else returns the jobId of last execution
   * @return jobId of the job
   */
  String getOozieJobId() {

    // If the user has specified the jobId then return the jobId
    if (project.hasProperty(JOB_ID)) {
      return project."${JOB_ID}";
    }

    // Otherwise look for the job id of last executed job
    Properties properties = new Properties();
    try {
      File file = new File(oozieAppDataDir, oozieAppDataFile);
      file.withInputStream { inputStream ->
        properties.load(inputStream);
      }
    }
    catch (IOException e) {
      // If cannot read the oozieAppData file then throw exception
      throw new GradleException("jobId not found for last execution. Please specify the job id as -PjobId=jobId")
    }

    // If jobId not found for last execution, then throw exception
    if (properties.get("jobId") == null) {
      throw new GradleException("jobId not found for last execution. Please specify the job id as -PjobId=jobId")
    }

    // Return jobId of the last execution
    return properties.get("jobId");
  }
}
