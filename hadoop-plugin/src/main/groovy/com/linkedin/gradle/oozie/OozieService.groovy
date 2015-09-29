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

import org.apache.oozie.client.AuthOozieClient;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.apache.oozie.client.WorkflowJob;
import org.apache.oozie.client.WorkflowJob.Status;
import org.gradle.api.Project;

import java.security.AccessControlException;

/**
 * OozieService class provides different methods to submit, run, start, suspend, resume, kill and get status of oozie job.
 */
public class OozieService {

  // Initialize oozieClient
  OozieClient oozieClient = null;

  /**
   * OozieService with simple authentication
   * @param url The url of the oozie server
   */
  public OozieService(Project project, String url) {
    oozieClient = new OozieClient(url);
    project.logger.info("initialized OozieService with simple authentication");
  }

  /**
   * OozieService with kerberos authentication
   * @param url The url of the oozie server
   * @param krb5Conf The krb5conf file
   */
  public OozieService(Project project, String url, File krb5Conf) throws AccessControlException {
    // Set kerberos authentication
    if (!checkForKinit()) {
      project.logger.error("The user has not kinited. Please kinit first.");
      throw new AccessControlException("The user has not kinited");
    }
    System.setProperty("java.security.krb5.conf", krb5Conf.getAbsolutePath());
    oozieClient = new AuthOozieClient(url);
    project.logger.info("initialized OozieService with kerberos authentication");
  }

  /**
   * Submits an oozie workflow but doesn't start it
   * @param jobPropertyFilePath
   * @return The jobId of the submitted job
   * @throws OozieClientException
   * @throws IOException
   */
  public String submitJob(Project project, String jobPropertyFilePath) throws OozieClientException {
    Properties conf = oozieClient.createConfiguration();
    conf.load(new FileInputStream(jobPropertyFilePath));
    String jobId = oozieClient.submit(conf);
    project.logger.info("Submitted job with id ${jobId}");
    return jobId;
  }

  /**
   * Submits and runs an oozie workflow
   * @param jobPropertyFilePath
   * @return The jobId of the job
   * @throws OozieClientException
   * @throws IOException
   */
  public String runJob(Project project, String jobPropertyFilePath) throws OozieClientException {
    Properties conf = oozieClient.createConfiguration();
    conf.load(new FileInputStream(jobPropertyFilePath));
    String jobId = oozieClient.run(conf);
    project.logger.info("Running job with id ${jobId}");
    return jobId;
  }

  /**
   * Suspends the job specified by jobId
   * @param jobId The job id of the job to suspend
   * @throws OozieClientException
   */
  public void suspendJob(Project project, String jobId) throws OozieClientException {
    project.logger.info("Suspending job ${jobId} ...");
    oozieClient.suspend(jobId);
    project.logger.info("${jobId} suspended successfully");
  }

  /**
   * Resumes a job which was suspended
   * @param jobId The job id of the job to resume
   * @throws OozieClientException
   */
  public void resumeJob(Project project, String jobId) throws OozieClientException {
    project.logger.info("Resuming job ${jobId} ...");
    oozieClient.resume(jobId);
    project.logger.info("${jobId} resumed successfully");
  }

  /**
   * Kills a job specified by the job id
   * @param jobId The job id of the job to kill
   * @throws OozieClientException
   */
  public void killJob(Project project, String jobId) throws OozieClientException {
    project.logger.info("Killing job ${jobId}");
    oozieClient.kill(jobId);
    project.logger.info("${jobId} killed successfully");
  }

  /**
   * Starts the job specified by the job id
   * @param jobId The job id of the job to start
   * @throws OozieClientException
   */
  public void start(Project project, String jobId) throws OozieClientException {
    project.logger.info("Starting job ${jobId}");
    oozieClient.start(jobId);
    project.logger.info("${jobId} started successfully");
  }

  /**
   * Gets the status of the job specified by the job id
   * @param jobID The job id of the job
   * @return The status of the job specified by job id
   * @throws OozieClientException
   */
  public Status getJobStatus(Project project, String jobId) throws OozieClientException {
    WorkflowJob job = oozieClient.getJobInfo(jobId);
    project.logger.info("Getting job status for job ${jobId}");
    Status jobStatus = job.getStatus();
    project.logger.info("Current status for job ${jobId} : ${jobStatus.toString()}");
    return jobStatus;
  }

  /**
   * Checks if the user has kinited.
   *
   * @return Whether the user has kinited or not
   */
  private boolean checkForKinit() throws AccessControlException {
    String[] command = ["klist", "-s"];

    def processBuilder = new ProcessBuilder();
    processBuilder.command(command);

    def process = processBuilder.start();
    process.waitFor();
    return (process.exitValue() == 0);
  }
}

