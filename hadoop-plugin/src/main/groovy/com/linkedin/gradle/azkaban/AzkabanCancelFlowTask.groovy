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

import com.linkedin.gradle.azkaban.client.AzkabanClient;
import com.linkedin.gradle.util.AzkabanClientUtil;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * AzkabanCancelFlowTask cancels a running flow execution in Azkaban.
 */
class AzkabanCancelFlowTask extends DefaultTask {

  AzkabanProject azkProject;

  /**
   * The Gradle task action to kill running flows in Azkaban.
   */
  @TaskAction
  void killer() {
    if (azkProject.azkabanUrl == null) {
      throw new GradleException("Please set azkaban.url in the .azkabanPlugin.json file in your project directory.");
    }
    cancelRunningAzkabanFlow(AzkabanHelper.readSession(azkProject.azkabanUrl));
  }

  /**
   * Cancels running flows in Azkaban.
   *
   * @param sessionId The Azkaban session ID. If this is null, an attempt will be made to login to Azkaban.
   */
  void cancelRunningAzkabanFlow(String sessionId) {
    // Fetch the project flows
    sessionId = AzkabanHelper.resumeOrGetSession(sessionId, azkProject);
    String fetchFlowsResponse = AzkabanClient.fetchProjectFlows(azkProject.azkabanUrl, azkProject.azkabanProjName, sessionId);

    if (fetchFlowsResponse.toLowerCase().contains("error")) {
      // Check if session has expired. If so, re-login.
      if (fetchFlowsResponse.toLowerCase().contains("session")) {
        logger.lifecycle("\nPrevious Azkaban session expired. Please re-login.");
        cancelRunningAzkabanFlow(null);
        return;
      }

      // If response contains other than session error
      String msg = "Fetching flows from ${azkProject.azkabanUrl} failed. Reason: " + new JSONObject(fetchFlowsResponse).get("error").toString();
      throw new GradleException(msg);
    }

    List<String> flows = AzkabanHelper.fetchSortedFlows(new JSONObject(fetchFlowsResponse));
    if (flows.isEmpty()) {
      logger.lifecycle("No flows defined in current project");
      return;
    }

    // Pool HTTP GET requests for getting running execution ID's of each flow
    List responseList = AzkabanClient.batchGetRunningExecutions(azkProject.azkabanUrl, azkProject.azkabanProjName, flows, sessionId);
    List<String> execIds = new ArrayList<String>();

    responseList.each { execResponse ->
      JSONObject execIDResponse = new JSONObject(execResponse);
      if (execIDResponse.has("error")) {
        logger.error("Could not get flow due to ${execIDResponse.get("error").toString()}");
      } else if (execIDResponse.has("execIds")) {
        JSONArray execArray = execIDResponse.getJSONArray("execIds");
        if (execArray != null) {
          for (int i = 0; i < execArray.length(); i++) {
            execIds.add(execArray.get(i).toString());
          }
        }
      }
    }

    if (execIds.isEmpty()) {
      logger.lifecycle("\nProject " + azkProject.azkabanProjName + " has no running flows to kill.");
      return;
    }

    Set<String> runningExecSet;

    if (AzkabanPlugin.interactive) {
      // Pool HTTP GET requests for getting exec Job statuses
      responseList = AzkabanClient.batchFetchFlowExecution(azkProject.azkabanUrl, execIds, sessionId);
      printStatus(flows, responseList);

      // Get exec ID's to kill
      def console = AzkabanHelper.getSystemConsole();
      String input = AzkabanHelper.consoleInput(console, " > Enter ID's of executions to be killed: ", true);

      if (input.isEmpty()) {
        logger.error("Nothing entered, exiting...");
        return;
      }

      runningExecSet = new HashSet<String>(Arrays.asList(input.split("\\D+")));
      if (runningExecSet != null && !runningExecSet.isEmpty()) {
        runningExecSet.sort();
        // Check if entered execIds are from this project
        for (String runningExec: runningExecSet) {
          if (!execIds.contains(runningExec)) {
            logger.error("Enter execution ID's from current project");
            return;
          }
        }
      } else {
        logger.error("Empty indices");
        return;
      }
    } else {
      List<String> execs = project.getProperties().get("execId").toString().split(",+");
      runningExecSet = new HashSet<String>();
      for (String exec : execs) {
        runningExecSet.add(exec);
      }
    }

    if (runningExecSet.isEmpty()) {
      logger.lifecycle("Entered no execution ID's");
      return;
    }

    // Pool HTTP GET requests for killing flows
    responseList = AzkabanClient.batchCancelFlowExecution(azkProject.azkabanUrl, runningExecSet, sessionId);

    for (String killResponse : responseList) {
      JSONObject killResponseObj = new JSONObject(killResponse);
      if (killResponseObj.has("error")) {
        logger.error(killResponseObj.get("error").toString());
      } else {
        logger.lifecycle("Successfully killed flow execution");
      }
    }
  }

  void printStatus(List<String> flows, List<String> responseList) {
    logger.lifecycle("-------------------");
    logger.lifecycle("FLOW RUNNING STATUS");
    logger.lifecycle("-------------------\n");

    flows.each { flow ->
      for (int j = 0; j < responseList.size(); j++) {
        String jobResponse = responseList.get(j);
        JSONObject jobsObject = new JSONObject(jobResponse);

        if(flow.equals(jobsObject.get("flow"))) {
          String execid = jobsObject.get("execid").toString();
          // String flowStatus = jobsObject.get("status").toString();
          String flowSubmitTime = AzkabanClientUtil.epochToDate(jobsObject.get("submitTime").toString());
          String flowStartTime = AzkabanClientUtil.epochToDate(jobsObject.get("startTime").toString());
          // String flowEndTime = AzkabanClientUtil.epochToDate(jobsObject.get("endTime").toString());
          String flowElapsedTime = AzkabanClientUtil.getElapsedTime(jobsObject.get("startTime").toString(), jobsObject.get("endTime").toString());

          System.out.println("Flow: ${flow} | Exec Id: ${execid} | Submitted: ${flowSubmitTime} | Started: ${flowStartTime} | Elapsed: ${flowElapsedTime}");

          System.out.println("---------------------------------------------------------------------------------------------------------------------------------------------");
          AzkabanHelper.printJobStats("JOB NAME", "JOB TYPE", "JOB STATUS", "JOB START TIME", "JOB END TIME", "ELAPSED");
          System.out.println("---------------------------------------------------------------------------------------------------------------------------------------------");

          if (jobsObject.has("nodes")) {
            JSONArray jobArray = jobsObject.getJSONArray("nodes");
            if (jobArray != null) {
              for(int i=0; i<jobArray.length(); i++) {
                JSONObject job = new JSONObject(jobArray.get(i).toString());
                String jobName = job.get("id").toString();
                String jobStatus = job.get("status").toString();
                String jobStartTime = AzkabanClientUtil.epochToDate(job.get("startTime").toString());
                String jobEndTime = AzkabanClientUtil.epochToDate(job.get("endTime").toString());
                String jobType = job.get("type").toString();
                String jobElapsedTime = AzkabanClientUtil.getElapsedTime(job.get("startTime").toString(), job.get("endTime").toString());
                AzkabanHelper.printJobStats(jobName, jobType, jobStatus, jobStartTime, jobEndTime, jobElapsedTime);
              }
            }
          }
          System.out.println("---------------------------------------------------------------------------------------------------------------------------------------------\n");
        }
      }
    }
  }
}
