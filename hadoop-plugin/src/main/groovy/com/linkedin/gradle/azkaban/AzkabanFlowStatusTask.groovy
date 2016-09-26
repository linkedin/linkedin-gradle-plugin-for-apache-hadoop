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

import com.linkedin.gradle.util.HttpUtil;

import org.apache.http.client.utils.URIBuilder;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * AzkabanFlowStatusTask fetches the status of flows in Azkaban.
 */
class AzkabanFlowStatusTask extends DefaultTask {

  AzkabanProject azkProject;

  /**
   * The Gradle task action for getting flowStatus from Azkaban.
   */
  @TaskAction
  void status() {
    if (azkProject.azkabanUrl == null) {
      throw new GradleException("Please set azkaban.url in the .azkabanPlugin.json file in your project directory.");
    }
    getAzkabanFlowStatus(AzkabanHelper.readSession());
  }

  /**
   * Uploads the zip file to Azkaban.
   *
   * @param sessionId The Azkaban session id. If this is null, an attempt will be made to login to Azkaban.
   */
  void getAzkabanFlowStatus(String sessionId) {

    sessionId = AzkabanHelper.resumeOrGetSession(sessionId, azkProject);

    try {
      //Fetch flows of the project
      URI fetchFlowsUri = new URIBuilder(azkProject.azkabanUrl)
          .setPath("/manager")
          .addParameter("session.id", sessionId)
          .addParameter("ajax", "fetchprojectflows")
          .addParameter("project", azkProject.azkabanProjName)
          .build();

      String response = HttpUtil.responseFromGET(fetchFlowsUri);

      if (response.toLowerCase().contains("error")) {
        // Check if session has expired. If so, re-login.
        if (response.toLowerCase().contains("session")) {
          logger.lifecycle("\nPrevious Azkaban session expired. Please re-login.");
          getAzkabanFlowStatus(null);
        } else {
          // If response contains other than session error
          logger.error("Fetching flows from " + azkProject.azkabanUrl + " failed. Reason: " + new JSONObject(response).get("error"));
        }
        return;
      }

      List<String> flows = AzkabanHelper.fetchSortedFlows(new JSONObject(response), azkProject);

      //Pool HTTP Get requests for getting most recent ExecID for each flow
      List<URI> uriList = new ArrayList<URI>();
      for (String flow : flows) {
        uriList.add(new URIBuilder(azkProject.azkabanUrl)
            .setPath("/manager")
            .addParameter("session.id", sessionId)
            .addParameter("ajax", "fetchFlowExecutions")
            .addParameter("project", azkProject.azkabanProjName)
            .addParameter("flow", flow)
            .addParameter("start", "0")
            .addParameter("length", "1")
            .build());
      }

      List<String> responseList = HttpUtil.batchGet(uriList);

      List<String> execIds = new ArrayList<String>();
      for (String execResponse : responseList) {
        JSONObject execIDResponse = new JSONObject(execResponse);
        if (execIDResponse.has("error")) {
          logger.error("Could not get flow due to " + execIDResponse.get("error").toString());
          return;
        } else if (execIDResponse.has("executions")) {
          JSONArray lastExecArray = execIDResponse.getJSONArray("executions");
          if (lastExecArray != null && !lastExecArray.isNull(0)) {
            JSONObject lastExecObj = new JSONObject(lastExecArray.get(0).toString());
            execIds.add(lastExecObj.get("execId").toString());
          }
        }
      }

      if (!execIds.isEmpty()) {

        uriList.clear();

        //Pool HTTP Get requests for getting exec Job statuses
        execIds.each { execId ->
          uriList.add(new URIBuilder(azkProject.azkabanUrl)
              .setPath("/executor")
              .setParameter("session.id", sessionId)
              .setParameter("ajax", "fetchexecflow")
              .setParameter("execid", execId)
              .build());
        }

        responseList.clear();
        responseList = HttpUtil.batchGet(uriList);

        if (project.hasProperty("flow")) {
          HashSet<String> flowSet = new HashSet<String>(Arrays.asList(project.getProperties().get("flow").toString().split(",+")));
          List<String> sortedFlowSet = new ArrayList(flowSet);
          if (sortedFlowSet != null && !sortedFlowSet.isEmpty()) {
            Collections.sort(sortedFlowSet);
          } else {
            logger.error("Empty list, Nothing to do.");
            return;
          }
          jobStatus(sortedFlowSet, responseList);
        } else {
          flowStatus(flows, responseList);
        }

      } else {
        logger.lifecycle("Project ${azkProject.azkabanProjName} has no flow previously executed.");
      }

    } catch (IOException ex) {
      ex.printStackTrace();
    } catch (IllegalArgumentException ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Prints the Job Status of the latest flow executions.
   *
   * @param flowSet
   * @param responseList
   */
  void jobStatus(List<String> flowSet, List<String> responseList) {
    flowSet.each { flow ->
      for(int j=0; j<responseList.size(); j++) {
        String jobResponse = responseList.get(j);
        JSONObject jobsObject = new JSONObject(jobResponse);

        if(flow.equals(jobsObject.get("flow"))) {

          String execid = jobsObject.get("execid").toString();
          String flowStatus = jobsObject.get("status").toString();
          String flowSubmitTime = AzkabanHelper.epochToDate(jobsObject.get("submitTime").toString());
          String flowStartTime = AzkabanHelper.epochToDate(jobsObject.get("startTime").toString());
          String flowEndTime = AzkabanHelper.epochToDate(jobsObject.get("endTime").toString());
          String flowElapsedTime = AzkabanHelper.getElapsedTime(jobsObject.get("startTime").toString(), jobsObject.get("endTime").toString());

          System.out.println("\nFlow: ${flow} | Exec Id: ${execid} | Status: ${flowStatus} | Submitted: ${flowSubmitTime} | Started: ${flowStartTime} | Ended : ${flowEndTime} | Elapsed: ${flowElapsedTime}");

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
                String jobStartTime = AzkabanHelper.epochToDate(job.get("startTime").toString());
                String jobEndTime = AzkabanHelper.epochToDate(job.get("endTime").toString());
                String jobType = job.get("type").toString();
                String jobElapsedTime = AzkabanHelper.getElapsedTime(job.get("startTime").toString(), job.get("endTime").toString());
                AzkabanHelper.printJobStats(jobName, jobType, jobStatus, jobStartTime, jobEndTime, jobElapsedTime);
              }
            }
          }
          System.out.println("---------------------------------------------------------------------------------------------------------------------------------------------");
          printUrls(execid);
          break;
        }

        if ( j == responseList.size()-1) {
          System.out.println("\nFlow: ${flow}\n---------------------------------------------------------------------------------------------------------------------------------------------");
          System.out.print("\tNo execution yet.\n");
          System.out.println("---------------------------------------------------------------------------------------------------------------------------------------------");
        }
      }
    }
  }

  /**
   * Prints the status of the latest execution of each flow in the project.
   *
   * @param flows
   * @param responseList
   */
  void flowStatus(List<String> flows, List<String> responseList) {
    //Print the status

    System.out.println("\nJob Statistics for individual flow\n------------------------------------------------------------------------------------------------------------------------------------------------");
    AzkabanHelper.printFlowStats("Flow Name", "Latest Exec ID", "Status", AzkabanStatus.getStatusLabels());
    System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------");

    flows.each { flow ->
      for(int j=0; j<responseList.size(); j++) {
        String jobResponse = responseList.get(j);
        JSONObject jobsObject = new JSONObject(jobResponse);

        if(flow.equals(jobsObject.get("flow"))) {
          //get execid
          String execid = jobsObject.get("execid");
          //get status
          String status = jobsObject.get("status");

          AzkabanStatus azkStatus = new AzkabanStatus();

          if (jobsObject.has("nodes")) {
            JSONArray jobArray = jobsObject.getJSONArray("nodes");
            if (jobArray != null) {
              for(int i=0; i<jobArray.length(); i++) {
                JSONObject job = new JSONObject(jobArray.get(i).toString());
                String jobStatus = job.get("status").toString();
                azkStatus.increment(jobStatus);
              }
            }
          }
          AzkabanHelper.printFlowStats(flow, execid, status, azkStatus.getStatusValues());
          break;
        }

        if ( j == responseList.size() - 1 ) {
          AzkabanHelper.printFlowStats(flow, "NONE", "-", AzkabanStatus.getStatusLabels());
        }
      }
    }
    System.out.println("------------------------------------------------------------------------------------------------------------------------------------------------");
  }

  /**
   * Builds Dr. Elephant URL
   *
   * @param execUrl
   * @return DrElephantUrl
   */
  public String getDrElephantURL(String execUrl) {
    final String DR_ELEPHANT_URL = null;
    if (DR_ELEPHANT_URL != null) {
      return DR_ELEPHANT_URL + "/search?flow-exec-id=" + URLEncoder.encode(execUrl, "UTF-8").toString();
    }
    return null;
  }

  /**
   * Prints URL's of Azkaban Execution and Dr. Elephant Report
   *
   * @param execId Execution Id of the flow
   */
   void printUrls(String execId) {
     String execUrl = azkProject.azkabanUrl + "/executor?execid=${execId}";
     logger.lifecycle("Execution URL: ${execUrl}");
     String DrElephantUrl = getDrElephantURL(execUrl);
     if (DrElephantUrl != null) {
       logger.lifecycle("Dr.Elephant URL: ${DrElephantUrl}");
     }
   }
}