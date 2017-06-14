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
import com.linkedin.gradle.util.IOUtils;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;
import org.json.JSONObject;

/**
 * This class provides the functionality to fetch the status of the flows when they are completed.
 * */
class AzkabanBlockedFlowStatusTask extends AzkabanFlowStatusTask {

  private static final Long POLLING_WAIT_TIME = 20000; // default 20s

  /**
   * The Gradle task action for getting flowStatus from Azkaban.
   **/
  @TaskAction
  void status() {
    if (azkProject.azkabanUrl == null) {
      throw new GradleException("Please set azkaban.url in the .azkabanPlugin.json file in your project directory.");
    }
    getBlockedAzkabanFlowStatus(AzkabanHelper.readSession());
  }

  /**
   * This function returns the list of flows from which status should be fetched.
   *
   * @return The list of flows from which status should be fetched
   */
  List<String> getFlowsToFetchStatus() {
    if (project.hasProperty("flow")) {
      if (project.getProperties().get("flow").toString().isEmpty()) {
        return null;
      } else {
        List<String> sortedFlowSet = new ArrayList(
            new HashSet<String>(Arrays.asList(project.getProperties().get("flow").toString().split(",+"))));
        Collections.sort(sortedFlowSet);
        return sortedFlowSet;
      }
    } else if (flowsToExecute != null && !flowsToExecute.isEmpty()) {
      return flowsToExecute;
    } else {
      return new ArrayList<String>();
    }
  }

  /**
   * This method waits for the flow to finish. It polls the Azkaban server and outputs the flows
   * that finished immediately. At the end of the function, it outputs the summary of all the flows
   * that have finished.
   *
   * @param sessionId The session ID for the Azkaban session
   */
  void getBlockedAzkabanFlowStatus(String sessionId) {
    // list of all the flows defined in the project
    List<String> flows = getSortedFlows(sessionId);

    // list of all the recent execution ids of the flows defined in the project
    List<String> execIds = getRecentFlowExecutionIds(sessionId, flows);

    // The response from Azkaban as a list
    List<String> responseList = new ArrayList<String>();

    // list of the flows from which the status should be fetched
    List<String> flowsToFetchStatusFrom = getFlowsToFetchStatus();

    // get list of flow names that have been executed in Azkaban
    List<String> flowNamesInAzkaban = getFlowNames(
        AzkabanClient.batchFetchFlowExecution(azkProject.azkabanUrl, execIds, sessionId));

    if (flowsToFetchStatusFrom == null || flowsToFetchStatusFrom.empty) {
      logger.error("No flows defined");
      return;
    }

    flowsToFetchStatusFrom.each {
      if (!flowNamesInAzkaban.contains(it)) {
        throw new RuntimeException("The flow hasn't been executed");
      }
    }

    Set<String> flowsYetToFinish = new HashSet<String>();
    flowsYetToFinish.addAll(flowsToFetchStatusFrom); // initially all flows are yet to finish
    int countOfFlowsFinished = 0;

    while (!flowsYetToFinish.empty && !execIds.isEmpty()) {

      if (!execIds.isEmpty()) {
        //Pool HTTP Get requests for getting exec Job statuses
        responseList = filterCompletedFlows(
            AzkabanClient.batchFetchFlowExecution(azkProject.azkabanUrl, execIds, sessionId));
        List<String> finishedFlowNames = getFlowNames(responseList);

        // print the completed flows
        if (!responseList.empty) {
          responseList.each {
            JSONObject jsonObject = new JSONObject(it);
            String flowName = jsonObject.get("flow");
            String flowStatus = jsonObject.get("status");
            if (flowsYetToFinish.contains(flowName) && flowsToFetchStatusFrom.contains(flowName)) {
              countOfFlowsFinished += 1;
              logger.lifecycle("");
              logger.lifecycle(
                  "\nFlows [${countOfFlowsFinished}/${flowsToFetchStatusFrom.size()}]:=> Flow ${flowName} completed with status ${flowStatus}\n");
            }
          }
        }

        // remove finished flows from flowsYetToFinish list
        finishedFlowNames.each {
          flowsYetToFinish.remove(it);
        }
      } else {
        logger.lifecycle("Project ${azkProject.azkabanProjName} has no flow previously executed.");
      }

      if (!flowsYetToFinish.empty) {
        IOUtils.printSpinningLoader(40,500);
        Thread.sleep(POLLING_WAIT_TIME);
      }
    }

    // once all the flows have completed, print the summary
    logger.lifecycle("Summary of the completed flows");
    if (project.hasProperty("flow")) {
      if (project.getProperties().get("flow").toString().isEmpty()) {
        jobStatus(flows, responseList);
      } else {
        List<String> sortedFlowSet = new ArrayList(
            new HashSet<String>(Arrays.asList(project.getProperties().get("flow").toString().split(",+"))));
        Collections.sort(sortedFlowSet);
        jobStatus(sortedFlowSet, responseList);
      }
    } else if (flowsToExecute != null && !flowsToExecute.isEmpty()) {
      flowStatus(flowsToExecute, responseList);
    } else {
      flowStatus(flows, responseList);
    }
  }

  /**
   * This method filters out the flows that have finished and returns them.
   *
   * @param responseList The reponseList from Azkaban
   * @return The list of flows that have completed
   */
  List<String> filterCompletedFlows(List<String> responseList) {
    List<String> filteredJobResponse = new ArrayList<String>();
    for (int j = 0; j < responseList.size(); j++) {
      String jobResponse = responseList.get(j);
      JSONObject jobsObject = new JSONObject(jobResponse);
      if (!jobsObject.get("status").equals("RUNNING") && !jobsObject.get("status").equals("READY")) {
        filteredJobResponse.add(jobResponse);
      }
    }
    return filteredJobResponse;
  }

  /**
   * Given a list of Azkaban response json, extract the flow names and return it as a list.
   *
   * @param responseList The responseList from Azkaban
   * @return The list of names of the workflows defined in responseList
   */
  List<String> getFlowNames(List<String> responseList) {
    List<String> filteredJobResponse = new ArrayList<String>();
    for (int j = 0; j < responseList.size(); j++) {
      String jobResponse = responseList.get(j);
      JSONObject jobsObject = new JSONObject(jobResponse);
      filteredJobResponse.add(jobsObject.get("flow").toString());
    }
    return filteredJobResponse;
  }
}
