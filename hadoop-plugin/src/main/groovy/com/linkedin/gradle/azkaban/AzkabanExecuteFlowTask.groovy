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

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;

import org.json.JSONObject;

/**
 * AzkabanExecuteFlowTask executes flows in Azkaban.
 */
class AzkabanExecuteFlowTask extends DefaultTask {

  AzkabanProject azkProject;

  /**
   * The Gradle task action for executing flows in Azkaban.
   */
  @TaskAction
  void execFlow() {
    if (azkProject.azkabanUrl == null) {
      throw new GradleException("Please set azkaban.url in the .azkabanPlugin.json file in your project directory.");
    }
    executeAzkabanFlow(AzkabanHelper.readSession());
  }

  /**
   * Executes flows in Azkaban.
   *
   * @param sessionId The Azkaban session id. If this is null, an attempt will be made to login to Azkaban.
   */
  String executeAzkabanFlow(String sessionId) {

    sessionId = AzkabanHelper.resumeOrGetSession(sessionId, azkProject);

    //Fetch flows of the project
    String fetchFlowsResponse = AzkabanClient.fetchProjectFlows(azkProject.azkabanUrl, azkProject.azkabanProjName, sessionId);

    if (fetchFlowsResponse.toLowerCase().contains("error")) {
      // Check if session has expired. If so, re-login.
      if (fetchFlowsResponse.toLowerCase().contains("session")) {
        logger.lifecycle("\nPrevious Azkaban session expired. Please re-login.");
        executeAzkabanFlow(null);
      } else {
        // If response contains other than session error
        logger.error("Fetching flows from ${azkProject.azkabanUrl} failed. Reason: ${new JSONObject(fetchFlowsResponse).get("error")}");
      }
      return;
    }

    List<String> flows = AzkabanHelper.fetchSortedFlows(new JSONObject(fetchFlowsResponse));
    if (flows.isEmpty()) {
      logger.lifecycle("No flows defined in current project");
      return;
    }

    List<String> inputFlows;
    try {
      if (AzkabanPlugin.interactive) {
        AzkabanHelper.printFlowsWithIndices(flows);
        Set<String> indexSet = getFlowIndicesInput();
        inputFlows = new ArrayList<String>()
        indexSet.size().times {
          inputFlows.add(flows.get(Integer.parseInt(indexSet.getAt(it))));
        }
      } else {
        inputFlows = project.getProperties().get("flow").toString().split(",+");
        for(String flowArg : inputFlows) {
          if (!flows.contains(flowArg)) {
            logger.error("Enter correct flow name(s) and try again.");
            return;
          }
        }
      }
    } catch (IndexOutOfBoundsException ex) {
      logger.error("${ex.getMessage()} : Entered indices not in range, try again.");
      return;
    }

    List<String> responseList = AzkabanClient.batchFlowExecution(azkProject.azkabanUrl, azkProject.azkabanProjName, inputFlows, sessionId);
    AzkabanHelper.printFlowExecutionResponses(responseList);

  }

  /**
   * Console Input for indices of flows.
   *
   * @return indexSet Set of entered indices corresponding to flows
   */
  static Set<String> getFlowIndicesInput() {
    def console = System.console();
    String input = AzkabanHelper.consoleInput(console, " > Enter indices of flows to be executed > ", true);
    Set<String> indexSet = new HashSet<String>(Arrays.asList(input.split("\\D+")));
    while(!input.trim().length() || indexSet.isEmpty()) {
      input = AzkabanHelper.consoleInput(console, "> Enter correct indices of flows to be executed > ", true);
      indexSet = new HashSet<String>(Arrays.asList(input.split("\\D+")));
    }
    return indexSet;
  }


}
