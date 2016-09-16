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
  void executeAzkabanFlow(String sessionId) {

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
          executeAzkabanFlow(null);
        } else {
          // If response contains other than session error
          logger.error("Fetching flows from ${azkProject.azkabanUrl} failed. Reason: ${new JSONObject(response).get("error")}");
        }
        return;
      }

      List<String> flows = AzkabanHelper.fetchSortedFlows(new JSONObject(response), azkProject);

      Set<String> indexSet;

      if (AzkabanPlugin.interactive) {
        logger.lifecycle("-----    -----");
        logger.lifecycle("INDEX    FLOWS");
        logger.lifecycle("-----    -----");

        flows.eachWithIndex { String flow, index ->
          logger.lifecycle("${index}        ${flow}");
        }
        logger.lifecycle("---------------------------");

        String input = AzkabanHelper.consoleInput(System.console(), " > Enter the flow indices to be executed > ", false);

        if(input.isEmpty()) {
          logger.lifecycle("Empty Input, Nothing to do.");
          return;
        }
        indexSet = new HashSet<String>(Arrays.asList(input.split("\\D+")));
      } else {
        List<String> inputFlows = project.getProperties().get("flow").toString().split(",+");
        indexSet = new HashSet<String>();

        for (String inputFlow : inputFlows) {
          for (int i = 0; i < flows.size(); i++) {
            if (inputFlow.equals(flows.get(i))) {
              indexSet.add(new Integer(i).toString());
              break;
            }
            if (i == flows.size() - 1) {
              logger.error("Enter correct flow name(s) and try again.");
              return;
            }
          }
        }
      }

      if (indexSet != null && !indexSet.isEmpty()) {
        indexSet.sort();
      } else {
        logger.error("Empty indices, Nothing to do.");
        return;
      }
      int[] flowIndices = new int[indexSet.size()];

      for (int i=0; i<indexSet.size(); i++) {
        String index = indexSet.getAt(i);
        if(index.toInteger().compareTo(0) < 0 || index.toInteger().compareTo(flows.size()) >= 0) {
          logger.error("Entered indices not in range, try again.");
          return;
        }
        flowIndices[i] = Integer.parseInt(index);
      }

      //Pool HTTP Get requests for executing flows
      List<URI> uriList = new ArrayList<URI>();
      flowIndices.each { index ->
        uriList.add(new URIBuilder(azkProject.azkabanUrl)
            .setPath("/executor")
            .addParameter("session.id", sessionId)
            .addParameter("ajax", "executeFlow")
            .addParameter("project", azkProject.azkabanProjName)
            .addParameter("flow", flows.get(index))
            .build());
      }

      List responseList = HttpUtil.batchGet(uriList);

      for (String execResponse : responseList) {
        JSONObject execResponseObj = new JSONObject(execResponse);
        if (execResponseObj.has("error")) {
          logger.error("Could not execute flow : ${execResponseObj.get("flow")}");
        } else {
          logger.lifecycle("${execResponseObj.get("flow")} : ${execResponseObj.get("message")}");
        }
      }

    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }
}
