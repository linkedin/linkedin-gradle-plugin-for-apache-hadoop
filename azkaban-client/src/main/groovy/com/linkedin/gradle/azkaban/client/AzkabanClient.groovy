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
package com.linkedin.gradle.azkaban.client;

import com.linkedin.gradle.util.HttpUtil;
import org.apache.http.client.utils.URIBuilder;

/**
 * Azkaban Client for Azkaban.
 */
class AzkabanClient {
  /**
   * Creates a project in Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param projectName Azkaban Project Name
   * @param description Project Description
   * @param sessionId The Azkaban session id
   * @return
   */
  static String createProject(String URL, String projectName, String description, String sessionId) throws Exception {
    return HttpUtil.responseFromPOST(new URIBuilder(URL)
        .setPath(AzkabanParams.MANAGER)
        .setParameter(AzkabanParams.SESSIONID, sessionId)
        .setParameter(AzkabanParams.ACTION, "create")
        .setParameter(AzkabanParams.NAME, projectName)
        .setParameter(AzkabanParams.DESCRIPTION, description)
        .build());
  }

  /**
   * Executes a flow in Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param projectName Azkaban Project Name
   * @param flow Flow Name
   * @param sessionId The Azkaban session id
   * @return
   */
  static String executeFlow(String URL, String projectName, String flow, String sessionId) throws Exception {
    return HttpUtil.responseFromGET(new URIBuilder(URL)
        .setPath(AzkabanParams.EXECUTOR)
        .addParameter(AzkabanParams.SESSIONID, sessionId)
        .addParameter(AzkabanParams.AJAX, "executeFlow")
        .addParameter(AzkabanParams.PROJECT, projectName)
        .addParameter(AzkabanParams.FLOW, flow)
        .build());
  }

  /**
   * Fetches flow Executions from Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param projectName Azkaban Project Name
   * @param flow Flow Name
   * @param startIndex Index of execution to start from
   * @param endIndex Index of execution to end
   * @param sessionId The Azkaban session id
   * @return
   */
  static String fetchExecutionsList(String URL, String projectName, String flow, String startIndex, String endIndex, String sessionId) throws Exception {
    return HttpUtil.responseFromGET(new URIBuilder(URL)
        .setPath(AzkabanParams.MANAGER)
        .addParameter(AzkabanParams.SESSIONID, sessionId)
        .addParameter(AzkabanParams.AJAX, "fetchFlowExecutions")
        .addParameter(AzkabanParams.PROJECT, projectName)
        .addParameter(AzkabanParams.FLOW, flow)
        .addParameter(AzkabanParams.START, startIndex)
        .addParameter(AzkabanParams.LENGTH, endIndex)
        .build());
  }

  /**
   * Fetches all the detailed information of that execution, including a list of all the job executions
   * from Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param execId Azkaban flow execution id
   * @param sessionId The Azkaban session id
   * @return
   */
  static String fetchFlowExecution(String URL, String execId, String sessionId) throws Exception {
    return HttpUtil.responseFromGET(new URIBuilder(URL)
        .setPath(AzkabanParams.EXECUTOR)
        .setParameter(AzkabanParams.SESSIONID, sessionId)
        .setParameter(AzkabanParams.AJAX, "fetchexecflow")
        .setParameter(AzkabanParams.EXECID, execId)
        .build());
  }

  /**
   * Fetch flows of the project in Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param projectName Azkaban Project Name
   * @param sessionId The Azkaban session id
   * @return
   */
  static String fetchProjectFlows(String URL, String projectName, String sessionId) throws Exception {
    return HttpUtil.responseFromGET(new URIBuilder(URL)
        .setPath(AzkabanParams.MANAGER)
        .addParameter(AzkabanParams.SESSIONID, sessionId)
        .addParameter(AzkabanParams.AJAX, "fetchprojectflows")
        .addParameter(AzkabanParams.PROJECT, projectName)
        .build());
  }

  /**
   * Executes Multiple flows in Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param projectName Azkaban Project Name
   * @param flows List of all flows in the Project
   * @param sessionId The Azkaban session id
   * @return
   */
  static List<String> batchFlowExecution(String URL, String projectName, List<String> flows, String sessionId) throws Exception {
    //Pool HTTP Get requests for executing flows
    List<URI> executeUriList = new ArrayList<URI>();
    flows.each { flow ->
      executeUriList.add(new URIBuilder(URL)
          .setPath(AzkabanParams.EXECUTOR)
          .addParameter(AzkabanParams.SESSIONID, sessionId)
          .addParameter(AzkabanParams.AJAX, "executeFlow")
          .addParameter(AzkabanParams.PROJECT, projectName)
          .addParameter(AzkabanParams.FLOW, flow)
          .build());
    }
    return HttpUtil.batchGet(executeUriList);
  }

  /**
   * Fetches Latest Flow Execution for Multiple flows in Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param projectName Azkaban Project Name
   * @param flows List of Flow names for which latest execution is needed
   * @param sessionId The Azkaban session id
   * @return List of response containing Latest execution for each flow.
   */
  static List<String> batchFetchLatestExecution(String URL, String projectName, List<String> flows, String sessionId) throws Exception {
    //Pool HTTP Get requests for getting most recent ExecID for each flow
    List<URI> uriList = new ArrayList<URI>();
    for (String flow : flows) {
      uriList.add(new URIBuilder(URL)
          .setPath(AzkabanParams.MANAGER)
          .addParameter(AzkabanParams.SESSIONID, sessionId)
          .addParameter(AzkabanParams.AJAX, "fetchFlowExecutions")
          .addParameter(AzkabanParams.PROJECT, projectName)
          .addParameter(AzkabanParams.FLOW, flow)
          .addParameter(AzkabanParams.START, "0")
          .addParameter(AzkabanParams.LENGTH, "1")
          .build());
    }
    return HttpUtil.batchGet(uriList);
  }

  /**
   * Fetches a batch of Flow Executions from Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param execIds List of Azkaban flow execution ids
   * @param sessionId The Azkaban session id
   * @return
   */
  static List<String> batchFetchFlowExecution(String URL, List<String> execIds, String sessionId) throws Exception {
    List<URI> uriList = new ArrayList<URI>();
    execIds.each { execId ->
      uriList.add(new URIBuilder(URL)
          .setPath(AzkabanParams.EXECUTOR)
          .setParameter(AzkabanParams.SESSIONID, sessionId)
          .setParameter(AzkabanParams.AJAX, "fetchexecflow")
          .setParameter(AzkabanParams.EXECID, execId)
          .build());
    }
    return HttpUtil.batchGet(uriList);
  }

  /**
   * Cancels Flow Execution in Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param execId Azkaban flow execution id
   * @param sessionId The Azkaban session id
   * @return
   */
  static String cancelFlowExecution(String URL, String execId, String sessionId) throws Exception {
    return HttpUtil.responseFromGET(new URIBuilder(URL)
        .setPath(AzkabanParams.EXECUTOR)
        .addParameter(AzkabanParams.SESSIONID, sessionId)
        .addParameter(AzkabanParams.AJAX, "cancelFlow")
        .addParameter(AzkabanParams.EXECID, execId)
        .build());
  }

  /**
   * Cancels a batch of Executions in Azkaban.
   *
   * @param URL AzkabanCluster URL
   * @param execIds List of Azkaban flow execution ids
   * @param sessionId The Azkaban session id
   * @return
   */
  static List<String> batchCancelFlowExecution(String URL, Set<String> execIds, String sessionId) throws Exception {
    List<URI> uriList = new ArrayList<URI>();
    execIds.each { execId ->
      uriList.add(new URIBuilder(URL)
          .setPath(AzkabanParams.EXECUTOR)
          .addParameter(AzkabanParams.SESSIONID, sessionId)
          .addParameter(AzkabanParams.AJAX, "cancelFlow")
          .addParameter(AzkabanParams.EXECID, execId)
          .build());
    }
    return HttpUtil.batchGet(uriList);
  }

  /**
   * Fetches execIds of Running Executions.
   *
   * @param URL AzkabanCluster URL
   * @param projectName Azkaban Project Name
   * @param flow Flow Name
   * @param sessionId The Azkaban session id
   * @return
   */
  static String getRunningExecutions(String URL, String projectName, String flow, String sessionId) throws Exception {
    return HttpUtil.responseFromGET(new URIBuilder(URL)
        .setPath(AzkabanParams.EXECUTOR)
        .addParameter(AzkabanParams.SESSIONID, sessionId)
        .addParameter(AzkabanParams.AJAX, "getRunning")
        .addParameter(AzkabanParams.PROJECT, projectName)
        .addParameter(AzkabanParams.FLOW, flow)
        .build());
  }

  /**
   * Fetches a batch of execIds of Running Executions.
   *
   * @param URL AzkabanCluster URL
   * @param projectName Azkaban Project Name
   * @param flows List of all flows in the Project
   * @param sessionId The Azkaban session id
   * @return
   */
  static List<String> batchGetRunningExecutions(String URL, String projectName, List<String> flows, String sessionId) throws Exception {
    List<URI> uriList = new ArrayList<URI>();
    flows.each { flow ->
      uriList.add(new URIBuilder(URL)
          .setPath(AzkabanParams.EXECUTOR)
          .addParameter(AzkabanParams.SESSIONID, sessionId)
          .addParameter(AzkabanParams.AJAX, "getRunning")
          .addParameter(AzkabanParams.PROJECT, projectName)
          .addParameter(AzkabanParams.FLOW, flow)
          .build());
    }
    return HttpUtil.batchGet(uriList);
  }
}
