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
 * AzkabanCreateProjectTask creates a new project in Azkaban.
 */
class AzkabanCreateProjectTask extends DefaultTask {

  AzkabanProject azkProject;

  /**
   * The Gradle task action for creating project in Azkaban.
   */
  @TaskAction
  void create() {
    if (azkProject.azkabanUrl == null) {
      throw new GradleException("Please set azkaban.url in the .azkabanPlugin.json file in your project directory.");
    }
    createAzkabanProject(AzkabanHelper.readSession());
  }

  /**
   * Creates a project in Azkaban from project name in .azkabanPlugin.json file.
   *
   * @param sessionId The Azkaban session id. If this is null, an attempt will be made to login to Azkaban.
   */
  void createAzkabanProject(String sessionId) {
    sessionId = AzkabanHelper.resumeOrGetSession(sessionId, azkProject);

    def console = System.console();
    if (console == null) {
      String msg = "\nCannot access the system console. To use this task, explicitly set JAVA_HOME to the version specified in product-spec.json (at LinkedIn) and pass --no-daemon in your command.";
      throw new GradleException(msg);
    }

    String input = AzkabanHelper.consoleInput(console, " > Enter Project description: ", false);
    while (input.isEmpty()) {
      input = AzkabanHelper.consoleInput(console, "Enter Non-Empty Project description: ", false);
    }

    String response = AzkabanClient.createProject(azkProject.azkabanUrl, azkProject.azkabanProjName, input, sessionId);

    if (response.toLowerCase().contains("error")) {
      // Check if session has expired. If so, re-login.
      if (response.toLowerCase().contains("login")) {
        logger.lifecycle("\nPrevious Azkaban session expired. Please re-login.");
        createAzkabanProject(null);
      } else {
        // If response contains other than login error
        logger.error("Creating Project in ${azkProject.azkabanUrl} failed. Reason: " + new JSONObject(response).get("message"));
      }
      return;
    }

    logger.lifecycle("Successfully created project: ${azkProject.azkabanProjName} in Azkaban.");
  }
}
