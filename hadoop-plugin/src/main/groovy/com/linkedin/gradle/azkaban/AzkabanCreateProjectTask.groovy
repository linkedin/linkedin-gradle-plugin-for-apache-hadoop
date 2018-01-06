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
 * AzkabanCreateProjectTask creates a new Azkaban project.
 */
class AzkabanCreateProjectTask extends DefaultTask {

  AzkabanProject azkProject;

  /**
   * The Gradle task action for creating an Azkaban project.
   */
  @TaskAction
  void create() {
    if (azkProject.azkabanUrl == null) {
      throw new GradleException("Please set azkaban.url in the .azkabanPlugin.json file in your project directory.");
    }
    createAzkabanProject(AzkabanHelper.readSession(azkProject.azkabanUrl));
  }

  /**
   * Creates a project in Azkaban from the project name in the .azkabanPlugin.json file.
   *
   * @param sessionId The Azkaban session ID. If this is null, an attempt will be made to login to Azkaban.
   */
  void createAzkabanProject(String sessionId) {
    sessionId = AzkabanHelper.resumeOrGetSession(sessionId, azkProject);

    def console = AzkabanHelper.getSystemConsole();
    String input = AzkabanHelper.consoleInput(console, " > Enter Azkaban project description: ", true);
    while (input.isEmpty()) {
      input = AzkabanHelper.consoleInput(console, "Enter non-empty Azkaban project description: ", false);
    }

    String response = AzkabanClient.createProject(azkProject.azkabanUrl, azkProject.azkabanProjName, input, sessionId);

    if (response.toLowerCase().contains("error")) {
      // Check if session has expired. If so, re-login.
      if (response.toLowerCase().contains("login")) {
        logger.lifecycle("\nPrevious Azkaban session expired. Please re-login.");
        createAzkabanProject(null);
        return;
      }

      // If response contains other than login error
      String msg = "Creating Azkaban project in ${azkProject.azkabanUrl} failed. Reason: " + new JSONObject(response).get("message");
      throw new GradleException(msg);
    }

    logger.lifecycle("Successfully created Azkaban project: ${azkProject.azkabanProjName}");
  }
}
