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
package com.linkedin.gradle.liazkaban;

import com.linkedin.gradle.azkaban.AzkabanDslCompiler;
import com.linkedin.gradle.azkaban.AzkabanFlowStatusTask;
import com.linkedin.gradle.azkaban.AzkabanPlugin;
import com.linkedin.gradle.azkaban.AzkabanProject;

import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to the Azkaban Plugin.
 */
class LiAzkabanPlugin extends AzkabanPlugin {

  private static final String MESSAGE_URL = "https://gitli.corp.linkedin.com/hadoop-dev/azkaban-linkedin-files/raw/li-hadoop-plugin_message.txt";

  /**
   * Factory method to return the AzkabanFlowStatusTask class. Subclasses can override this method to
   * return their own AzkabanFlowStatusTask class.
   *
   * @return Class that implements the AzkabanFlowStatusTask
   */
  @Override
  Class<? extends AzkabanFlowStatusTask> getAzkabanFlowStatusTaskClass() {
    return LiAzkabanFlowStatusTask.class;
  }

  /**
   * Factory method to build a default AzkabanProject for use with the writePluginJson method. Can
   * be overridden by subclasses.
   * <p>
   * The LinkedIn override of this method sets a couple of properties to common LinkedIn values.
   *
   * @param project The Gradle project
   * @return The AzkabanProject object
   */
  @Override
  AzkabanProject makeDefaultAzkabanProject(Project project) {
    AzkabanProject azkabanProject = makeAzkabanProject();
    azkabanProject.azkabanProjName = "";
    azkabanProject.azkabanUrl = "https://ltx1-holdemaz01.grid.linkedin.com:8443";
    azkabanProject.azkabanUserName = System.getProperty("user.name");
    azkabanProject.azkabanValidatorAutoFix = "true";
    azkabanProject.azkabanZipTask = "";
    return azkabanProject;
  }

  /**
   * Factory method to build the Hadoop DSL compiler for Azkaban. Subclasses can override this
   * method to provide their own compiler.
   *
   * @param project The Gradle project
   * @return The AzkabanDslCompiler
   */
  @Override
  AzkabanDslCompiler makeCompiler(Project project) {
    return new LiAzkabanDslCompiler(project);
  }

  /**
   * Prints Linkedin specific message after upload to Azkaban completes, mostly to notify Azkaban users of
   * upcoming changes. Subclasses can override this method.
   */
  @Override
  void printUploadMessage() {
    try {
      String message = MESSAGE_URL.toURL().getText();
      if (!message.isEmpty() && message.length() > 0) {
        logger.lifecycle("\n"+message.trim());
      }
    } catch (Exception ex) {
      logger.error("Failed to fetch message. Error: " + ex.getMessage());
    }
  }
}
