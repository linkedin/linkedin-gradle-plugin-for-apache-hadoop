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
package com.linkedin.gradle.lioozie;

import com.linkedin.gradle.oozie.OoziePlugin;
import com.linkedin.gradle.oozie.OozieProject
import org.gradle.api.Project;
import org.gradle.api.Task;

/**
 * Linkedin specific customizations to the OoziePlugin.
 */
class LiOoziePlugin extends OoziePlugin {
  /**
   * Override the createUploadTask method to make it of type LiOozieUploadTask instead of
   * OozieUploadTask.
   *
   * @param The Gradle project
   * @return The created task
   */
  @Override
  Task createUploadTask(Project project) {
    return project.tasks.create(name: "oozieUpload", type: LiOozieUploadTask) { task ->
      description = "Uploads Oozie project folder to HDFS.";
      group = "Hadoop Plugin";

      doFirst{
        oozieProject = super.readOozieProject(project);
      }
    }
  }

  /**
   * Factory method to build a default OozieProject for use with the writePluginJson method. Can be
   * overridden by subclasses.
   * <p>
   * The LinkedIn override of this method sets a couple of properties to common LinkedIn values.
   *
   * @param project The Gradle project
   * @return The OozieProject object
   */
  @Override
  OozieProject makeDefaultOozieProject(Project project) {
    OozieProject oozieProject = makeOozieProject();
    oozieProject.clusterURI = "webhdfs://eat1-nertznn01.grid.linkedin.com:50070";
    oozieProject.oozieZipTask = "";
    oozieProject.projectName = "";
    oozieProject.uploadPath = "/user/${System.getProperty('user.name')}";
    return oozieProject;
  }
}