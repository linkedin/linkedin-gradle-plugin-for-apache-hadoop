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
import org.gradle.api.Project;
import org.gradle.api.Task;

/**
 * Linkedin specific customizations to the OoziePlugin.
 */
class LiOoziePlugin extends OoziePlugin {
  /**
   * Override the createOozieUploadTask method to make it of type LiOozieUploadTask instead of
   * OozieUploadTask.
   *
   * @param The Gradle project
   * @return The created task
   */
  @Override
  Task createOozieUploadTask(Project project) {
    return project.tasks.create(name: "oozieUpload", type: LiOozieUploadTask) { task ->
      description = "Uploads Oozie project folder to HDFS.";
      group = "Hadoop Plugin";

      doFirst{
        oozieProject = super.readOozieProject(project);
      }
    }
  }
}