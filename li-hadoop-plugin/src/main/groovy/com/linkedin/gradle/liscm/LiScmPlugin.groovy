/*
 * Copyright 2014 LinkedIn Corp.
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
package com.linkedin.gradle.liscm;

import com.linkedin.gradle.scm.ScmPlugin;

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.bundling.Zip

/**
 * LinkedIn-specific customizations to the SCM Plugin.
 */
class LiScmPlugin extends ScmPlugin {
  /**
   * Applies the ScmPlugin.
   * <p>
   * In the LiScmPlugin, any azkabanZip tasks (from the li-azkaban2 plugin in RUM) are made to be
   * dependent on the buildScmMetadata task, and to include the metadata JSON file.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    super.apply(project);

    // After we apply the SCM plugin, grab the buildScmMetadata task.
    Task buildScmTask = project.tasks["buildScmMetadata"];

    // Look up any li-azkaban2 zip tasks (e.g. "azkabanZip" or "azkabanMagicZip") and make them
    // depend on the buildScmTask (and add the buildMetadata.json file to the zip).
    String zipRegex = "azkaban(.*)Zip";

    project.tasks.each { Task task ->
      if (task.getName().matches(zipRegex)) {
        Zip zipTask = (Zip)task;
        zipTask.dependsOn(buildScmTask);
        zipTask.from(getMetadataFilePath(project));
      }
    }
  }
}