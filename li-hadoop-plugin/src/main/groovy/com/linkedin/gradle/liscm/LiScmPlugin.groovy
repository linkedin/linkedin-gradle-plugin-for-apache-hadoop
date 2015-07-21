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
    // Enable users to skip the plugin
    if (project.hasProperty("disableScmPlugin")) {
      println("ScmPlugin disabled");
      return;
    }

    super.apply(project);

    if (!project.hasProperty("disableLiAzkabanPlugin2")) {
      // After we apply the SCM plugin, grab the buildScmMetadata task.
      Task buildScmTask = project.tasks["buildScmMetadata"];

      // After we apply the SCM plugin, grab the root project's buildSourceZip task.
      Task buildSrcTask = project.getRootProject().tasks["buildSourceZip"];

      // Look up any li-azkaban2 zip tasks (e.g. "azkabanZip" or "azkabanMagicZip") and make them
      // depend on the buildScmTask (and add the buildMetadata.json file to the zip).
      String zipRegex = "azkaban(.*)Zip";

      project.tasks.each { Task task ->
        if (task.getName().matches(zipRegex)) {
          Zip zipTask = (Zip)task;
          zipTask.dependsOn(buildScmTask);
          zipTask.dependsOn(buildSrcTask);
          zipTask.from(getMetadataFilePath(project));
          zipTask.from(getSourceZipFilePath(project));
        }
      }
    }
  }

  /**
   * Builds a list of relative paths to exclude from the sources zip for the project.
   *
   * @param project The Gradle project
   * @return The list of relative paths to exclude from the sources zip
   */
  @Override
  List<String> buildExcludeList(Project project) {
    List<String> excludeList = super.buildExcludeList(project);
    excludeList.add("ligradle");
    return excludeList;
  }
}