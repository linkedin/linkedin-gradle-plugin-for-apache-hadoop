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

package com.linkedin.gradle.dependency;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;

class DependencyPlugin implements Plugin<Project> {

  // by default disable dependency check. Subclasses can set this variable to true to enable dependencyCheck
  private boolean enableDependencyCheck = false;

  @Override
  void apply(Project project) {
    if(isDependencyCheckEnabled()) {
      createCheckForDependenciesTask(project);
    }
  }

  /**
   * Method to check if dependency check is enabled
   * @return true if dependency check is enabled else false.
   */
  boolean isDependencyCheckEnabled() {
    return enableDependencyCheck;
  }

  /**
   * Method to create the task checkDependencies
   * @param project The gradle project
   * @return The created task.
   */
  Task createCheckForDependenciesTask(Project project) {
    return project.tasks.create(name: "checkDependencies", type: getCheckDependencyTask()) {
      description = "Task to help in controlling and monitoring the dependencies used in the project"
      group = "Hadoop Plugin";
    }
  }

  /**
   * Factory method to return the CheckDependencyTask class. Subclasses can override this method to
   * return their own CheckDependencyTask class.
   *
   * @return Class that implements the CheckDependencyTask
   */
  Class<? extends CheckDependencyTask> getCheckDependencyTask() {
    return CheckDependencyTask.class;
  }
}
