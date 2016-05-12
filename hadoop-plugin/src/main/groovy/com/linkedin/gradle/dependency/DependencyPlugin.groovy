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

/**
 * Plugin that implements checks against project dependencies.
 */
class DependencyPlugin implements Plugin<Project> {

  // By default disable the dependency check. Subclasses can set this variable to true to enable it.
  private boolean enableDependencyCheck = false;

  @Override
  void apply(Project project) {
    if (isDependencyCheckEnabled()) {
      createCheckForDependenciesTask(project);
      createDisallowLocalDependenciesTask(project);
    }
  }

  /**
   * Method to check if the dependency check is enabled.
   *
   * @return Whether or not the dependency check is enabled
   */
  boolean isDependencyCheckEnabled() {
    return enableDependencyCheck;
  }

  /**
   * Method to create the checkDependencies task.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createCheckForDependenciesTask(Project project) {
    return project.tasks.create(name: "checkDependencies", type: getCheckDependencyTask()) {
      description = "Task to help in controlling and monitoring the dependencies used in the project"
      group = "Hadoop Plugin";
    }
  }

  /**
   * Method to create the disallowLocalDependencies task.
   *
   * @param project The Gradle project
   * @return The created task
   */
  Task createDisallowLocalDependenciesTask(Project project) {
    return project.tasks.create(name: "disallowLocalDependencies", type: getDisallowLocalDependencyTask()) {
      description = "Task to disallow users from checking in local dependencies";
      group = "Hadoop Plugin";
    }
  }

  /**
   * Factory method to return the CheckDependencyTask class. Subclasses can override this method to
   * return their own CheckDependencyTask class.
   *
   * @return Class that extends the CheckDependencyTask class
   */
  Class<? extends CheckDependencyTask> getCheckDependencyTask() {
    return CheckDependencyTask.class;
  }

  /**
   * Factory method to return the DisallowLocalDependencyTask class. Subclasses can override this
   * method to return their own DisallowLocalDependencyTask class.
   *
   * @return Class that extends the DisallowLocalDependencyTask class
   */
  Class<? extends DisallowLocalDependencyTask> getDisallowLocalDependencyTask() {
    return DisallowLocalDependencyTask.class;
  }
}
