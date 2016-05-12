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

import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.tasks.TaskAction;

class DisallowLocalDependencyTask extends DefaultTask {
  boolean containsLocalDep = false;

  @TaskAction
  void findLocalDependencies() {
    def localDeps = project.getConfigurations()
      *.getAllDependencies()
      *.findAll { dep -> dep instanceof org.gradle.api.artifacts.FileCollectionDependency }
      .flatten();
    def nonBuiltLocalFileDeps = localDeps
      .collect { dep -> dep.resolve() }
      .flatten()
      .collect { fileDep -> fileDep.getAbsolutePath() }
      .findAll { fileDepPath -> fileDepPath.contains(project.rootDir.getAbsolutePath()) && !fileDepPath.contains(project.buildDir.getAbsolutePath()) }
      .unique();
    nonBuiltLocalFileDeps.each { dep -> println "Local file dependency: " + dep };

    containsLocalDep = nonBuiltLocalFileDeps.size() > 0;
    checkLocalDependencies(project);
  }

  /**
   * Returns whether this project contains a local dependency.
   */
  boolean containsLocalDependency() {
    return containsLocalDep;
  }

  /**
   * Handles the result of whether there is a local dependency or not.
   *
   * @param project The Gradle project
   */
  void checkLocalDependencies(Project project) {
    if (containsLocalDependency()) {
      project.logger.warn("WARN: should not include local file dependencies");
    }
  }
}
