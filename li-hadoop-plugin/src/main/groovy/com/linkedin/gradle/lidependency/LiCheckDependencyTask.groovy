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
package com.linkedin.gradle.lidependency;

import com.linkedin.gradle.dependency.CheckDependencyTask;
import com.linkedin.gradle.dependency.DependencyPattern;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;

class LiCheckDependencyTask extends CheckDependencyTask {

  /**
   * This method returns the xml file which contains the information about the dependency patterns.
   * Subclasses can override this method to return their own dependencyXml file.
   * @param project The gradle project
   * @return The path of the xml file which contains dependencyPattern information
   */
  @Override
  String getDependencyPatternFile(Project project) {
    return "dependencyPattern.json";
  }

  /**
   * Method to handle warning. Subclasses can override this method to handle warning in their own way.
   * @param project The Gradle project
   * @param dependency The dependency to check
   * @param matchedPattern The dependencyPattern against which dependency should be checked.
   */
  @Override
  void handleWarn(Project project, Dependency dependency, DependencyPattern matchedPattern) {
    project.logger.warn("WARN: Invalid dependency ($dependency.group:$dependency.name:$dependency.version), ${matchedPattern.getMessage()}");
  }

  /**
   * Returns the set of all the dependencies which are defined in any visible configuration
   * extending from runtime.
   * @param project The gradle project
   * @return dependencies of the project which are defined in visible configurations extending runtime.
   */
  @Override
  Set<Dependency> getDependencies(Project project) {
    Set<Dependency> dependencies = new HashSet<Dependency>();
    project.getConfigurations().each {
      configuration ->
        if(configuration.hierarchy.contains(project.getConfigurations().findByName("runtime")) || configuration.hierarchy.contains(project.getConfigurations().findByName("hadoopRuntime")) && configuration.visible) {
          dependencies.addAll(configuration.getAllDependencies());
        }
    }
    return dependencies;
  }
}
