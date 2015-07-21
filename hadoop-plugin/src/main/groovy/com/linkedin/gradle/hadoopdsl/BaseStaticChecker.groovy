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
package com.linkedin.gradle.hadoopdsl;

import org.gradle.api.Project;

/**
 * Base class for static checking rules.
 */
abstract class BaseStaticChecker extends BaseVisitor implements StaticChecker {
  /**
   * Member variable that tracks whether or not the checker found an error or not. Subclasses
   * set this variable when the check the DSL.
   */
  boolean foundError = false;

  /**
   * Member variable for the Gradle project so we can access the logger.
   */
  Project project;

  /**
   * Base constructor for the BaseStaticChecker.
   *
   * @param project The Gradle project
   */
  BaseStaticChecker(Project project) {
    this.project = project;
  }

  /**
   * Helper function to build a message describing a cycle.
   *
   * @param cycleNames The (LinkedHashSet) set of names that form a cycle
   * @param startCycle The name of the element that starts the cycle
   */
  String buildCyclesText(Set<String> cycleNames, String startCycle) {
    List<String> cycleList = new ArrayList<String>(cycleNames);
    cycleList.add(startCycle);
    return cycleList.join("->");
  }

  /**
   * Makes this static check on the DSL.
   *
   * @param plugin The Hadoop DSL plugin
   */
  @Override
  void check(HadoopDslPlugin plugin) {
    visitPlugin(plugin);
  }

  /**
   * Asks the checker rule whether or not the check failed.
   *
   * @return Whether or not the check failed
   */
  @Override
  boolean failedCheck() {
    return foundError;
  }

  @Override
  void visitPlugin(HadoopDslPlugin plugin) {
    // During static checking, we specifically only visit DSL elements nested under the extension,
    // so that users have the flexibility to write down "template" DSL elements in global scope
    // that are not valid, but fill them out correctly when cloning them under the extension.
    visitExtension(plugin.extension);
  }
}