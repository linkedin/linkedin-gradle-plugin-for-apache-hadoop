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
   * Makes this static check on the DSL.
   *
   * @param extension The Hadoop DSL extension
   */
  @Override
  void checkHadoopDsl(HadoopDslExtension extension) {
    visitExtension(extension);
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
}