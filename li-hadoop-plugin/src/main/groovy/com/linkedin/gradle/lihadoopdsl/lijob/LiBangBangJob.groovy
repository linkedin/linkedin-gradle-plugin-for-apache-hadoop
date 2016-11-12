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
package com.linkedin.gradle.lihadoopdsl.lijob;

/**
 * All the bangbang supported job types must implement this interface
 **/
interface LiBangBangJob {
  /**
   * Returns the dependency of compiler which was set using runsOn.
   *
   * @return The dependency of compiler set using runsOn
   */
  String getDependency();

  /**
   * Whether or not the generated script should be overwritten.
   *
   * @return Whether or not the generated script should be overwritten
   */
  boolean isOverwritten();

  /**
   * Specify whether to overwrite the generated Gradle file or not.
   *
   * @param overWrite Whether the generated file will be overwritten or not
   */
  void overwrite(boolean overWrite);

  /**
   * Specify the Gradle coordinates of the compiler to user for running the script.
   *
   * @param dependency The Gradle coordinates of the compiler to use
   */
  void runsOn(String dependency);
}