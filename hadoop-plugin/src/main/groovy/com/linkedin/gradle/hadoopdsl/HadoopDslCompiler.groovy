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

/**
 * Interface for Hadoop DSL compilers.
 */
interface HadoopDslCompiler extends Visitor {
  /**
   * Cleans up generated files from the build directory.
   *
   * @param buildDirectoryFile Java File object representing the build directory
   */
  void cleanBuildDirectory(File buildDirectoryFile);

  /**
   * Builds the Hadoop DSL.
   *
   * @param plugin The Hadoop DSL plugin
   */
  void compile(HadoopDslPlugin plugin);

  /**
   * Selects the appropriate build directory for the given compiler.
   *
   * @param hadoop The HadoopDslExtension object
   * @return The build directory for this compiler
   */
  String getBuildDirectory(HadoopDslExtension hadoop);
}
