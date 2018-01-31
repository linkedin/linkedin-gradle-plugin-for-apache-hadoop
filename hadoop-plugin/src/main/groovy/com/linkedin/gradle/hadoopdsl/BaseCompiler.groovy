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
 * Base class for Hadoop DSL compilers.
 */
abstract class BaseCompiler extends BaseVisitor implements HadoopDslCompiler {
  /**
   * Current build directory location in which to place the generated files.
   */
  String parentDirectory;

  /**
   * Member variable for the Gradle project so we can access the logger.
   */
  Project project;

  /**
   * Base constructor for the BaseCompiler.
   *
   * @param project The Gradle project
   */
  BaseCompiler(Project project) {
    this.project = project;
  }

  /**
   * Cleans up generated files from the build directory.
   *
   * @param buildDirectoryFile Java File object representing the build directory
   */
  @Override
  abstract void cleanBuildDirectory(File buildDirectoryFile);

  /**
   * Builds the Hadoop DSL.
   *
   * @param plugin The Hadoop DSL plugin
   */
  @Override
  void compile(HadoopDslPlugin plugin) {
    visitPlugin(plugin);
  }

  /**
   * Selects the appropriate build directory for the given compiler.
   *
   * @param hadoop The HadoopDslExtension object
   * @return The build directory for this compiler
   */
  @Override
  abstract String getBuildDirectory(HadoopDslExtension hadoop);

  @Override
  void visitPlugin(HadoopDslPlugin plugin) {
    // Compilation only considers DSL elements nested under the extension.
    visitExtension(plugin.extension);
  }

  /**
   * Override this with a method that's supposed to run only once immediately after the build
   * directory is cleaned.
   */
  abstract void doOnceAfterCleaningBuildDirectory();

  /**
   * Builds the Hadoop DSL extension, which builds the workflows and properties that have been
   * specified in the DSL and added to the extension with the hadoop { ... } DSL syntax.
   *
   * @param hadoop The HadoopDslExtension object
   */
  @Override
  void visitExtension(HadoopDslExtension hadoop) {
    String buildDirectory = getBuildDirectory(hadoop);

    if (buildDirectory == null || buildDirectory.isEmpty()) {
      throw new IOException("You must set the build directory to use the Hadoop DSL. For Azkaban, use hadoop { buildPath \"path\" }.");
    }

    this.parentDirectory = buildDirectory;
    File file = new File(this.parentDirectory);

    if (file.exists()) {
      if (!file.isDirectory()) {
        throw new IOException("Directory ${this.parentDirectory} must specify a directory");
      }
    }
    else {
      // Try to make the directory automatically if we can. For git users, this is convenient as
      // git will not push empty directories in the repository (and users will often add the
      // generated job files to their gitignore).
      if (!file.mkdirs()) {
        throw new IOException("Directory ${this.parentDirectory} could not be created");
      }
    }

    if (hadoop.cleanFirst) {
      cleanBuildDirectory(file);
    }

    doOnceAfterCleaningBuildDirectory();

    // Visit the workflows and properties under the extension
    super.visitExtension(hadoop);
  }
}