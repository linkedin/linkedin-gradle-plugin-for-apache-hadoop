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
 * Base class for Hadoop DSL compilers.
 */
abstract class BaseCompiler extends BaseVisitor implements HadoopDslCompiler {
  /**
   * Directory location in which to place the generated files
   */
  String buildDirectory;

  /**
   * Keeps track of the current parent scope for the DSL element being built.
   */
  String parentScope;

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
    this.buildDirectory = null;
    this.parentScope = null;
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
   * @param extension The Hadoop DSL extension
   */
  @Override
  void compile(HadoopDslExtension extension) {
    visitExtension(extension);
  }

  /**
   * Builds the Hadoop DSL extension, which builds the workflows and properties that have been
   * specified in the DSL and added to the extension with the hadoop { ... } DSL syntax.
   *
   * @param hadoop The HadoopDslExtension object
   */
  @Override
  void visitExtension(HadoopDslExtension hadoop) {
    if (hadoop.buildDirectory == null || hadoop.buildDirectory.isEmpty()) {
      throw new IOException("You must set the buildDirectory property to use the Hadoop DSL. Use the hadoop { buildPath \"path\" } method to do this.");
    }

    this.buildDirectory = hadoop.buildDirectory;
    File file = new File(buildDirectory);

    if (file.exists()) {
      if (!file.isDirectory()) {
        throw new IOException("Directory ${this.buildDirectory} must specify a directory");
      }
    }
    else {
      // Try to make the directory automatically if we can. For git users, this is convenient as
      // git will not push empty directories in the repository (and users will often add the
      // generated job files to their gitignore).
      if (!file.mkdir()) {
        throw new IOException("Directory ${this.buildDirectory} does not exist and could not be created");
      }
    }

    if (hadoop.cleanFirst) {
      cleanBuildDirectory(file);
    }

    // Visit the workflows and properties under the extension
    super.visitExtension(hadoop);
  }

  /**
   * Builds the workflow.
   * <p>
   * NOTE: not all jobs in the workflow are built by default. Only those jobs that can be found
   * from a transitive walk starting from the jobs the workflow targets actually get built.
   *
   * @param workflow The workflow to build
   */
  @Override
  void visitWorkflow(Workflow workflow) {
    // Build the next parent scope for the jobs and properties in the workflow
    String oldParentScope = this.parentScope;
    this.parentScope = (this.parentScope == null) ? workflow.name : "${parentScope}_${workflow.name}";

    // Build the list of jobs to build for the workflow
    Set<Job> jobsToBuild = workflow.buildJobList();

    // Visit each job to build in the workflow
    jobsToBuild.each() { Job job ->
      visitJob(job);
    }

    // Visit each properties object in the workflow
    workflow.properties.each() { Properties props ->
      visitProperties(props);
    }

    // Restore the old parent scope
    this.parentScope = oldParentScope;
  }
}