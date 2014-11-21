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

  @Override
  void visitPlugin(HadoopDslPlugin plugin) {
    // Compilation only considers DSL elements nested under the extension.
    visitExtension(plugin.extension);
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
    // Save the last scope information
    NamedScope oldParentScope = this.parentScope;
    String oldParentScopeName = this.parentScopeName;

    // Don't bother prefixing all scope names with "hadoop" even though everything being compiled
    // is under hadoop scope.
    boolean hadoopScope = this.parentScope == extension.scope;

    // Set the new parent scope
    this.parentScopeName = (this.parentScopeName == null || hadoopScope) ? workflow.name : "${parentScopeName}_${workflow.name}";
    this.parentScope = workflow.scope;

    // Build the list of jobs to build for the workflow
    Set<Job> jobsToBuild = workflow.buildJobList();

    // Visit each job to build in the workflow
    jobsToBuild.each() { Job job ->
      visitJobToBuild(job);
    }

    // Visit each properties object in the workflow
    workflow.properties.each() { Properties props ->
      visitProperties(props);
    }

    // Visit each embedded workflow in the workflow
    workflow.workflows.each() { Workflow childWorkflow ->
      visitWorkflow(childWorkflow);
    }

    // Restore the last parent scope
    this.parentScope = oldParentScope;
    this.parentScopeName = oldParentScopeName;
  }

  /**
   * Builds a job that has been found on a transitive walk starting from the jobs the workflow
   * targets. These are the jobs that should actually be built by the compiler.
   *
   * @param The job to build
   */
  abstract void visitJobToBuild(Job job);
}
