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
package com.linkedin.gradle.azkaban;

import com.linkedin.gradle.hadoopdsl.BaseCompiler;
import com.linkedin.gradle.hadoopdsl.HadoopDslExtension;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Namespace;
import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.Job;

import org.gradle.api.Project;

/**
 * Hadoop DSL compiler for Azkaban.
 */
class AzkabanDslCompiler extends BaseCompiler {
  /**
   * Constructor for the AzkabanDslCompiler.
   *
   * @param project The Gradle project
   */
  AzkabanDslCompiler(Project project) {
    super(project);
  }

  /**
   * Cleans up generated files from the build directory.
   *
   * Clean up both .job/.properties as well as .flow/.project for easy Flow 2.0 upgrade/rollback.
   *
   * @param buildDirectoryFile Java File object representing the build directory
   */
  @Override
  void cleanBuildDirectory(File buildDirectoryFile) {
    buildDirectoryFile.eachFileRecurse(groovy.io.FileType.FILES) { f ->
      String fileName = f.getName().toLowerCase();
      if (fileName.endsWith(".job") || fileName.endsWith(".properties") ||
              fileName.endsWith(".flow") || fileName.endsWith(".project")) {
        f.delete();
      }
    }
  }

  /**
   * Selects the appropriate build directory for the given compiler.
   *
   * @param hadoop The HadoopDslExtension object
   * @return The build directory for this compiler
   */
  @Override
  String getBuildDirectory(HadoopDslExtension hadoop) {
    return hadoop.buildDirectory;
  }

  /**
   * Nothing needs to be done when AzkabanDslCompiler is created.
   */
  @Override
  void doOnceAfterCleaningBuildDirectory() {
  }

  /**
   * Builds the namespace. Creates a subdirectory for everything under the namespace.
   *
   * @param namespace The namespace to build
   */
  @Override
  void visitNamespace(Namespace namespace) {
    // Save the last parent directory information
    String oldParentDirectory = this.parentDirectory;

    // Set the new parent directory information
    this.parentDirectory = "${this.parentDirectory}/${namespace.name}";

    // Build a directory for the namespace
    File file = new File(this.parentDirectory);
    if (file.exists()) {
      if (!file.isDirectory()) {
        throw new IOException("Directory ${this.parentDirectory} for the namespace ${namespace.name} must specify a directory");
      }
    }
    else {
      // Try to make the directory automatically if we can. For git users, this is convenient as
      // git will not push empty directories in the repository (and users will often add the
      // generated job files to their gitignore).
      if (!file.mkdirs()) {
        throw new IOException("Directory ${this.parentDirectory} for the namespace ${namespace.name} could not be created");
      }
    }

    // Visit the elements in the namespace
    visitScopeContainer(namespace);

    // Restore the last parent directory
    this.parentDirectory = oldParentDirectory;
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
    visitWorkflow(workflow, false);
  }

  /**
   * Builds the workflow. If the flow is a subflow, it will be constructed with a root FlowJob.
   * <p>
   * NOTE: not all jobs in the workflow are built by default. Only those jobs that can be found
   * from a transitive walk starting from the jobs the workflow targets actually get built.
   *
   * @param workflow The workflow to build
   * @param subflow Whether or not the workflow is a subflow
   */
  void visitWorkflow(Workflow workflow, boolean subflow) {
    // Save the last scope information
    NamedScope oldParentScope = this.parentScope;
    String oldParentDirectory = this.parentDirectory;

    // Set the new parent scope information
    this.parentScope = workflow.scope;
    this.parentDirectory = "${this.parentDirectory}/${workflow.name}";

    // Build a directory for the workflow
    File file = new File(this.parentDirectory);
    if (file.exists()) {
      if (!file.isDirectory()) {
        throw new IOException("Directory ${this.parentDirectory} for the workflow ${workflow.name} must specify a directory");
      }
    }
    else {
      // Try to make the directory automatically if we can. For git users, this is convenient as
      // git will not push empty directories in the repository (and users will often add the
      // generated job files to their gitignore).
      if (!file.mkdirs()) {
        throw new IOException("Directory ${this.parentDirectory} for the workflow ${workflow.name} could not be created");
      }
    }

    // Build the list of jobs and subflows to build for the workflow
    workflow.buildWorkflowTargets(subflow);

    workflow.jobsToBuild.each { Job job ->
      visitJobToBuild(job);
    }

    // Visit each properties object in the workflow
    workflow.properties.each { Properties props ->
      visitProperties(props);
    }

    // Visit each subflow to build in the workflow
    workflow.flowsToBuild.each { Workflow flow ->
      visitWorkflow(flow, true);
    }

    // Visit each child namespace in the workflow
    workflow.namespaces.each { Namespace namespace ->
      visitNamespace(namespace);
    }

    // Restore the last parent scope
    this.parentScope = oldParentScope;
    this.parentDirectory = oldParentDirectory;
  }

  /**
   * Builds a properties file.
   *
   * @param props The Properties object to build
   */
  @Override
  void visitProperties(Properties props) {
    Map<String, String> allProperties = props.buildProperties(this.parentScope);
    if (allProperties.size() == 0) {
      return;
    }

    String fileName = props.buildFileName(this.parentScope);
    File file = new File(this.parentDirectory, "${fileName}.properties");
    List<String> sortedKeys = sortPropertiesToBuild(allProperties.keySet());

    file.withWriter { out ->
      out.writeLine("# This file generated from the Hadoop DSL. Do not edit by hand.");
      sortedKeys.each { key ->
        out.writeLine("${key}=${allProperties.get(key)}");
      }
    }

    // Set to read-only to remind people that they should not be editing the job files.
    file.setWritable(false);
  }

  /**
   * Builds a job that has been found on a transitive walk starting from the jobs the workflow
   * targets. These are the jobs that should actually be built by the compiler.
   *
   * @param job The job to build
   */
  void visitJobToBuild(Job job) {
    Map<String, String> allProperties = job.buildProperties(this.parentScope);
    if (allProperties.size() == 0) {
      return;
    }

    String fileName = job.buildFileName(this.parentScope);
    File file = new File(this.parentDirectory, "${fileName}.job");
    List<String> sortedKeys = sortPropertiesToBuild(allProperties.keySet());

    file.withWriter { out ->
      out.writeLine("# This file generated from the Hadoop DSL. Do not edit by hand.");
      sortedKeys.each { key ->
        out.writeLine("${key}=${allProperties.get(key)}");
      }
    }

    // Set to read-only to remind people that they should not be editing the job files.
    file.setWritable(false);
  }

  /**
   * Helper method to sort a list of properties from a Job or a Properties object into a
   * standardized, sorted order that will make reading job and property files easy.
   *
   * @param propertyNames The property names for a Job or Properties object
   * @return The property names in a standardized, sorted order
   */
  static List<String> sortPropertiesToBuild(Set<String> propertyNames) {
    // First, sort the properties alphabetically.
    List<String> propertyList = new ArrayList<String>(propertyNames);
    Collections.sort(propertyList);

    List<String> sortedKeys = new ArrayList<String>(propertyList.size());

    // List the job type and dependencies first if they exist.
    if (propertyList.remove("type")) {
      sortedKeys.add("type");
    }

    if (propertyList.remove("dependencies")) {
      sortedKeys.add("dependencies");
    }

    // Then add the rest of the keys to the final list of sorted keys.
    sortedKeys.addAll(propertyList);
    return sortedKeys;
  }
}
