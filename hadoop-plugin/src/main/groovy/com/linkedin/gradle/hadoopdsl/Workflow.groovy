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

import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.LaunchJob;

import org.gradle.api.Project;

/**
 * A Workflow is a collection of jobs and properties that represent a logical workflow, i.e. jobs
 * with dependencies that form a DAG.
 * <p>
 * In the DSL, a workflow can be specified with:
 * <pre>
 *   workflow('workflowName') {
 *     // Declare jobs and properties
 *     ...
 *     // Declare the job targets for the workflow
 *     targets 'jobName1', 'jobName2'
 *   }
 * </pre>
 */
class Workflow extends BaseNamedScopeContainer {
  String name;

  // The names of the jobs targeted by the workflow.
  Set<String> launchJobDependencies;

  /**
   * Base constructor for a Workflow.
   *
   * @param name The workflow name
   * @param project The Gradle project
   */
  Workflow(String name, Project project) {
    this(name, project, null);
  }

  /**
   * Constructor for a Workflow given a parent scope.
   *
   * @param name The workflow name
   * @param project The Gradle project
   * @param parentScope The parent scope
   */
  Workflow(String name, Project project, NamedScope parentScope) {
    super(project, parentScope, name);
    this.launchJobDependencies = new LinkedHashSet<String>();
    this.name = name;
  }

  /**
   * Generate the list of jobs to build for this workflow by performing a transitive (breadth-
   * first) walk of the jobs in the workflow, starting from the target jobs for the workflow.
   * <p>
   * NOTE: this means that users can declare jobs in a workflow that are not built, if there is no
   * transitive path from the jobs the workflow targets to a declared job. This capability is by
   * design. In this case, the static checker will display a warning message.
   *
   * @return The list (as a LinkedHashSet) of jobs to build for the workflow
   */
  Set<Job> buildJobList() {
    Map<String, Job> jobMap = buildJobMap();
    Queue<Job> queue = new LinkedList<Job>();
    Set<Job> jobsToBuild = new LinkedHashSet<Job>();

    LaunchJob launchJob = factory.makeLaunchJob(name);
    launchJob.dependencyNames.addAll(launchJobDependencies);
    queue.add(launchJob);

    while (!queue.isEmpty()) {
      Job job = queue.remove();

      if (!jobsToBuild.contains(job)) {
        jobsToBuild.add(job);

        // Add the parents of this job to the queue in a breadth-first manner.
        for (String parentJob : job.dependencyNames) {
          queue.add(jobMap.get(parentJob));
        }
      }
    }
    return jobsToBuild;
  }

  /**
   * Helper function to return a map of the job names to jobs in the workflow. This does not
   * include the launch job that is implicitly added when the workflow is built.
   *
   * @return A map of the job names to jobs in the workflow
   */
  Map<String, Job> buildJobMap() {
    Map<String, Job> jobMap = new HashMap<String, Job>();

    jobs.each() { Job job ->
      jobMap.put(job.name, job);
    }

    return jobMap;
  }

  /**
   * Clones the workflow given its new parent scope.
   *
   * @param parentScope The new parent scope
   * @return The cloned workflow
   */
  @Override
  Workflow clone(NamedScope parentScope) {
    return clone(new Workflow(name, project, parentScope));
  }

  /**
   * Helper method to set the properties on a cloned workflow.
   *
   * @param workflow The workflow being cloned
   * @return The cloned workflow
   */
  Workflow clone(Workflow workflow) {
    workflow.launchJobDependencies.addAll(launchJobDependencies);
    return super.clone(workflow);
  }

  /**
   * Helper method to configure a Workflow in the DSL. Can be called by subclasses to configure
   * custom Workflow subclass types.
   *
   * @param workflow The workflow to configure
   * @param configure The configuration closure
   * @return The input workflow, which is now configured
   */
  @Override
  Workflow configureWorkflow(Workflow workflow, Closure configure) {
    throw new Exception("Workflows cannot yet be nested inside workflows. Support for this (and Azkaban embedded workflows) is coming soon.");
  }

  /**
   * @deprecated This method has been deprecated in favor of the targets method.
   *
   * DSL method that declares the jobs on which this workflow depends.
   * <p>
   * The depends method has been deprecated in favor of targets, so that workflow and job
   * dependencies can more easily visually distinguished.
   *
   * @param jobNames The list of job names on which this workflow depends
   */
  @Deprecated
  void depends(String... jobNames) {
    project.logger.lifecycle("The Workflow depends method is deprecated. Please use the targets method to declare that the workflow ${name} targets the jobs ${jobNames}.")
    launchJobDependencies.addAll(jobNames.toList());
  }

  /**
   * @deprecated This method has been deprecated in favor of the targets method.
   *
   * DSL method that declares the jobs this workflow executes.
   *
   * @param jobNames The list of job names this workflow executes
   */
  @Deprecated
  void executes(String... jobNames) {
    project.logger.lifecycle("The Workflow executes method is deprecated. Please use the targets method to declare that the workflow ${name} targets the jobs ${jobNames}.")
    launchJobDependencies.addAll(jobNames.toList());
  }

  /**
   * DSL method that declares the target jobs for the workflow.
   *
   * @param jobNames The list of target job for the workflow
   */
  void targets(String... jobNames) {
    launchJobDependencies.addAll(jobNames.toList());
  }

  /**
   * Returns a string representation of the workflow.
   *
   * @return A string representation of the workflow
   */
  String toString() {
    return "(Workflow: name = ${name})";
  }
}