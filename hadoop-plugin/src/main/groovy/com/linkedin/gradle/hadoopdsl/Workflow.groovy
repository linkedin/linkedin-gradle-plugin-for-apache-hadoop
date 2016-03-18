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

import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.LaunchJob;
import com.linkedin.gradle.hadoopdsl.job.StartJob

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
 * <p>
 * You can also declare workflows inside of workflows ("subflows") and declare them as targets and
 * dependencies:
 * <pre>
 *   workflow('workflow1') { ... }
 *
 *   workflow('workflow2') {
 *     job('job1') { ... }
 *
 *     // Create a subflow by cloning a workflow from another scope into this workflow
 *     addWorkflow('workflow1', 'subflow1') {
 *       flowDepends 'job1'       // Subflows can depend on jobs inside workflow2
 *     }
 *
 *     // Declare an inline subflow
 *     workflow('subflow2') {
 *       flowDepends 'subflow1'   // Subflows can depend on other subflows inside workflow2
 *     }
 *
 *     job('job2') {
 *       depends 'subflow1'       // Jobs can depend on subflows
 *     }
 *
 *     targets 'job2', 'subflow2' // Workflows can depend on subflows
 *   }
 * </pre>
 */
class Workflow extends BaseNamedScopeContainer {
  String name;

  // Helper member variables to hold the jobs and subflows to build for this workflow. These member
  // variables are set by calling the method buildWorkflowTargets.
  Set<Workflow> flowsToBuild;
  Set<Job> jobsToBuild;

  // The names of target dependencies for the workflow
  Set<String> launchDependencies;

  // For a subflow, the names of targets dependencies in the parent workflow upon which this
  // workflow depends.
  Set<String> parentDependencies;

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
    this.flowsToBuild = null;
    this.jobsToBuild = null;
    this.launchDependencies = new LinkedHashSet<String>();
    this.name = name;
    this.parentDependencies = new LinkedHashSet<String>();
  }

  /**
   * Generate the list of jobs and subflows to build for this workflow by performing a transitive
   * (breadth-first) walk of the build targets in the workflow, starting from the declared workflow
   * targets.
   * <p>
   * NOTE: this means that users can declare jobs and subflows in a workflow that are not built, if
   * there is no transitive path from the workflow targets to a declared job or subflow. This
   * capability is by design. In this case, the static checker will display a warning message.
   * <p>
   * For a subflow, this generates an additional placeholder job at the root of the subflow that
   * will depend upon the declared parent dependencies.
   */
  void buildWorkflowTargets(boolean subflow) {
    // Both the static checker and compiler need to build the workflow targets, but don't build
    // them more than once or you will add the LaunchJob and StartJob to the jobs again.
    if (flowsToBuild != null && jobsToBuild != null) {
      return;
    }

    // Initialize the flowsToBuild and jobsToBuild properties for the workflow.
    flowsToBuild = new LinkedHashSet<Workflow>();
    jobsToBuild = new LinkedHashSet<Job>();

    // Make sure the user did not declare something in the workflow with the launch job name.
    if (scope.thisLevel.containsKey(name)) {
      throw new Exception("An object with the name ${name} is already declared in the workflow ${name}. Do not use this name as it will be used for the workflow launch job.");
    }

    // First, build the launch job for the workflow and it to the list of jobs.
    LaunchJob launchJob = factory.makeLaunchJob(name);
    launchJob.dependencyNames.addAll(launchDependencies);
    scope.bind(launchJob.name, launchJob);
    jobs.add(launchJob);

    if (subflow) {
      // If this is a subflow, build a placeholder startJob at the root of the subflow.
      StartJob startJob = factory.makeStartJob("_start");
      startJob.flowDependencyNames.addAll(parentDependencies);

      // Now, make the actual root jobs for the subflow depend on the startJob.
      findRootJobs().each { Job job ->
        job.dependencyNames.add(startJob.name);
      }

      // If this workflow is a subflow, look at each of its subflows. If they do not declare any
      // parent dependencies, add a parent dependency on the start job so that all target paths in
      // this subflow end up connected to the start job. We must do this before we recursively
      // build the subflows.
      workflows.each { Workflow flow ->
        if (flow.parentDependencies.size() == 0) {
          flow.parentDependencies.add(startJob.name);
        }
      }

      // Then add the startJob to the jobs so it will get built.
      scope.bind(startJob.name, startJob);
      jobs.add(startJob);
    }

    Map<String, Workflow> flowMap = buildFlowMap();
    Map<String, Job> jobMap = buildJobMap();

    Queue<String> queue = new LinkedList<String>();
    queue.add(launchJob.name);

    while (!queue.isEmpty()) {
      String targetName = queue.remove();

      // Check if the target is a job.
      if (jobMap.containsKey(targetName)) {
        Job job = jobMap.get(targetName);

        if (!jobsToBuild.contains(job)) {
          jobsToBuild.add(job);

          // Add the parents of this job to the queue in a breadth-first manner.
          queue.addAll(job.dependencyNames);
        }
      }
      else {
        // Else the target must be a subflow.
        Workflow workflow = flowMap.get(targetName);

        if (!flowsToBuild.contains(workflow)) {
          flowsToBuild.add(workflow);

          // Add the parents of the subflow's root job to the queue in a breadth-first manner.
          queue.addAll(workflow.parentDependencies);
        }
      }
    }
  }

  /**
   * Helper function to return a map of workflow names to workflows belonging to this workflow.
   *
   * @return A map of workflow names to workflows that belongs to this workflow
   */
  Map<String, Workflow> buildFlowMap() {
    Map<String, Workflow> flowMap = new HashMap<String, Workflow>();

    workflows.each() { Workflow flow ->
      flowMap.put(flow.name, flow);
    }

    return flowMap;
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
    workflow.launchDependencies.addAll(launchDependencies);
    workflow.parentDependencies.addAll(parentDependencies);
    return super.clone(workflow);
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
    launchDependencies.addAll(jobNames.toList());
  }

  /**
   * @deprecated This method has been deprecated in favor of the targets method.
   *
   * DSL method that declares the jobs this workflow executes.
   *
   * @param jobNames The list of job names this workflow executes
   */
  @Deprecated
  void executes(String... targetNames) {
    project.logger.lifecycle("The Workflow executes method is deprecated. Please use the targets method to declare that the workflow ${name} targets the jobs ${jobNames}.")
    launchDependencies.addAll(jobNames.toList());
  }

  /**
   * Helper method to find the jobs that are at the root of this workflow, i.e. jobs with no
   * further dependencies.
   *
   * @return A set of the root jobs for this workflow
   */
  Set<Job> findRootJobs() {
    Set<Job> rootJobs = new HashSet<Job>();

    jobs.each { Job job ->
      if (job.dependencyNames.isEmpty()) {
        rootJobs.add(job);
      }
    }

    return rootJobs;
  }

  /**
   * DSL method for a subflow that declares the targets in the parent workflow upon which this
   * workflow depends.
   *
   * @param targetNames The list of targets in the parent workflow upon on which this subflow depends
   */
  void flowDepends(String... targetNames) {
    flowDepends(false, targetNames.toList());
  }

  /**
   * DSL method for a subflow that declares the targets in the parent workflow upon which this
   * workflow depends.
   *
   * @param clear Whether or not to clear the previously declared parent targets
   * @param targetNames The list of targets in the parent workflow upon on which this subflow depends
   */
  void flowDepends(boolean clear, List<String> targetNames) {
    if (clear) {
      parentDependencies.clear();
    }
    parentDependencies.addAll(targetNames);
  }

  /**
   * DSL method that declares the targets for the workflow.
   *
   * @param targetNames The variable-length targets for the workflow
   */
  void targets(String... targetNames) {
    targets(targetNames.toList());
  }

  /**
   * DSL method that declares the targets for the workflow.
   *
   * @param targetNames The list of targets for the workflow
   */
  void targets(List<String> targetNames) {
    targets(false, targetNames);
  }

  /**
   * DSL method that declares the targets for the workflow.
   *
   * @param clear Whether or not to clear the previously declared targets
   * @param targetNames The list of targets for the workflow
   */
  void targets(boolean clear, List<String> targetNames) {
    if (clear) {
      launchDependencies.clear();
    }
    launchDependencies.addAll(targetNames);
  }

  /**
   * DSL method that declares the targets for the workflow.
   *
   * @param args Args whose optional key 'clear' specifies whether or not to clear the previously declared targets and
   *                  required key 'targetNames' specifies the list of targets for the workflow
   */
  void targets(Map args) {
    boolean clear = args.containsKey("clear") ? args["clear"] : false;
    List<String> targetNames = args["targetNames"];
    targets(clear, targetNames);
  }

  /**
   * Returns a string representation of the workflow.
   *
   * @return A string representation of the workflow
   */
  @Override
  String toString() {
    return "(Workflow: name = ${name})";
  }
}