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
package com.linkedin.gradle.hadoopdsl.checker;

import com.linkedin.gradle.hadoopdsl.BaseStaticChecker;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.Job;

import org.gradle.api.Project;

/**
 * The WorkflowJobChecker rule checks the following:
 * <ul>
 *   <li>WARN if a workflow does not declare any targets</li>
 *   <li>ERROR if a workflow declares a target that is not in the workflow</li>
 *   <li>ERROR if a job in a workflow depends on a target is not in the workflow. Note that this
 *       prevents you from doing things like depending on a job in global scope.</li>
 *   <li>WARN if a workflow does not contain any jobs or subflows</li>
 *   <li>WARN if a workflow declares flow dependency targets, but is not a subflow</li>
 *   <li>ERROR if a subflow declares itself as a flow dependency target</li>
 *   <li>ERROR if a subflow declares a flow dependency target that is not in its parent workflow</li>
 *   <li>WARN if a workflow contains jobs that will not be built</li>
 *   <li>WARN if a workflow contains subflows that will not be built</li>
 * </ul>
 */
class WorkflowJobChecker extends BaseStaticChecker {
  /**
   * Constructor for the WorkflowJobChecker.
   *
   * @param project The Gradle project
   */
  WorkflowJobChecker(Project project) {
    super(project);
  }

  @Override
  void visitWorkflow(Workflow workflow) {
    visitWorkflow(workflow, false);
  }

  void visitWorkflow(Workflow workflow, boolean subflow) {
    boolean workflowError = false;

    // WARN if a workflow does not declare any targets
    if (workflow.launchDependencies.isEmpty()) {
      project.logger.lifecycle("WorkflowJobChecker WARNING: Workflow ${workflow.name} does not declare any targets. Use the workflow targets method to declare the target jobs and subflows for the workflow.");
    }

    Map<String, Workflow> flowMap = workflow.buildFlowMap();
    Map<String, Job> jobMap = workflow.buildJobMap();

    // ERROR if a workflow declares a target that is not in the workflow
    workflow.launchDependencies.each() { String dependencyName ->
      if (!flowMap.containsKey(dependencyName) && !jobMap.containsKey(dependencyName)) {
        project.logger.lifecycle("WorkflowJobChecker ERROR: Workflow ${workflow.name} declares that it targets ${dependencyName}, but this target does not exist in the workflow.");
        workflowError = true;
      }
    }

    // ERROR if a job in the workflow depends on a target that is not in the workflow
    workflow.jobs.each() { Job job ->
      job.dependencyNames.each() { String dependencyName ->
        if (!flowMap.containsKey(dependencyName) && !jobMap.containsKey(dependencyName)) {
          project.logger.lifecycle("WorkflowJobChecker ERROR: Workflow ${workflow.name} contains the job ${job.name} that declares that target ${dependencyName}, but this target does not exist in the workflow.");
          workflowError = true;
        }
      }
    }

    // WARN if a workflow does not contain any jobs or subflows
    if (flowMap.isEmpty() && jobMap.isEmpty()) {
      project.logger.lifecycle("WorkflowJobChecker WARNING: Workflow ${workflow.name} does not contain any jobs or subflows. No job files will be built for this workflow.");
    }

    // WARN if a workflow declares flow dependency targets, but is not a subflow
    if (!workflow.parentDependencies.isEmpty() && subflow == false) {
      project.logger.lifecycle("WorkflowJobChecker WARNING: Workflow ${workflow.name} declares the flow dependency targets ${workflow.parentDependencies}, but is not itself a subflow. The flow dependencies will be ignored.");
    }

    // ERROR if a subflow declares itself as a flow dependency target
    workflow.workflows.each() { Workflow flow ->
      flow.parentDependencies.each { String dependencyName ->
        if (flow.name.equals(dependencyName)) {
          project.logger.lifecycle("WorkflowJobChecker ERROR: Workflow ${workflow.name} contains the subflow ${flow.name} that declares itself as a flow dependency target.");
          workflowError = true;
        }
      }
    }

    // ERROR if a subflow declares a flow dependency target that is not in its parent workflow
    workflow.workflows.each() { Workflow flow ->
      flow.parentDependencies.each { String dependencyName ->
        if (!flowMap.containsKey(dependencyName) && !jobMap.containsKey(dependencyName)) {
          project.logger.lifecycle("WorkflowJobChecker ERROR: Workflow ${workflow.name} contains the subflow ${flow.name} that declares the flow dependency target ${dependencyName}, but this target does not exist in the workflow.");
          workflowError = true;
        }
      }
    }

    // The workflow must have passed the previous static checks before calling buildWorkflowTargets
    if (workflowError == false) {

      // Identify the job and subflow targets that will be built
      workflow.buildWorkflowTargets(subflow);

      // WARN if a workflow contains jobs that will not be built
      jobMap.values().each() { Job job ->
        if (!workflow.jobsToBuild.contains(job)) {
          project.logger.lifecycle("WorkflowJobChecker WARNING: Workflow ${workflow.name} contains the job ${job.name} that is not targeted by the workflow and is not a transitive dependency of the workflow targets. This job will not be built.");
        }
      }

      // WARN if a workflow contains subflows that will not be built
      flowMap.values().each() { Workflow flow ->
        if (!workflow.flowsToBuild.contains(flow)) {
          project.logger.lifecycle("WorkflowJobChecker WARNING: Workflow ${workflow.name} contains the subflow ${flow.name} that is not targeted by the workflow and is not a transitive dependency of the workflow targets. This subflow will not be built.");
        }
      }
    }

    // Recursively check each of the subflows
    workflow.workflows.each() { Workflow flow ->
      visitWorkflow(flow, true);
    }

    // Indicate to the static checker whether or not we passed all the static checks
    foundError |= workflowError
  }
}