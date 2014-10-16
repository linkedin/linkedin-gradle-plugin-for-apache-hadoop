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
package com.linkedin.gradle.azkaban.checker;

import com.linkedin.gradle.azkaban.AzkabanJob;
import com.linkedin.gradle.azkaban.AzkabanWorkflow;
import com.linkedin.gradle.azkaban.BaseStaticChecker;

import org.gradle.api.Project;

/**
 * The WorkflowJobChecker rule checks the following:
 * <ul>
 *   <li>WARN if a default workflow declares jobs that it executes</li>
 *   <li>WARN if a workflow does not declare any jobs that it executes</li>
 *   <li>ERROR if a workflow declares that it executes a job that is not in the workflow</li>
 *   <li>ERROR if a job in a workflow depends on a job that is not in the workflow. Note that this
 *       prevents you from doing things like depending on a job in global scope.</li>
 *   <li>WARN if a workflow does not contain any jobs</li>
 *   <li>WARN if a workflow contains jobs that will not be built</li>
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
  void visitAzkabanWorkflow(AzkabanWorkflow workflow) {
    if ("default".equals(workflow.name)) {
      // WARN if a default workflow declares jobs that it executes
      if (!workflow.launchJobDependencies.isEmpty()) {
        project.logger.lifecycle("WorkflowJobChecker WARNING: The default workflow ${workflow.name} declares that it executes jobs. These will be ignored as the default workflow builds all jobs declared in the workflow.");
      }
    }
    else {
      // WARN if a workflow does not declare any jobs that it executes
      if (workflow.launchJobDependencies.isEmpty()) {
        project.logger.lifecycle("WorkflowJobChecker WARNING: Workflow ${workflow.name} does not execute any jobs. No job files will be built for this workflow. Use the workflow executes method to declare the jobs executed by the workflow.");
      }
    }

    // Build a map from the jobs in the workflow that maps the job name to the job.
    Map<String, AzkabanJob> jobMap = workflow.buildJobMap();

    // ERROR if a workflow declares that it executes a job that is not in the workflow
    workflow.launchJobDependencies.each() { String launchJobName ->
      if (!jobMap.containsKey(launchJobName)) {
        project.logger.lifecycle("WorkflowJobChecker ERROR: Workflow ${workflow.name} declares that it executes the job ${launchJobName}, but this job does not exist in the workflow");
        foundError = true;
      }
    }

    // ERROR if a job in the workflow depends on a job that is not in the workflow
    workflow.jobs.each() { AzkabanJob job ->
      job.dependencyNames.each() { String dependencyName ->
        if (!jobMap.containsKey(dependencyName)) {
          project.logger.lifecycle("WorkflowJobChecker ERROR: Workflow ${workflow.name} contains the job ${job.name} that declares that it depends on job ${dependencyName}, but this job is not contained in the workflow");
          foundError = true;
        }
      }
    }

    // WARN if a workflow does not contain any jobs
    if (jobMap.isEmpty()) {
      project.logger.lifecycle("WorkflowJobChecker WARNING: Workflow ${workflow.name} does not contain any jobs. No job files will be built for this workflow.");
    }

    if (!"default".equals(workflow.name)) {
      // Walk the workflow and job dependencies to build the set of job names that will be built.
      Set<AzkabanJob> jobsToBuild = workflow.buildJobList();

      // WARN if a workflow contains jobs that will not be built
      jobMap.values().each() { AzkabanJob job ->
        if (!jobsToBuild.contains(job)) {
          project.logger.lifecycle("WorkflowJobChecker WARNING: Workflow ${workflow.name} contains the job ${job.name} that is not executed by the workflow and is not a transitive dependency of the jobs the workflow does execute. This job will not be built.");
        }
      }
    }
  }
}