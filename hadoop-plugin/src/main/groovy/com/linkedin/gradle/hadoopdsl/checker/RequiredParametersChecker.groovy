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
package com.linkedin.gradle.hadoopdsl.checker

import com.linkedin.gradle.hadoopdsl.job.Job
import com.linkedin.gradle.hadoopdsl.BaseStaticChecker
import com.linkedin.gradle.hadoopdsl.Workflow

import org.gradle.api.Project

class RequiredParametersChecker extends BaseStaticChecker {
  /**
   * Constructor for the requiredParametersChecker.
   *
   * @param project The Gradle project
   */
  RequiredParametersChecker(Project project) {
    super(project);
  }

  void checkRequiredParameters(Job job, List<String> flowPath) {
    if (!job.requiredParameters.isEmpty()) {
      job.requiredParameters.each { para ->
        if (!job.jobProperties.containsKey(para)) {
          String jobPath = flowPath.join('/') + '/' + job.name;
          project.logger.lifecycle("RequiredParametersChecker ERROR: Job ${jobPath} requires parameter ${para}, but it is not set.");
          foundError = true;
        }
      }
    }
  }

  @Override
  void visitWorkflow(Workflow workflow) {
    List<String> flowPath = [workflow.name];
    visitWorkflow(workflow, flowPath);
  }

  void visitWorkflow(Workflow workflow, List<String> flowPath) {
    workflow.workflows.each { Workflow flow ->
      flowPath.push(flow.name);
      visitWorkflow(flow, flowPath);
    }

    workflow.jobs.each { Job job ->
      checkRequiredParameters(job, flowPath);
    }

    flowPath.pop();
  }
}
