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
package com.linkedin.gradle.hadoopdsl.job;

import com.linkedin.gradle.hadoopdsl.NamedScope;

/**
 * SubFlowJob is a job that groups the jobs in a child workflow into a single node in the Azkaban
 * web user interface.
 */
class SubFlowJob extends StartJob {
  /**
   * The launch job of the subflow to group.
   */
  LaunchJob launchJob;

  /**
   * Constructor for a SubFlowJob.
   *
   * @param jobName The job name
   */
  SubFlowJob(String jobName) {
    super(jobName);
    setJobProperty("type", "flow");
  }

  /**
   * Builds the job properties that go into the generated job file.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jobProperties correctly.
   *
   * @param parentScope The parent scope in which to lookup the base properties
   * @return The job properties map that holds all the properties that will go into the built job file
   */
  @Override
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> allProperties = super.buildProperties(parentScope);

    // Set flow.name to the file name of the launch job used to group the subflow
    allProperties['flow.name'] = launchJob.buildFileName(parentScope);

    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  SubFlowJob clone() {
    return clone(new SubFlowJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  SubFlowJob clone(SubFlowJob cloneJob) {
    cloneJob.launchJob = launchJob;
    return ((SubFlowJob)super.clone(cloneJob));
  }

  /**
   * Sets the launch job of the subflow that will be grouped.
   *
   * @param launchJob The launch job of the subflow that will be grouped
   */
  void declareJobToGroup(LaunchJob jobToGroup) {
    launchJob = jobToGroup;
  }
}
