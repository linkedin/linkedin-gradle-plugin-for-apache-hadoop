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
 * SubFlowJob is a job that groups all its children jobs into a parent node in Azkaban.
 */
class SubFlowJob extends StartJob {
  /**
   * Root of a jobs' graph that defines the group
   */
  String targetJobName;

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
   * Method to construct the file name to use for the job file. In Azkaban, all job files must have
   * unique names.
   * <p>
   * For a SubFlowJob, the file name is simply the cleaned fully-qualified workflow name.
   *
   * @param parentScope The parent scope in which the job is bound
   * @return The name to use when generating the job file
   */
  @Override
  String buildFileName(NamedScope parentScope) {
    // The file name for a launch job is just the cleaned up fully-qualified workflow name.
    return cleanFileName(parentScope.getQualifiedName());
  }

  void targets(String jobName) {
    targetJobName = jobName;
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
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
    // flow.name defines a root of jobs' dependency graph that will be used to group them
    allProperties['flow.name'] = buildFileName(parentScope, targetJobName);

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
    return ((SubFlowJob)super.clone(cloneJob));
  }

}
