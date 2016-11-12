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
 * A LaunchJob is a special kind of NoOpJob that is used to launch a workflow. The name of the
 * generated LaunchJob file is simply the workflow name.
 * <p>
 * Launch jobs are not specified in the DSL. When you build a workflow, a LaunchJob (with
 * dependencies set to be the names of the jobs the workflow executes) is created for you.
 */
class LaunchJob extends NoOpJob {
  /**
   * Constructor for a LaunchJob.
   *
   * @param jobName The job name
   */
  LaunchJob(String jobName) {
    super(jobName);
  }

  /**
   * Method to construct the file name to use for the job file. In Azkaban, all job files must have
   * unique names.
   * <p>
   * For a LaunchJob, the file name is simply the cleaned fully-qualified workflow name.
   *
   * @param parentScope The parent scope in which the job is bound
   * @return The name to use when generating the job file
   */
  @Override
  String buildFileName(NamedScope parentScope) {
    // The file name for a launch job is just the cleaned up fully-qualified workflow name.
    return cleanFileName(parentScope.getQualifiedName());
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  LaunchJob clone() {
    return clone(new LaunchJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  LaunchJob clone(LaunchJob cloneJob) {
    return ((LaunchJob)super.clone(cloneJob));
  }
}