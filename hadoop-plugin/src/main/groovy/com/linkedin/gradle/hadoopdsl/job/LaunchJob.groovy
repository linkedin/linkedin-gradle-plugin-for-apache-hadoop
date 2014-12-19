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
package com.linkedin.gradle.hadoopdsl.job;

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
   * Override for LaunchJob. The name of the generated LaunchJob file is simply the workflow name.
   *
   * @param name The job name
   * @param parentScopeName The fully-qualified name of the scope in which the job is bound
   * @return The name to use when generating the job file
   */
  @Override
  String buildFileName(String name, String parentScopeName) {
    return name;
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
  @Override
  LaunchJob clone(LaunchJob cloneJob) {
    return super.clone(cloneJob);
  }
}