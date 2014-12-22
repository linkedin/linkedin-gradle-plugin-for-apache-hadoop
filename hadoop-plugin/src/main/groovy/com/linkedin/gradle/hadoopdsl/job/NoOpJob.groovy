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
 * Job class for type=noop jobs.
 * <p>
 * In the DSL, a NoOpJob can be specified with:
 * <pre>
 *   noOpJob('jobName') {
 *     depends 'job1', 'job2'  // Typically in a NoOpJob the only thing you will ever declare are job dependencies
 *   }
 * </pre>
 */
class NoOpJob extends Job {
  /**
   * Constructor for a NoOpJob.
   *
   * @param jobName The job name
   */
  NoOpJob(String jobName) {
    super(jobName);
    setJobProperty("type", "noop");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  NoOpJob clone() {
    return clone(new NoOpJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  NoOpJob clone(NoOpJob cloneJob) {
    return super.clone(cloneJob);
  }
}