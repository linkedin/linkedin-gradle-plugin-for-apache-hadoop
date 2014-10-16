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
package com.linkedin.gradle.lihadoopdsl;

import com.linkedin.gradle.hadoopdsl.PigJob;

/**
 * Extend the Hadoop Plugin Job classes with the LinkedIn-specific "pigLi" job type.
 * <p>
 * In the DSL, a PigLiJob can be specified with:
 * <pre>
 *   pigLiJob('jobName') {
 *     uses 'myScript.pig'     // Required
 *     reads files: [
 *       'foo' : '/data/databases/foo'
 *     ]
 *     writes files: [
 *       'bazz' : '/user/bazz/foobar'
 *     ]
 *     set parameters: [
 *       'param1' : 'val1'
 *       'param2' : 'val2'
 *     ]
 *   }
 * </pre>
 */
class PigLiJob extends PigJob {
  /**
   * Constructor for a PigLiJob.
   *
   * @param jobName The job name
   */
  PigLiJob(String jobName) {
    super(jobName);
  }

  // Override the job type with the LinkedIn-specific pigLi type.
  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    super.buildProperties(allProperties);
    allProperties["type"] = "pigLi";
    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  PigLiJob clone() {
    return clone(new PigLiJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  PigLiJob clone(PigLiJob cloneJob) {
    cloneJob.parameters.putAll(parameters);
    cloneJob.script = script;
    return super.clone(cloneJob);
  }
}