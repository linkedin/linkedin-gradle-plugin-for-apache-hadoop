/*
 * Copyright 2016 LinkedIn Corp.
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
 * Job class for type=hadoopShell jobs.
 * <p>
 * HadoopShell is a Hadoop-security enabled "command" jobtype. This jobtype adheres to same format
 * and other details as the "command" jobtype.
 * <p>
 * In the DSL, a HadoopShellJob can be specified with:
 * <pre>
 *   def commands = ['echo "hello"', 'echo "This is how one runs a command job"', 'whoami']
 *
 *   hadoopShellJob('jobName') {
 *     uses 'echo "hello world"'  // Exactly one of uses or usesCommands is required
 *     usesCommands commands      // Exactly one of uses or usesCommands is required
 *   }
 * </pre>
 */
class HadoopShellJob extends CommandJob {
  /**
   * Constructor for a HadoopShellJob.
   *
   * @param jobName The job name
   */
  HadoopShellJob(String jobName) {
    super(jobName);
    setJobProperty("type", "hadoopShell");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  HadoopShellJob clone() {
    return clone(new HadoopShellJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  HadoopShellJob clone(HadoopShellJob cloneJob) {
    cloneJob.command = command;
    cloneJob.commands = commands;
    return ((HadoopShellJob)super.clone(cloneJob));
  }
}
