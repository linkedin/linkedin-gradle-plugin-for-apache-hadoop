/*
 * Copyright 2018 LinkedIn Corp.
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

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;

/**
 * Job class for type=CarbonJob jobs. This job class is aimed at doing data life cycle management tasks,
 * e.g. 1) file based distributed copy between HDFS/Azure clusters, 2) hive based dist-cp, 3) hdfs retention.
 */
class CarbonJob extends HadoopJavaJob {
  // Required
  String taskType;

  /**
   * Constructor for CarbonJob.
   *
   * @param jobName - The job name
   */
  CarbonJob(String jobName) {
    super(jobName);
    setJobProperty("type", "CarbonJob");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  CarbonJob clone() {
    return clone(new CarbonJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob - The job being cloned
   * @return The cloned job
   */
  CarbonJob clone(CarbonJob cloneJob) {
    cloneJob.taskType = taskType;
    return ((CarbonJob) super.clone(cloneJob));
  }

  /**
   * DSL taskType method causes task.type=value to be set in the job file.
   *
   * @param taskType - type of the task to perform. required parameters of the CarbonJob will depend on the task type.
   */
  @HadoopDslMethod
  void taskType(String taskType) {
    this.taskType = taskType;
    setJobProperty("task.type", taskType);
  }
}
