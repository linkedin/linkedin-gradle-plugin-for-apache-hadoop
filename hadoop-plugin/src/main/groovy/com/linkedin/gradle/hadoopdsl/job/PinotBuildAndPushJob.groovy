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

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;

/**
 * Job class for type=PinotBuildandPushJob jobs. This job class is aimed at launching a job to
 * generate segments and push data to Pinot. The open source code is at:
 * https://github.com/linkedin/pinot.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. Please check that these values are appropriate for your application. In the
 * DSL, a PinotBuildandPush can be specified with:
 * <pre>
 *   pinotBuildAndPushJob('jobName') {
 *     usesTableName 'internalTesting'
 *     usesInputPath '/user/input'
 *     usesPushLocation 'host:port'
 *   }
 * </pre>
 */
class PinotBuildAndPushJob extends HadoopJavaJob {
  // Required
  String inputPath;
  String pushLocation;
  String tableName;

  /**
   * Constructor for PinotBuildPushJob.
   *
   * @param jobName The job name
   */
  PinotBuildAndPushJob(String jobName) {
    super(jobName);
    setJobProperty("type", "PinotBuildAndPushJob");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  PinotBuildAndPushJob clone() {
    return clone(new PinotBuildAndPushJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  PinotBuildAndPushJob clone(PinotBuildAndPushJob cloneJob) {
    cloneJob.inputPath = inputPath;
    cloneJob.pushLocation = pushLocation;
    cloneJob.tableName = tableName;
    return ((PinotBuildAndPushJob)super.clone(cloneJob));
  }

  /**
   * DSL usesInputPath method causes path.to.input=value to be set in the job file.
   *
   * @param inputPath The path where the input files are stored
   */
  @HadoopDslMethod
  void usesInputPath(String inputPath) {
    this.inputPath = inputPath;
    setJobProperty("path.to.input", inputPath);
  }

  /**
   * DSL usesPushLocation method causes push.location=value to be set in the job file.
   *
   * @param pushLocation The location files are pushed to, eg: host:port
   */
  @HadoopDslMethod
  void usesPushLocation(String pushLocation) {
    this.pushLocation = pushLocation;
    setJobProperty("push.location", pushLocation);
  }

  /**
   * DSL usesTableName method causes segment.table.name=value to be set in the job file.
   *
   * @param tableName The table to which the data is pushed to
   */
  @HadoopDslMethod
  void usesTableName(String tableName) {
    this.tableName = tableName;
    setJobProperty("segment.table.name", tableName);
  }
}