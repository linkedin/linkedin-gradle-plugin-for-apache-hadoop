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
import com.linkedin.gradle.hadoopdsl.NamedScope;

/**
 * Job class for type=VenicePushJob jobs. This job class is aimed at launching a job to push data to Venice.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. Please check that these values are appropriate for your application. In the
 * DSL, a VenicePushJob can be specified with:
 * <pre>
 *   venicePushJob('jobName') {
 *     usesAvroKeyField 'key'
 *     usesAvroValueField 'value'
 *     usesClusterName '${venice.cluster.0}'
 *     usesInputPath '/user/voldtest/h2v_test/1k'
 *     usesVeniceStoreName 'venice_h2v_test_1k'
 *   }
 * </pre>
 */
class VenicePushJob extends HadoopJavaJob {
  // Required
  String avroKeyField;
  String avroValueField;
  String clusterName;
  String inputPath;
  String veniceStoreName;

  /**
   * Constructor for VenicePushJob.
   *
   * @param jobName The job name
   */
  VenicePushJob(String jobName) {
    super(jobName);
    setJobProperty("type", "VenicePushJob");
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
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> allProperties = super.buildProperties(parentScope);
    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  VenicePushJob clone() {
    return clone(new VenicePushJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  VenicePushJob clone(VenicePushJob cloneJob) {
    cloneJob.avroKeyField = avroKeyField;
    cloneJob.avroValueField = avroValueField;
    cloneJob.clusterName = clusterName;
    cloneJob.inputPath = inputPath;
    cloneJob.veniceStoreName = veniceStoreName;
    return ((VenicePushJob)super.clone(cloneJob));
  }

  /**
   * DSL usesAvroKeyField method causes avro.key.field=value to be set in the job file.
   *
   * @param avroKeyField the avro key schema for Venice
   */
  @HadoopDslMethod
  void usesAvroKeyField(String avroKeyField) {
    this.avroKeyField = avroKeyField;
    setJobProperty("avro.key.field", avroKeyField);
  }

  /**
   * DSL usesAvroValueField method causes avro.value.field=value to be set in the job file.
   *
   * @param avroValueField the avro value schema for Venice
   */
  @HadoopDslMethod
  void usesAvroValueField(String avroValueField) {
    this.avroValueField = avroValueField;
    setJobProperty("avro.value.field", avroValueField);
  }

  /**
   * DSL usesClusterName method causes cluster.name=value to be set in the job file.
   *
   * @param clusterName the Venice cluster that data is pushed to
   */
  @HadoopDslMethod
  void usesClusterName(String clusterName) {
    this.clusterName = clusterName;
    setJobProperty("cluster.name", clusterName);
  }

  /**
   * DSL usesInputPath method causes input.path=value to be set in the job file.
   *
   * @param inputPath the path where the input files are stored at
   */
  @HadoopDslMethod
  void usesInputPath(String inputPath) {
    this.inputPath = inputPath;
    setJobProperty("input.path", inputPath);
  }

  /**
   * DSL usesVeniceStoreName method causes venice.store.name=value to be set in the job file.
   *
   * @param veniceStoreName the Venice store that data is pushed to
   */
  @HadoopDslMethod
  void usesVeniceStoreName(String veniceStoreName) {
    this.veniceStoreName = veniceStoreName;
    setJobProperty("venice.store.name", veniceStoreName);
  }
}
