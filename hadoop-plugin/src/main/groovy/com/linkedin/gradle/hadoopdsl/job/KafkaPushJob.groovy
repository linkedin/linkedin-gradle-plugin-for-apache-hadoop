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
 * Job class for type=KafkaPushJob jobs.
 * <p>
 * These are documented internally at LinkedIn at https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Hadoop+to+Kafka+Bridge.
 * <p>
 * The code for this job is at http://svn.corp.linkedin.com/netrepo/hadoop-to-kafka-bridge/trunk/.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. Please check that these values are appropriate for your application. In the
 * DSL, a KafkaPushJob can be specified with:
 * <pre>
 *   kafkaPushJob('jobName') {
 *     usesInputPath '/data/databases/MEMBER2/MEMBER_PROFILE/#LATEST'  // Required
 *     usesTopic 'kafkatestpush'                                       // Required
 *     usesNameNode 'hdfs://theNameNode.linkedin.com:9000'             // Optional (Required in versions 0.5.3 through 0.5.14)
 *     usesBatchNumBytes 1000000                                       // Optional
 *     usesDisableSchemaRegistration true                              // Optional
 *     usesKafkaUrl 'theKafkaNode.linkedin.com:10251'                  // Optional
 *     usesSchemaRegistryUrl 'http://theKafkaNode.linkedin.com:10252/schemaRegistry/schemas'  // Optional
 *     usesDisableAuditing true                                        // (Since version 0.4.5) Optional
 *   }
 * </pre>
 */
class KafkaPushJob extends HadoopJavaJob {
  // Required
  String inputPath;
  String topic;
  String nameNode;

  // Optional
  Integer batchNumBytes;
  Boolean disableSchemaRegistration;
  String kafkaUrl;
  String schemaRegistryUrl;
  Boolean disableAuditing;

  /**
   * Constructor for a KafkaPushJob.
   *
   * @param jobName The job name
   */
  KafkaPushJob(String jobName) {
    super(jobName);
    setJobProperty("type", "KafkaPushJob");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  KafkaPushJob clone() {
    return clone(new KafkaPushJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  KafkaPushJob clone(KafkaPushJob cloneJob) {
    cloneJob.inputPath = inputPath;
    cloneJob.topic = topic;
    cloneJob.batchNumBytes = batchNumBytes;
    cloneJob.kafkaUrl = kafkaUrl;
    cloneJob.nameNode = nameNode;
    cloneJob.schemaRegistryUrl = schemaRegistryUrl;
    cloneJob.disableAuditing = disableAuditing
    return ((KafkaPushJob)super.clone(cloneJob));
  }

  /**
   * DSL usesInputPath method causes input.path=value to be set in the job file.
   *
   * @param inputPath
   */
  @HadoopDslMethod
  void usesInputPath(String inputPath) {
    this.inputPath = inputPath;
    setJobProperty("input.path", inputPath);
  }

  /**
   * DSL usesTopic method causes topic=value to be set in the job file.
   *
   * @param topic
   */
  @HadoopDslMethod
  void usesTopic(String topic) {
    this.topic = topic;
    setJobProperty("topic", topic);
  }

  /**
   * DSL usesBatchNumBytes method causes batch.num.bytes=value to be set in the job file.
   *
   * @param batchNumBytes
   */
  @HadoopDslMethod
  void usesBatchNumBytes(Integer batchNumBytes) {
    this.batchNumBytes = batchNumBytes;
    setJobProperty("batch.num.bytes", batchNumBytes);
  }

  /**
   * DSL usesDisableSchemaRegistration method causes disable.schema.registration=value to be set in
   * the job file.
   *
   * @param disableSchemaRegistration
   */
  @HadoopDslMethod
  void usesDisableSchemaRegistration(Boolean disableSchemaRegistration) {
    this.disableSchemaRegistration = disableSchemaRegistration;
    setJobProperty("disable.schema.registration", disableSchemaRegistration);
  }

  /**
   * DSL usesKafkaUrl method causes kafka.url=value to be set in the job file.
   *
   * @param kafkaUrl
   */
  @HadoopDslMethod
  void usesKafkaUrl(String kafkaUrl) {
    this.kafkaUrl = kafkaUrl;
    setJobProperty("kafka.url", kafkaUrl);
  }

  /**
   * DSL usesNameNode method causes name.node=value to be set in the job file.
   *
   * @param nameNode
   */
  @HadoopDslMethod
  void usesNameNode(String nameNode) {
    this.nameNode = nameNode;
    setJobProperty("name.node", nameNode);
  }

  /**
   * DSL usesSchemaRegistryUrl method causes schemaregistry.rest.url=value to be set in the job
   * file.
   *
   * @param schemaRegistryUrl
   */
  @HadoopDslMethod
  void usesSchemaRegistryUrl(String schemaRegistryUrl) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    setJobProperty("schemaregistry.rest.url", schemaRegistryUrl);
  }

  /**
   * DSL usesDisableAuditing method causes disable.auditing=value to be set in the job file.
   *
   * @param disableAuditing
   */
  @HadoopDslMethod
  void usesDisableAuditing(Boolean disableAuditing) {
    this.disableAuditing = disableAuditing;
    setJobProperty("disable.auditing", disableAuditing);
  }
}
