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
 * Job class for type=VoldemortBuildandPush jobs.
 * <p>
 * These are documented internally at LinkedIn at https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Voldemort+Build+and+Push.
 * <p>
 * The code for this job is at https://github.com/voldemort/voldemort/blob/master/contrib/hadoop-store-builder/src/java/voldemort/store/readonly/mr/azkaban/VoldemortBuildAndPushJob.java.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. Please check that these values are appropriate for your application. In the
 * DSL, a VoldemortBuildandPush can be specified with:
 * <pre>
 *   voldemortBuildPushJob('jobName') {
 *     usesStoreName 'test-store'          // Required
 *     usesClusterName 'tcp://foo:10103'   // Required
 *     usesInputPath '/user/foo/input'     // Required
 *     usesOutputPath '/user/foo/output'   // Required
 *     usesStoreOwners 'foo@linkedin.com'  // Required
 *     usesStoreDesc 'Store for testing'   // Required
 *     usesTempDir '/tmp/foo'              // Optional
 *     usesRepFactor 2                     // Optional
 *     usesCompressValue false             // Optional
 *     usesKeySelection 'memberId'         // Optional
 *     usesValueSelection 'lastName'       // Optional
 *     usesNumChunks(-1)                   // Optional
 *     usesChunkSize 1073741824            // Optional
 *     usesKeepOutput false                // Optional
 *     usesPushHttpTimeoutSeconds 86400    // Optional
 *     usesPushNode 0                      // Optional
 *     usesBuildStore true                 // Optional
 *     usesPushStore true                  // Optional
 *     usesFetcherProtocol 'hftp'          // Optional
 *     usesFetcherPort '50070'             // Optional
 *     usesAvroSerializerVersioned false   // Optional
 *     usesAvroData false                  // Optional
 *     usesAvroKeyField 'memberId'         // Optional unless isAvroData is true
 *     usesAvroValueField 'firstName'      // Optional unless isAvroData is true
 *   }
 * </pre>
 */
class VoldemortBuildPushJob extends HadoopJavaJob {
  // Required
  String storeName;
  String clusterName;
  String buildInputPath;
  String buildOutputPath;
  String storeOwners;
  String storeDesc;

  // Optional
  String buildTempDir;
  Integer repFactor;
  Boolean compressValue;
  String keySelection;
  String valueSelection;
  Integer numChunks;
  Integer chunkSize;
  Boolean keepOutput;
  Integer pushHttpTimeoutSeconds;
  Integer pushNode;
  Boolean buildStore;
  Boolean pushStore;
  String fetcherProtocol;
  String fetcherPort;
  Boolean isAvroSerializerVersioned;
  Boolean isAvroData;
  String avroKeyField;    // Required if isAvroData is true
  String avroValueField;  // Required if isAvroData is true

  /**
   * Constructor for a VoldemortBuildPushJob.
   *
   * @param jobName The job name
   */
  VoldemortBuildPushJob(String jobName) {
    super(jobName);
    setJobProperty("type", "VoldemortBuildandPush");
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
    if (isAvroData != null && isAvroData) {
      allProperties["avro.key.field"] = avroKeyField;
      allProperties["avro.value.field"] = avroValueField;
    }
    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  VoldemortBuildPushJob clone() {
    return clone(new VoldemortBuildPushJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  VoldemortBuildPushJob clone(VoldemortBuildPushJob cloneJob) {
    cloneJob.storeName = storeName;
    cloneJob.clusterName = clusterName;
    cloneJob.buildInputPath = buildInputPath;
    cloneJob.buildOutputPath = buildOutputPath;
    cloneJob.storeOwners = storeOwners;
    cloneJob.storeDesc = storeDesc;
    cloneJob.buildTempDir = buildTempDir;
    cloneJob.repFactor = repFactor;
    cloneJob.compressValue = compressValue;
    cloneJob.keySelection = keySelection;
    cloneJob.valueSelection = valueSelection;
    cloneJob.numChunks = numChunks;
    cloneJob.chunkSize = chunkSize;
    cloneJob.keepOutput = keepOutput;
    cloneJob.pushHttpTimeoutSeconds = pushHttpTimeoutSeconds;
    cloneJob.pushNode = pushNode;
    cloneJob.buildStore = buildStore;
    cloneJob.pushStore = pushStore;
    cloneJob.fetcherProtocol = fetcherProtocol;
    cloneJob.fetcherPort = fetcherPort;
    cloneJob.isAvroSerializerVersioned = isAvroSerializerVersioned;
    cloneJob.isAvroData = isAvroData;
    cloneJob.avroKeyField = avroKeyField;
    cloneJob.avroValueField = avroValueField;
    return ((VoldemortBuildPushJob)super.clone(cloneJob));
  }

  /**
   * DSL usesStoreName method causes push.store.name=value to be set in the job file.
   *
   * @param storeName
   */
  @HadoopDslMethod
  void usesStoreName(String storeName) {
    this.storeName = storeName;
    setJobProperty("push.store.name", storeName);
  }

  /**
   * DSL usesClusterName method causes push.cluster=value to be set in the job file.
   *
   * @param clusterName
   */
  @HadoopDslMethod
  void usesClusterName(String clusterName) {
    this.clusterName = clusterName;
    setJobProperty("push.cluster", clusterName);
  }

  /**
   * DSL usesInputPath method causes build.input.path=value to be set in the job file.
   *
   * @param buildInputPath
   */
  @HadoopDslMethod
  void usesInputPath(String buildInputPath) {
    this.buildInputPath = buildInputPath;
    setJobProperty("build.input.path", buildInputPath);
  }

  /**
   * DSL usesOutputPath method causes build.output.dir=value to be set in the job file.
   *
   * @param buildOutputPath
   */
  @HadoopDslMethod
  void usesOutputPath(String buildOutputPath) {
    this.buildOutputPath = buildOutputPath;
    setJobProperty("build.output.dir", buildOutputPath);
  }

  /**
   * DSL usesStoreOwners method causes push.store.owners=value to be set in the job file.
   *
   * @param storeOwners
   */
  @HadoopDslMethod
  void usesStoreOwners(String storeOwners) {
    this.storeOwners = storeOwners;
    setJobProperty("push.store.owners", storeOwners);
  }

  /**
   * DSL usesStoreDesc method causes push.store.description=value to be set in the job file.
   *
   * @param storeDesc
   */
  @HadoopDslMethod
  void usesStoreDesc(String storeDesc) {
    this.storeDesc = storeDesc;
    setJobProperty("push.store.description", storeDesc);
  }

  /**
   * DSL usesTempDir method causes build.temp.dir=value to be set in the job file.
   *
   * @param buildTempDir
   */
  @HadoopDslMethod
  void usesTempDir(String buildTempDir) {
    this.buildTempDir = buildTempDir;
    setJobProperty("build.temp.dir", buildTempDir);
  }

  /**
   * DSL usesRepFactor method causes build.replication.factor=value to be set in the job file.
   *
   * @param repFactor
   */
  @HadoopDslMethod
  void usesRepFactor(Integer repFactor) {
    this.repFactor = repFactor;
    setJobProperty("build.replication.factor", repFactor);
  }

  /**
   * DSL usesCompressValue method causes build.compress.value=value to be set in the job file.
   *
   * @param compressValue
   */
  @HadoopDslMethod
  void usesCompressValue(Boolean compressValue) {
    this.compressValue = compressValue;
    setJobProperty("build.compress.value", compressValue);
  }

  /**
   * DSL usesKeySelection method causes key.selection=value to be set in the job file.
   *
   * @param keySelection
   */
  @HadoopDslMethod
  void usesKeySelection(String keySelection) {
    this.keySelection = keySelection;
    setJobProperty("key.selection", keySelection);
  }

  /**
   * DSL usesValueSelection method causes value.selection=value to be set in the job file.
   *
   * @param valueSelection
   */
  @HadoopDslMethod
  void usesValueSelection(String valueSelection) {
    this.valueSelection = valueSelection;
    setJobProperty("value.selection", valueSelection);
  }

  /**
   * DSL usesNumChunks method causes num.chunks=value to be set in the job file.
   *
   * @param numChunks
   */
  @HadoopDslMethod
  void usesNumChunks(Integer numChunks) {
    this.numChunks = numChunks;
    setJobProperty("num.chunks", numChunks);
  }

  /**
   * DSL usesChunkSize method causes build.chunk.size=value to be set in the job file.
   *
   * @param chunkSize
   */
  @HadoopDslMethod
  void usesChunkSize(Integer chunkSize) {
    this.chunkSize = chunkSize;
    setJobProperty("build.chunk.size", chunkSize);
  }

  /**
   * DSL usesKeepOutput method causes build.output.keep=value to be set in the job file.
   *
   * @param keepOutput
   */
  @HadoopDslMethod
  void usesKeepOutput(Boolean keepOutput) {
    this.keepOutput = keepOutput;
    setJobProperty("build.output.keep", keepOutput);
  }

  /**
   * DSL usesPushHttpTimeoutSeconds method causes push.http.timeout.seconds=value to be set in the
   * job file.
   *
   * @param pushHttpTimeoutSeconds
   */
  @HadoopDslMethod
  void usesPushHttpTimeoutSeconds(Integer pushHttpTimeoutSeconds) {
    this.pushHttpTimeoutSeconds = pushHttpTimeoutSeconds;
    setJobProperty("push.http.timeout.seconds", pushHttpTimeoutSeconds);
  }

  /**
   * DSL usesPushNode method causes push.node=value to be set in the job file.
   *
   * @param pushNode
   */
  @HadoopDslMethod
  void usesPushNode(Integer pushNode) {
    this.pushNode = pushNode;
    setJobProperty("push.node", pushNode);
  }

  /**
   * DSL usesBuildStore method causes build=value to be set in the job file.
   *
   * @param buildStore
   */
  @HadoopDslMethod
  void usesBuildStore(Boolean buildStore) {
    this.buildStore = buildStore;
    setJobProperty("build", buildStore);
  }

  /**
   * DSL usesPushStore method causes push=value to be set in the job file.
   *
   * @param pushStore
   */
  @HadoopDslMethod
  void usesPushStore(Boolean pushStore) {
    this.pushStore = pushStore;
    setJobProperty("push", pushStore);
  }

  /**
   * DSL usesFetcherProtocol method causes voldemort.fetcher.protocol=value to be set in the job
   * file.
   *
   * @param fetcherProtocol
   */
  @HadoopDslMethod
  void usesFetcherProtocol(String fetcherProtocol) {
    this.fetcherProtocol = fetcherProtocol;
    setJobProperty("voldemort.fetcher.protocol", fetcherProtocol);
  }

  /**
   * DSL usesFetcherPort method causes voldemort.fetcher.port=value to be set in the job file.
   *
   * @param fetcherPort
   */
  @HadoopDslMethod
  void usesFetcherPort(String fetcherPort) {
    this.fetcherPort = fetcherPort;
    setJobProperty("voldemort.fetcher.port", fetcherPort);
  }

  /**
   * DSL usesAvroSerializerVersioned method causes avro.serializer.versioned=value to be set in the
   * job file.
   *
   * @param isAvroSerializerVersioned
   */
  @HadoopDslMethod
  void usesAvroSerializerVersioned(Boolean isAvroSerializerVersioned) {
    this.isAvroSerializerVersioned = isAvroSerializerVersioned;
    setJobProperty("avro.serializer.versioned", isAvroSerializerVersioned);
  }

  /**
   * DSL usesAvroData method causes build.type.avro=value to be set in the job file.
   *
   * @param isAvroData
   */
  @HadoopDslMethod
  void usesAvroData(Boolean isAvroData) {
    this.isAvroData = isAvroData;
    setJobProperty("build.type.avro", isAvroData);
  }

  /**
   * DSL usesAvroKeyField method causes avro.key.field=value to be set in the job file. NOTE: this
   * property is only added to the job file if isAvroData is set to true.
   *
   * @param avroKeyField
   */
  @HadoopDslMethod
  void usesAvroKeyField(String avroKeyField) {
    this.avroKeyField = avroKeyField;
    // Only set the job property at build time if isAvroData is true
  }

  /**
   * DSL usesAvroValueField method causes avro.value.field=value to be set in the job file. NOTE:
   * this property is only added to the job file if isAvroData is set to true.
   *
   * @param avroValueField
   */
  @HadoopDslMethod
  void usesAvroValueField(String avroValueField) {
    this.avroValueField = avroValueField;
    // Only set the job property at build time if isAvroData is true
  }
}