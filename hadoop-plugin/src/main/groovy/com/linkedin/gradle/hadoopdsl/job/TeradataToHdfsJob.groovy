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

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;
import com.linkedin.gradle.hadoopdsl.NamedScope;

/**
 * Job class for type=teradataToHdfsJob jobs.
 * <p>
 * According to the Azkaban documentation at http://azkaban.github.io/azkaban/docs/latest/#job-types,
 * this is the job type to move data from HDFS to Teradata.
 * <p>
 * In the DSL, a TeradataToHdfsJob can be specified with:
 * <pre>
 *   teradataToHdfsJob('jobName') {
 *     hostName 'dw.foo.com'                        // Required
 *     userId 'scott'                               // Required
 *     credentialName 'com.linkedin.teradata.scott' // Required
 *     encryptedCredential ''                       // Required
 *     cryptoKeyFilePath '/hdfs/file/path'          // Required
 *     sourceTable 'person'                         // Either sourceTable or sourceQuery is required
 *     sourceQuery 'select * from person;'          // Either sourceTable or sourceQuery is required
 *     targetHdfsPath '/job/data/test/output'       // Required
 *     avroSchemaPath '/job/data/src/avro.avsc'
 *     avroSchemaInline '{"type":"record","namespace":"com.example","name":"FullName","fields":[{"name":"first","type":"string"},{"name":"last","type":"string"}]}'
 *     set hadoopProperties: [
 *       'hadoopPropertyName1' : 'hadoopPropertyValue1',
 *       'hadoopPropertyName2' : 'hadoopPropertyValue2'
 *     ]
 *   }
 * </pre>
 */
class TeradataToHdfsJob extends Job {
  String hostName;
  String userId;
  String credentialName;
  String encryptedCredential;
  String cryptoKeyFilePath;
  String sourceTable;
  String sourceQuery;
  String targetHdfsPath;
  String avroSchemaPath;
  String avroSchemaInline;
  Map<String, Object> hadoopProperties;

  TeradataToHdfsJob(String jobName) {
    super(jobName);
    setJobProperty("type", "teradataToHdfs");
    hadoopProperties = new LinkedHashMap<String, Object>();
  }

  @Override
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> allProperties = super.buildProperties(parentScope);

    if (hadoopProperties.size() > 0) {
      String hadoopConfig = hadoopProperties.collect { String key, Object val -> return "-D${key}=${val.toString()}" }.join(" ");
      allProperties["hadoop.config"] = hadoopConfig;
    }

    return allProperties;
  }

  @HadoopDslMethod
  void hostName(String hostName) {
    this.hostName = hostName;
    setJobProperty("td.hostname", hostName);
  }

  @HadoopDslMethod
  void userId(String userId) {
    this.userId = userId;
    setJobProperty("td.userid", userId);
  }

  @HadoopDslMethod
  void credentialName(String credentialName) {
    this.credentialName = credentialName;
    setJobProperty("td.credentialName", credentialName);
  }

  @HadoopDslMethod
  void encryptedCredential(String encryptedCredential) {
    this.encryptedCredential = encryptedCredential;
    setJobProperty("td.encrypted.credential", encryptedCredential);
  }

  @HadoopDslMethod
  void cryptoKeyFilePath(String cryptoKeyFilePath) {
    this.cryptoKeyFilePath = cryptoKeyFilePath;
    setJobProperty("td.crypto.key.path", cryptoKeyFilePath);
  }

  @HadoopDslMethod
  void sourceTable(String sourceTable) {
    this.sourceTable = sourceTable;
    setJobProperty("source.td.tablename", sourceTable);
  }

  @HadoopDslMethod
  void sourceQuery(String sourceQuery) {
    this.sourceQuery = sourceQuery;
    setJobProperty("source.td.sourcequery", sourceQuery);
  }

  @HadoopDslMethod
  void targetHdfsPath(String targetHdfsPath) {
    this.targetHdfsPath = targetHdfsPath;
    setJobProperty("target.hdfs.path", targetHdfsPath);
  }

  @HadoopDslMethod
  void avroSchemaPath(String avroSchemaPath) {
    this.avroSchemaPath = avroSchemaPath;
    setJobProperty("avro.schema.path", avroSchemaPath);
  }

  @HadoopDslMethod
  void avroSchemaInline(String avroSchemaInline) {
    this.avroSchemaInline = avroSchemaInline;
    setJobProperty("avro.schema.inline", avroSchemaInline);
  }

  @HadoopDslMethod
  void setHadoopProperty(String name, Object value) {
    hadoopProperties.put(name, value);
  }

  @HadoopDslMethod
  @Override
  void set(Map args) {
    super.set(args);

    if (args.containsKey("hadoopProperties")) {
      Map<String, Object> hadoopProperties = args.hadoopProperties;
      hadoopProperties.each { name, value ->
        setHadoopProperty(name, value);
      }
    }
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  TeradataToHdfsJob clone() {
    return clone(new TeradataToHdfsJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  TeradataToHdfsJob clone(TeradataToHdfsJob cloneJob) {
    cloneJob.hostName = hostName;
    cloneJob.userId = userId;
    cloneJob.credentialName = credentialName;
    cloneJob.encryptedCredential = encryptedCredential;
    cloneJob.cryptoKeyFilePath = cryptoKeyFilePath;
    cloneJob.sourceTable = sourceTable;
    cloneJob.sourceQuery = sourceQuery;
    cloneJob.targetHdfsPath = targetHdfsPath;
    cloneJob.avroSchemaPath = avroSchemaPath;
    cloneJob.avroSchemaInline = avroSchemaInline;
    cloneJob.hadoopProperties.putAll(hadoopProperties);
    return ((TeradataToHdfsJob)super.clone(cloneJob));
  }
}
