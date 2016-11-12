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

import com.linkedin.gradle.hadoopdsl.NamedScope;

/**
 * Job class for type=hdfsToTeradataJob jobs.
 * <p>
 * According to the Azkaban documentation at http://azkaban.github.io/azkaban/docs/latest/#job-types,
 * this is the job type to move data from HDFS to Teradata
 * <p>
 * In the DSL, a HdfsToTeradataJob can be specified with:
 * <pre>
 *   hdfsToTeradataJob('jobName') {
 *     hostName 'dw.foo.com'  // Required
 *     userId 'scott' //Required
 *     credentialName 'com.linkedin.teradata.scott' //*Required
 *     encryptedCredential '' //*Required
 *     cryptoKeyFilePath '/hdfs/file/path' //*Required
 *     sourceHiveDatabase 'hive_database_name' //**Required
 *     sourceHiveTable 'hive_table_name' //**Required
 *     sourceHdfsPath '/job/data/src' //**Required
 *     avroSchemaPath '/job/data/src/avro.avsc' //**Required
 *     avroSchemaInline '{"type":"record","namespace":"com.example","name":"FullName","fields":[{"name":"first","type":"string"},{"name":"last","type":"string"}]}' //**Required
 *     targetTable 'teradatatable' //Required
 *
 *     set hadoopProperties: [
 *       'hadoopPropertyName1' : 'hadoopPropertyValue1',
 *       'hadoopPropertyName2' : 'hadoopPropertyValue2'
 *     ]
 *   }
 *
 *   **Required: To use Hive as a source, both sourceHiveDatabase and sourceHiveTable need to be defined.
 *               To use Avro file as a source, sourceHdfsPath and either avroSchemaPath or avroSchemaInline needs to be defined.
 * </pre>
 */
class HdfsToTeradataJob extends Job {
  String hostName;
  String userId;
  String credentialName;
  String encryptedCredential;
  String cryptoKeyFilePath;

  String sourceHiveDatabase;
  String sourceHiveTable;

  String sourceHdfsPath;
  String avroSchemaPath;
  String avroSchemaInline;

  String targetTable;
  Map<String, Object> hadoopProperties;

  HdfsToTeradataJob(String jobName) {
    super(jobName);
    setJobProperty("type", "hdfsToTeradata");
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

  void hostName(String hostName) {
    this.hostName = hostName;
    setJobProperty("td.hostname", hostName);
  }

  void userId(String userId) {
    this.userId = userId;
    setJobProperty("td.userid", userId);
  }

  void credentialName(String credentialName) {
    this.credentialName = credentialName;
    setJobProperty("td.credentialName", credentialName);
  }

  void encryptedCredential(String encryptedCredential) {
    this.encryptedCredential = encryptedCredential;
    setJobProperty("td.encrypted.credential", encryptedCredential);
  }

  void cryptoKeyFilePath(String cryptoKeyFilePath) {
    this.cryptoKeyFilePath = cryptoKeyFilePath;
    setJobProperty("td.crypto.key.path", cryptoKeyFilePath);
  }

  void sourceHiveDatabase(String sourceHiveDatabase) {
    this.sourceHiveDatabase = sourceHiveDatabase;
    setJobProperty("source.hive.databasename", sourceHiveDatabase);
  }

  /**
   * Setting table name. Also, setting jobtype as hive.
   * @param sourceHiveTable
   */
  void sourceHiveTable(String sourceHiveTable) {
    this.sourceHiveTable = sourceHiveTable;
    setJobProperty("source.hive.tablename", sourceHiveTable);
    setJobProperty("tdch.jobtype", "hive");
  }

  void sourceHdfsPath(String sourceHdfsPath) {
    this.sourceHdfsPath = sourceHdfsPath;
    setJobProperty("source.hdfs.path", sourceHdfsPath);
  }

  void avroSchemaPath(String avroSchemaPath) {
    this.avroSchemaPath = avroSchemaPath;
    setJobProperty("avro.schema.path", avroSchemaPath);
  }

  void avroSchemaInline(String avroSchemaInline) {
    this.avroSchemaInline = avroSchemaInline;
    setJobProperty("avro.schema.inline", avroSchemaInline);
  }

  void targetTable(String targetTable) {
    this.targetTable = targetTable;
    setJobProperty("target.td.tablename", targetTable);
  }

  void setHadoopProperty(String name, Object value) {
    hadoopProperties.put(name, value);
  }

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
  HdfsToTeradataJob clone() {
    return clone(new HdfsToTeradataJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  HdfsToTeradataJob clone(HdfsToTeradataJob cloneJob) {
    cloneJob.hostName = hostName;
    cloneJob.userId = userId;
    cloneJob.credentialName = credentialName;
    cloneJob.encryptedCredential = encryptedCredential;
    cloneJob.cryptoKeyFilePath = cryptoKeyFilePath;
    cloneJob.sourceHiveDatabase = sourceHiveDatabase;
    cloneJob.sourceHiveTable = sourceHiveTable;
    cloneJob.sourceHdfsPath = sourceHdfsPath;
    cloneJob.avroSchemaPath = avroSchemaPath;
    cloneJob.avroSchemaInline = avroSchemaInline;
    cloneJob.targetTable = targetTable;
    cloneJob.hadoopProperties.putAll(hadoopProperties);
    return ((HdfsToTeradataJob)super.clone(cloneJob));
  }
}
