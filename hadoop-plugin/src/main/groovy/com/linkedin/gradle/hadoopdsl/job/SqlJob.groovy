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
 * Job class for type=sql jobs.
 *
 * <p>
 * These are currently internal job type at LinkedIn.
 * This is the job type to execute one or more SQL statements.
 * <p>
 * In the DSL, a SQL job type can be specified with:
 * <pre>
 *   sqlJob('jobName') {
 *     jdbcDriverClass com.teradata.jdbc.TeraDriver //Required
 *     jdbcUrl 'jdbc:teradata://foo.com/DBS_PORT=1025,CHARSET=UTF8,TMODE=TERA' //Required
 *     jdbcUserId 'foo'           //Required
 *     jdbcEncryptedCredential 'eyJ2YWwiOiJiQzVCU09HbDVwYndxNFRXV00yZ'  //Required
 *     jdbcCryptoKeyPath '/hdfs/path/to/cryptokey/file'         //Required
 *     set properties: [
 *       'user.to.proxy' : 'testUser'
 *       'jdbc.sql.1' : 'DELETE test_table_publish ALL;',
 *       'jdbc.sql.2' : 'INSERT INTO test_table_publish SELECT * FROM test_table;',
 *       'propertyName1' : 'propertyValue1',
 *       'propertyName2' : 'propertyValue2',
 *     ]
 *   }
 * </pre>
 */
class SqlJob extends Job {
  String jdbcDriverClass;
  String jdbcUrl;
  String jdbcUserId;
  String jdbcEncryptedCredential;
  String jdbcCryptoKeyPath;

  SqlJob(String jobName) {
    super(jobName);
    setJobProperty("type", "sql");
  }

  void jdbcDriverClass(String jdbcDriverClass) {
    this.jdbcDriverClass = jdbcDriverClass;
    setJobProperty("jdbc.driver.class", jdbcDriverClass);
  }

  void jdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
    setJobProperty("jdbc.url", jdbcUrl);
  }

  void jdbcUserId(String jdbcUserId) {
    this.jdbcUserId = jdbcUserId;
    setJobProperty("jdbc.userid", jdbcUserId);
  }

  void jdbcEncryptedCredential(String jdbcEncryptedCredential) {
    this.jdbcEncryptedCredential = jdbcEncryptedCredential;
    setJobProperty("jdbc.encrypted.credential", jdbcEncryptedCredential);
  }

  void jdbcCryptoKeyPath(String jdbcCryptoKeyPath) {
    this.jdbcCryptoKeyPath = jdbcCryptoKeyPath;
    setJobProperty("jdbc.crypto.key.path", jdbcCryptoKeyPath);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  SqlJob clone() {
    return clone(new SqlJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  HdfsToEspressoJob clone(HdfsToEspressoJob cloneJob) {
    cloneJob.jdbcDriverClass = jdbcDriverClass;
    cloneJob.jdbcUrl = jdbcUrl;
    cloneJob.jdbcUserId = jdbcUserId;
    cloneJob.jdbcEncryptedCredential = jdbcEncryptedCredential;
    cloneJob.jdbcCryptoKeyPath = jdbcCryptoKeyPath;
    return super.clone(cloneJob);
  }
}
