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
 * Job class for type=gobblinJob jobs.
 *
 * <p>
 * According to the Azkaban documentation at http://azkaban.github.io/azkaban/docs/latest/#job-types,
 * this is the job type that lauches Gobblin in Azkaban
 * <p>
 * In the DSL, a GobblinJob can be specified with:
 * <pre>
 *   gobblinJob('jobName') {
 *     workDir '/job/data/src'  //Optional
 *     preset 'mysqlToHdfs'     //Optional
 *     set properties: [        //Optional Add Gobblin job properties. (https://github.com/linkedin/gobblin/wiki/Working-with-Job-Configuration-Files)
 *       'source.querybased.schema' : 'DATABASE',
 *       'source.entity' : 'user',
 *       'source.conn.host' : 'mysql.host.com'
 *       'source.conn.username' : 'USERNAME',
 *       'source.conn.password' : 'ENCRYPTED_PASSWORD'
 *       'encrypt.key.loc' : '/path/to/key'
 *       'extract.table.type' : 'snapshot_only',
 *       'extract.is.full' : true,
 *       'data.publisher.replace.final.dir' : true,
 *       'data.publisher.final.dir' :  '${gobblin.work_dir}/job-output',
 *       'gobblinJobPropertyKey1' : 'gobblinJobPropertyValue1',
 *       'gobblinJobPropertyKey2' : 'gobblinJobPropertyValue2',
 *     ]
 *   }
 * </pre>
 */
class GobblinJob extends Job {
  String workDir;
  String preset;

  GobblinJob(String jobName) {
    super(jobName);
    setJobProperty("type", "gobblin");
  }

  void workDir(String workDir) {
    this.workDir = workDir;
    setJobProperty("gobblin.work_dir", workDir);
  }

  void preset(String preset) {
    this.preset = preset;
    setJobProperty("gobblin.config_preset", preset);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  GobblinJob clone() {
    return clone(new GobblinJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  GobblinJob clone(GobblinJob cloneJob) {
    cloneJob.workDir = workDir;
    cloneJob.preset = preset;
    return super.clone(cloneJob);
  }
}
