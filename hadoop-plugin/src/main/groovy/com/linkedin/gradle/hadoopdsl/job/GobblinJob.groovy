package com.linkedin.gradle.hadoopdsl.job;

import com.linkedin.gradle.hadoopdsl.NamedScope;

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
 *       'source.querybased.schema' : 'merlin_users',
 *       'source.entity' : 'user',
 *       'source.conn.host' : 'eat1-db42.corp.linkedin.com',
 *       'source.conn.username' : 'merlin_u',
 *       'source.conn.password' : 'ENC(7Bv+t0aw5rdyOYuyLqkrpTetE3KvfDRK)',
 *       'encrypt.key.loc' : '/jobs/jnchang/mstr_merlin.key',
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

  public GobblinJob(String jobName) {
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
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  GobblinJob clone(GobblinJob cloneJob) {
    cloneJob.workDir = workDir;
    cloneJob.preset = preset;
    return super.clone(cloneJob);
  }
}
