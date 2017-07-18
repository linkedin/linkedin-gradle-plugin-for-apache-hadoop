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
 * Job class for HdfsWait jobs.
 * <p>
 * This is the job type you should use when you have one job that depends on data output
 * from an external source or from another job that is in a different project. Parameters:
 * directoryPath is the path to the directory that contains the folder(s) you want checked,
 * directoryFreshness is the string that contains integer(s) and time unit(s) you specify
 * that makes it so a file is invalid if it has not been updated in that time frame, timeout
 * is the string that contains integer(s) and time unit(s) you specify that will cause the
 * job to finish after that amount of time, sleepTime is the string that contains integer(s)
 * and time unit(s) you specify that causes the job to wait that amount of time between each
 * check for a fresh folder, which will default to a value of 1 minute if not specified by
 * the user, checkExactPath is the boolean value you specify that when true (defaults to
 * false), the job will simply check for the existence of the dirPath, and if it exists the job
 * will succeed, timezone, which defaults to America/Los_Angeles, is a string that represents the
 * timezone of the dirPath if the user wants to find a file path that correlates to a date,
 * and failOnTimeout is the boolean value you specify that determines if the job fails
 * fails or succeeds on timeout. The time units that are supported when declaring the
 * 'directoryFreshness', 'timeoutAfter', and 'sleepTime' parameters are:
 *    Seconds: 'S'  ex. '49S' = 49 seconds,
 *    Minutes: 'M'  ex. '26M' = 26 minutes,
 *    Hours:   'H'  ex. '7H'  = 7 hours,
 *    Days:    'D'  ex. '2D'  = 2 days
 * The syntax for declaring these parameters is such:
 *    '(#ofUnit)(unit)(whitespace)...'
 *    ex. '20S' = 20 seconds,
 *    ex. '40H 22M' = 40 hours and 22 minutes,
 *    ex. '0D 89S' = 0 days and 89 seconds,
 *    ex. '26H 10D 37M' = 26 hours, 10 days and 37 minutes
 * For parameter 'directoryPath', you are able to append these two things to the 
 * end of the file path:
 *    "%Y-%m-%d" or "%Y/%m/%d"
 *    ex. directoryPath 'foo/blah/%Y-%m-%d' will construct a file path of the form:
 *        'foo/blah/YYYY-MM-DD'
 *    ex. directoryPath 'foo/blah/%Y/%m/%d' will construct a file path of the form:
 *        'foo/blah/YYYY/MM/DD'
 *    The date put into the file path depends on the specified 'timezone'. All available
 *    timezones can be seen at http://joda-time.sourceforge.net/timezones.html.
 * <p>
 * In the DSL, an HdfsWaitJob can be specified with:
 * <pre>
 *   hdfsWaitJob('jobName') {
 *     jvmClasspath './*:./lib/*'
 *     directoryPath 'data/derived/foo'   // Required
 *     directoryFreshness '15H 4M 10S'    // Required
 *     timeoutAfter '1D 11S'              // Required
 *     sleepTime '2M 10S'
 *     checkExactPath true
 *     timezone 'America/Los_Angeles'
 *     failOnTimeout true                 // Required
 *     depends 'job1'
 *   }
 * </pre>
 */
class HdfsWaitJob extends HadoopJavaJob {
  String dirPath;
  String freshness;
  String sleepInterval;
  String timeout;
  String timezone;
  Boolean exactPath;
  Boolean forceJobToFail;

  /**
   * Constructor for an HdfsWaitJob.
   *
   * @param jobName The job name
   */
  HdfsWaitJob(String jobName) {
    super(jobName);
    setJobProperty("job.class", "com.linkedin.hadoop.jobs.HdfsWaitJob");
  }

  /**
   * DSL method checkExactPath specifies whether the job should be checking for just
   * the existence of the dirPath in HDFS, or if it should behave like normal. This method
   * causes the property exactPath=value to be added to the job.
   *
   * @param exactPath The boolean value of whether to only check for the existence of the dirPath
   */
  @HadoopDslMethod
  void checkExactPath(Boolean exactPath) {
    this.exactPath = exactPath;
    setJobProperty("exactPath", exactPath);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  HdfsWaitJob clone() {
    return clone(new HdfsWaitJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
  */
  HdfsWaitJob clone(HdfsWaitJob cloneJob) {
    cloneJob.dirPath = dirPath;
    cloneJob.freshness = freshness;
    cloneJob.sleepInterval = sleepInterval;
    cloneJob.timeout = timeout;
    cloneJob.timezone = timezone;
    cloneJob.exactPath = exactPath;
    cloneJob.forceJobToFail = forceJobToFail;
    return ((HdfsWaitJob)super.clone(cloneJob));
  }

  /**
   * DSL method directoryFreshness specifies the limit on how old a file can be in the folder.
   * This method causes the property freshness=value to be added to the job.
   *
   * @param freshness The time frame in which a folder has to have been modified
   */
  @HadoopDslMethod
  void directoryFreshness(String freshness) {
    this.freshness = freshness;
    setJobProperty("freshness", freshness);
  }

  /**
   * DSL method directoryPath specifies the directory that the job should be searching in.
   * This method causes the property pathToDirectory=value to be added to the job.
   *
   * @param dirPath The directory path for this job 
   */
  @HadoopDslMethod
  void directoryPath(String dirPath) {
    this.dirPath = dirPath;
    setJobProperty("pathToDirectory", dirPath);
  }

  /**
   * DSL method failOnTimeout specifies whether the job fails or succeeds on timeout.
   * This method causes the property forceJobToFail=value to be added to the job.
   *
   * @param forceJobToFail The boolean value of whether the job succeeds or fails on timeout.
   */
  @HadoopDslMethod
  void failOnTimeout(Boolean forceJobToFail) {
    this.forceJobToFail = forceJobToFail;
    setJobProperty("forceJobToFail", forceJobToFail);
  }

  /**
   * DSL method sleepTime specifies the amount of time we wait between each check for
   * a fresh folder. If no value is specified, it defaults to 1 minute. This method
   * causes the property sleepInterval=value to be added to the job.
   *
   * @param sleepInterval The time we wait between each check for a fresh folder
   */
  @HadoopDslMethod
  void sleepTime(String sleepInterval) {
    this.sleepInterval = sleepInterval;
    setJobProperty("sleepInterval", sleepInterval);
  }

  /**
   * DSL method timeoutAfter specifies the amount of time this job can be running.
   * This method causes the property timeout=value to be added to the job.
   *
   * @param timeout The time until the job times out 
   */
  @HadoopDslMethod
  void timeoutAfter(String timeout) {
    this.timeout = timeout;
    setJobProperty("timeout", timeout);
  }

  /**
   * DSL method timezone specifies the timezone in which we should construct
   * the file path. This method causes the property timezone=value to be added to the job.
   *
   * @param timezone The timezone for constructing the file path
   */
  @HadoopDslMethod
  void timezone(String timezone) {
    this.timezone = timezone;
    setJobProperty("timezone", timezone);
  }
}