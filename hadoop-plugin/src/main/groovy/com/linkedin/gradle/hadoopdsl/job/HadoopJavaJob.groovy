/*
 * Copyright 2014 LinkedIn Corp.
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
 * Job class for type=hadoopJava jobs.
 * <p>
 * According to the Azkaban documentation at http://azkaban.github.io/azkaban/docs/2.5/#job-types,
 * this is the job type you should if you need to have a Java job that needs to acquire a secure
 * token to talk to your Hadoop cluster. If your job does not need to securely talk to Hadoop, use
 * a javaprocess-type job instead.
 * <p>
 * In the DSL, a HadoopJavaJob can be specified with:
 * <pre>
 *   hadoopJavaJob('jobName') {
 *     uses 'com.linkedin.foo.HelloHadoopJavaJob'  // Required
 *     reads files: [
 *       'foo' : '/data/databases/foo'
 *     ]
 *     writes files: [
 *       'bazz' : '/user/bazz/foobar'
 *     ]
 *     set confProperties: [
 *       'mapred.property1' : 'value1',
 *       'mapred.property2' : 'value2'
 *     ]
 *     set properties: [
 *       'propertyName1' : 'propertyValue1'
 *     ]
 *     queue 'marathon'
 *   }
 * </pre>
 */
class HadoopJavaJob extends HadoopJavaProcessJob {
  String jobClass;

  /**
   * Constructor for a HadoopJavaJob.
   *
   * @param jobName The job name
   */
  HadoopJavaJob(String jobName) {
    super(jobName);
    setJobProperty("type", "hadoopJava");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  HadoopJavaJob clone() {
    return clone(new HadoopJavaJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  @Override
  HadoopJavaJob clone(HadoopJavaJob cloneJob) {
    cloneJob.jobClass = jobClass;
    return super.clone(cloneJob);
  }

  /**
   * DSL method uses specifies the Java class for the job. This method causes the property
   * job.class=value to be added the job. This method is required to build the job.
   *
   * @param jobClass The Java class for the job
   */
  @Override
  void uses(String jobClass) {
    this.jobClass = jobClass;
    setJobProperty("job.class", jobClass);
  }
}