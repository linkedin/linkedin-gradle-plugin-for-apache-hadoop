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

/**
 * @deprecated JavaJob has been deprecated in favor of HadoopJavaJob or JavaProcessJob.
 *
 * Job class for type=java jobs.
 * <p>
 * According to the Azkaban documentation at http://azkaban.github.io/azkaban/docs/2.5/#job-types,
 * this type of job has been deprecated. Either javaprocess or hadoopJava-type jobs should be used
 * instead of java-type jobs.
 * <p>
 * In the DSL, a JavaJob can be specified with:
 * <pre>
 *   javaJob('jobName') {
 *     uses 'com.linkedin.foo.HelloJavaJob'  // Required
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
@Deprecated
class JavaJob extends HadoopJavaProcessJob {
  String jobClass;

  /**
   * Constructor for a JavaJob.
   *
   * @param jobName The job name
   */
  JavaJob(String jobName) {
    super(jobName);
    setJobProperty("type", "java");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  JavaJob clone() {
    return clone(new JavaJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  JavaJob clone(JavaJob cloneJob) {
    cloneJob.jobClass = jobClass;
    return ((JavaJob)super.clone(cloneJob));
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