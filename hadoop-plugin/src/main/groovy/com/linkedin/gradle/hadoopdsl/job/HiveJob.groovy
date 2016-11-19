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
 * Job class for type=hive jobs.
 * <p>
 * In the DSL, a HiveJob can be specified with:
 * <pre>
 *   hiveJob('jobName') {
 *     uses 'hello.q'  // Required
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
 *     set parameters: [
 *       'param1' : 'val1',
 *       'param2' : 'val2'
 *     ]
 *     queue 'marathon'
 *   }
 * </pre>
 */
class HiveJob extends HadoopJavaProcessJob {
  Map<String, Object> parameters;
  String script;

  /**
   * Constructor for a HiveJob.
   *
   * @param jobName The job name
   */
  HiveJob(String jobName) {
    super(jobName);
    this.parameters = new LinkedHashMap<String, Object>();
    setJobProperty("type", "hive");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  HiveJob clone() {
    return clone(new HiveJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  HiveJob clone(HiveJob cloneJob) {
    cloneJob.parameters.putAll(parameters);
    cloneJob.script = script;
    return ((HiveJob)super.clone(cloneJob));
  }

  /**
   * DSL method to specify HDFS paths read by the job. In addition to the functionality of the base
   * class, for Hive jobs, using this method will cause a Hive parameter to be added to the job
   * properties for each HDFS path specified.
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job reads
   */
  @HadoopDslMethod
  @Override
  void reads(Map args) {
    super.reads(args);

    // For Hive jobs, additionally emit a Hive script parameter
    Map<String, String> files = args.files;
    files.each { String name, String value ->
      setParameter(name, value);
    }
  }

  /**
   * DSL method to specify HDFS paths written by the job. In addition to the functionality of the
   * base class, for Hive jobs, using this method will cause a Hive parameter to be added to the
   * job properties for each HDFS path specified.
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job reads
   */
  @HadoopDslMethod
  @Override
  void writes(Map args) {
    super.writes(args);

    // For Hive jobs, additionally emit a Hive script parameter
    Map<String, String> files = args.files;
    files.each { String name, String value ->
      setParameter(name, value);
    }
  }

  /**
   * DSL method to specify job properties to set on the job. Specifying job properties causes lines
   * of the form key=val to be written to the job file.
   * <p>
   * Additionally for JavaProcessJobs, you can specify JVM properties by using the syntax
   * "set jvmProperties: [ ... ]", which causes a line of the form
   * jvm.args=-Dkey1=val1 ... -DkeyN=valN to be written to the job file.
   * <p>
   * Additionally for HadoopJavaProcessJob subclasses, you can specify Hadoop job configuration
   * properties by using the syntax "set confProperties: [ ... ]", which causes lines of the form
   * hadoop-inject.key=val to be written to the job file.
   * <p>
   * Additionally for HiveJobs, you can specify Hive parameters by using the syntax
   * "set parameters: [ ... ]". For each parameter you set, a line of the form hivevar.key=val will
   * be written to the job file.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set;
   *   or a key 'jvmProperties' with a map value that specifies the JVM properties to set;
   *   or a key 'confProperties' with a map value that specifies the Hadoop job configuration properties to set;
   *   or a key 'parameters' with a map value that specifies the Hive parameters to set
   */
  @HadoopDslMethod
  @Override
  void set(Map args) {
    super.set(args);

    if (args.containsKey("parameters")) {
      Map<String, Object> parameters = args.parameters;
      parameters.each { String name, Object value ->
        setParameter(name, value);
      }
    }
  }

  /**
   * DSL parameter method sets Hive parameters for the job. When the job file is built, job
   * properties of the form hivevar.name=value are added to the job file.
   *
   * @param name The Hive parameter name
   * @param value The Hive parameter value
   */
  void setParameter(String name, Object value) {
    parameters.put(name, value);
    setJobProperty("hivevar.${name}", value);
  }

  /**
   * DSL method uses specifies the Hive script for the job. The specified value can be either an
   * absolute or relative path to the script file. This method causes the property
   * hive.script=value to be added the job. This method is required to build the job.
   *
   * @param script The Hive script for the job
   */
  @HadoopDslMethod
  @Override
  void uses(String script) {
    this.script = script;
    setJobProperty("hive.script", script);
  }
}