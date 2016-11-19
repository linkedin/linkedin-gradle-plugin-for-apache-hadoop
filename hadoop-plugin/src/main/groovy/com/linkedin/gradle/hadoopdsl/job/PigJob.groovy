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
 * Job class for type=pig jobs.
 * <p>
 * In the DSL, a PigJob can be specified with:
 * <pre>
 *   pigJob('jobName') {
 *     uses 'myScript.pig'  // Required
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
class PigJob extends HadoopJavaProcessJob {
  Map<String, Object> parameters;
  String script;

  /**
   * Constructor for a PigJob.
   *
   * @param jobName The job name
   */
  PigJob(String jobName) {
    super(jobName);
    parameters = new LinkedHashMap<String, Object>();
    setJobProperty("type", "pig");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  PigJob clone() {
    return clone(new PigJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  PigJob clone(PigJob cloneJob) {
    cloneJob.parameters.putAll(parameters);
    cloneJob.script = script;
    return ((PigJob)super.clone(cloneJob));
  }

  /**
   * DSL method to specify HDFS paths read by the job. In addition to the functionality of the base
   * class, for Pig jobs, using this method will cause a Pig parameter to be added to the job
   * properties for each HDFS path specified.
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job reads
   */
  @HadoopDslMethod
  @Override
  void reads(Map args) {
    super.reads(args);

    // For Pig jobs, additionally emit a Pig script parameter
    Map<String, String> files = args.files;
    files.each { String name, String value ->
      setParameter(name, value);
    }
  }

  /**
   * DSL method to specify HDFS paths written by the job. In addition to the functionality of the
   * base class, for Pig jobs, using this method will cause a Pig parameter to be added to the job
   * properties for each HDFS path specified.
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job reads
   */
  @HadoopDslMethod
  @Override
  void writes(Map args) {
    super.writes(args);

    // For Pig jobs, additionally emit a Pig script parameter
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
   * Additionally for PigJobs, you can specify Pig parameters by using the syntax
   * "set parameters: [ ... ]". For each parameter you set, a line of the form param.key=val will
   * be written to the job file.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set;
   *   or a key 'jvmProperties' with a map value that specifies the JVM properties to set;
   *   or a key 'confProperties' with a map value that specifies the Hadoop job configuration properties to set;
   *   or a key 'parameters' with a map value that specifies the Pig parameters to set
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
   * DSL parameter method sets Pig parameters for the job. When the job file is built, job
   * properties of the form param.name=value are added to the job file. With your parameters
   * set this way, you can use $name in your Pig script and get the associated value.
   *
   * @param name The Pig parameter name
   * @param value The Pig parameter value
   */
  void setParameter(String name, Object value) {
    parameters.put(name, value);
    setJobProperty("param.${name}", value);
  }

  /**
   * DSL method uses specifies the Pig script for the job. The specified value can be either an
   * absolute or relative path to the script file. This method causes the property pig.script=value
   * to be added the job. This method is required to build the job.
   *
   * @param script The Pig script for the job
   */
  @HadoopDslMethod
  @Override
  void uses(String script) {
    this.script = script;
    setJobProperty("pig.script", script);
  }
}