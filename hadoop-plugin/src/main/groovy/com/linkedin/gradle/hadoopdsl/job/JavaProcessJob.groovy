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

import com.linkedin.gradle.hadoopdsl.BasePropertySet;
import com.linkedin.gradle.hadoopdsl.NamedScope;

/**
 * Job class for type=javaprocess jobs.
 * <p>
 * According to the Azkaban documentation at http://azkaban.github.io/azkaban/docs/2.5/#job-types,
 * this is the job type you should use for Java-only jobs that do not need to acquire a secure
 * token to your Hadoop cluster. This class is also the base class for all job types that run with
 * a JVM. It exposes functionality to set JVM properties.
 * <p>
 * In the DSL, a JavaProcessJob can be specified with:
 * <pre>
 *   javaProcessJob('jobName') {
 *     uses 'com.linkedin.foo.HelloJavaProcessJob'  // Required
 *     jvmClasspath './*:./lib/*'
 *     set jvmProperties: [
 *       'jvmPropertyName1' : 'jvmPropertyValue1',
 *       'jvmPropertyName2' : 'jvmPropertyValue2'
 *     ]
 *     Xms 96
 *     Xmx 384
 *   }
 * </pre>
 */
class JavaProcessJob extends Job {
  String javaClass;
  String javaClasspath;
  Map<String, Object> jvmProperties;
  Integer xms;
  Integer xmx;

  /**
   * Constructor for a JavaProcessJob.
   *
   * @param jobName The job name
   */
  JavaProcessJob(String jobName) {
    super(jobName);
    jvmProperties = new LinkedHashMap<String, Object>();
    setJobProperty("type", "javaprocess");
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jobProperties correctly.
   *
   * @param parentScope The parent scope in which to lookup the base properties
   * @return The job properties map that holds all the properties that will go into the built job file
   */
  @Override
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> allProperties = super.buildProperties(parentScope);

    if (jvmProperties.size() > 0) {
      String jvmArgs = jvmProperties.collect { String key, Object val -> return "-D${key}=${val.toString()}" }.join(" ");
      allProperties["jvm.args"] = jvmArgs;
    }

    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  JavaProcessJob clone() {
    return clone(new JavaProcessJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  JavaProcessJob clone(JavaProcessJob cloneJob) {
    cloneJob.javaClass = javaClass;
    cloneJob.javaClasspath = javaClasspath;
    cloneJob.jvmProperties.putAll(jvmProperties);
    cloneJob.xms = xms;
    cloneJob.xmx = xmx;
    return super.clone(cloneJob);
  }

  /**
   * Sets the classpath for the JavaProcessJob. Note that this sets the classpath for the client
   * process only. In particular, this does not set the classpath for map and reduce tasks of
   * Hadoop jobs.
   *
   * @param javaClasspath The classpath for the client process
   */
  void jvmClasspath(String javaClasspath) {
    this.javaClasspath = javaClasspath;
    setJobProperty("classpath", javaClasspath);
  }

  /**
   * DSL method to specify job properties to set on the job. Specifying job properties causes lines
   * of the form key=val to be written to the job file.
   * <p>
   * Additionally for JavaProcessJobs, you can specify JVM properties by using the syntax
   * "set jvmProperties: [ ... ]", which causes a line of the form
   * jvm.args=-Dkey1=val1 ... -DkeyN=valN to be written to the job file.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set;
   *   or a key 'jvmProperties' with a map value that specifies the JVM properties to set
   */
  @Override
  void set(Map args) {
    super.set(args);

    if (args.containsKey("jvmProperties")) {
      Map<String, Object> jvmProperties = args.jvmProperties;
      jvmProperties.each { name, value ->
        setJvmProperty(name, value);
      }
    }
  }

  /**
   * Sets the given JVM property.
   *
   * @param name The JVM property name to set
   * @param value The JVM property value
   */
  void setJvmProperty(String name, Object value) {
    jvmProperties.put(name, value);
  }

  /**
   * Adds any properties set on the given BasePropertySet (that are not already set), so that the
   * final set of properties set on the job represents the union of the properties.
   *
   * @param propertySet The BasePropertySet to union to the job
   */
  @Override
  void unionProperties(BasePropertySet propertySet) {
    super.unionProperties(propertySet);

    propertySet.jvmProperties.each { String name, Object value ->
      if (!jvmProperties.containsKey(name)) {
        setJvmProperty(name, value);
      }
    }
  }

  /**
   * DSL method uses specifies the Java class for the job. This method causes the property
   * java.class=value to be added the job. This method is required to build the job.
   *
   * @param javaClass The Java class for the job
   */
  void uses(String javaClass) {
    this.javaClass = javaClass;
    setJobProperty("java.class", javaClass);
  }

  /**
   * Sets the Azkaban -Xms for the JavaProcessJob. Note that this sets -Xms for the Azkaban process
   * only. In particular, this does not set -Xms for map and reduce tasks of Hadoop jobs. This
   * method causes the line Xms=valM to be written to the job file.
   *
   * @param xmsMb How many megabytes to set with -Xms for the Azkaban process
   */
  void Xms(int xmsMb) {
    if (xmsMb <= 0) {
      throw new Exception("You must set Xms to be a positive number");
    }
    xms = xmsMb;
    setJobProperty("Xms", "${xms.toString()}M");
  }

  /**
   * Sets the Azkaban -Xmx for the JavaProcessJob. Note that this sets -Xmx for the Azkaban process
   * only. In particular, this does not set -Xmx for map and reduce tasks of Hadoop jobs. This
   * method causes the line Xmx=valM to be written to the job file.
   *
   * @param xmxMb How many megabytes to set with -Xmx for the Azkaban process
   */
  void Xmx(int xmxMb) {
    if (xmxMb <= 0) {
      throw new Exception("You must set Xmx to be a positive number");
    }
    xmx = xmxMb;
    setJobProperty("Xmx", "${xmx.toString()}M");
  }
}