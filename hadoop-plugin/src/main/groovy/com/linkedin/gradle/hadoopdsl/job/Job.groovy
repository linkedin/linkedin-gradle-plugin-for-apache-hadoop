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

import com.linkedin.gradle.hadoopdsl.BasePropertySet;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.PropertySet;

/**
 * Base class for all Hadoop DSL job types.
 * <p>
 * In the DSL, a Job can be specified with:
 * <pre>
 *   job('jobName') {
 *     reads files: [
 *       'foo' : '/data/databases/foo',
 *       'bar' : '/data/databases/bar',
 *     ]
 *     writes files: [
 *       'bazz' : '/user/bazz/foobar'
 *     ]
 *     set properties: [
 *       'propertyName1' : 'propertyValue1'
 *     ]
 *     depends 'job1'
 *   }
 * </pre>
 */
class Job {
  String basePropertySetName;
  String name;
  Set<String> dependencyNames;
  Map<String, String> jobProperties;
  List<String> reading;
  List<String> writing;

  /**
   * Base constructor for a Job.
   *
   * @param jobName The job name
   */
  Job(String jobName) {
    dependencyNames = new LinkedHashSet<String>();
    jobProperties = new LinkedHashMap<String, String>();
    name = jobName;
    reading = new ArrayList<String>();
    writing = new ArrayList<String>();
  }

  /**
   * Sets the name of the base PropertySet from which to get the base properties.
   *
   * @param propertySetName The name of the base PropertySet
   */
  void baseProperties(String propertySetName) {
    this.basePropertySetName = propertySetName;
  }

  /**
   * Helper method to construct the name to use with the job file. By default, the name constructed
   * is "${parentScopeName}_${name}", but subclasses can override this method if they need to
   * customize how the name is constructed.
   * <p>
   * As an example, if the job named "job1" is nested inside the workflow "testWorkflow", this
   * method will form the name "testWorkflow_job1" as the file name.
   *
   * @param name The job name
   * @param parentScopeName The fully-qualified name of the scope in which the job is bound
   * @return The name to use when generating the job file
   */
  String buildFileName(String name, String parentScopeName) {
    return parentScopeName == null ? name : "${parentScopeName}_${name}";
  }

  /**
   * Helper overload of the buildProperties method that specifically handles adding the job
   * dependencies to the job properties map. Subclasses can override this method if they want to
   * customize how the dependencies property is generated.
   *
   * @param parentScope The parent scope in which to lookup the base properties
   * @param parentScopeName The fully-qualified name of the scope in which the job is bound
   * @return The job properties map that holds all the properties that will go into the built job file
   */
  Map<String, String> buildProperties(NamedScope parentScope, String parentScopeName) {
    Map<String, String> allProperties = buildProperties(parentScope);
    if (dependencyNames.size() > 0) {
      allProperties["dependencies"] = dependencyNames.collect() { String jobName -> return (parentScopeName == null) ? jobName : "${parentScopeName}_${jobName}"; }.join(",");
    }
    return allProperties;
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and jobProperties correctly.
   *
   * @param parentScope The parent scope in which to lookup the base properties
   * @return The job properties map that holds all the properties that will go into the built job file
   */
  Map<String, String> buildProperties(NamedScope parentScope) {
    if (basePropertySetName != null) {
      PropertySet propertySet = (PropertySet) parentScope.lookup(basePropertySetName);
      propertySet.fillProperties();    // The base property set looks up its base properties in its own scope
      unionProperties(propertySet);
    }

    Map<String, String> allProperties = new LinkedHashMap<String, String>();
    allProperties.putAll(jobProperties);
    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  Job clone() {
    return clone(new Job(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  Job clone(Job cloneJob) {
    cloneJob.basePropertySetName = basePropertySetName;
    cloneJob.dependencyNames.addAll(dependencyNames);
    cloneJob.jobProperties.putAll(jobProperties);
    cloneJob.reading.addAll(reading);
    cloneJob.writing.addAll(writing);
    return cloneJob;
  }

  /**
   * DSL depends method declares the jobs on which this job depends.
   *
   * @param jobNames The list of job names on which this job depends
   */
  void depends(String... jobNames) {
    dependencyNames.addAll(jobNames.toList());
  }

  /**
   * DSL method to specify HDFS paths read by the job. When you use this method, the static checker
   * will verify that this job is dependent or transitively dependent on any jobs that write paths
   * that this job reads. This is an important race condition in workflows that can be completely
   * eliminated with this static check.
   * <p>
   * Using this method additionally causes lines of the form form key=hdfsPath to be written to
   * the job file (i.e. the keys you use are available as job properties).
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job reads
   */
  void reads(Map args) {
    Map<String, String> files = args.files;
    for (Map.Entry<String, String> entry : files) {
      reading.add(entry.value);
      setJobProperty(entry.key, entry.value);
    }
  }

  /**
   * DSL method to specify the HDFS paths written by the job. When you use this method, the static
   * checker will verify that any jobs that read paths this job writes are dependent or transitively
   * dependent on this job. This is an important race condition in workflows that can be completely
   * eliminated with this static check.
   * <p>
   * Using this method additionally causes lines of the form form key=hdfsPath to be written to
   * the job file (i.e. the keys you use are available as job properties).
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job writes
   */
  void writes(Map args) {
    Map<String, String> files = args.files;
    for (Map.Entry<String, String> entry : files) {
      writing.add(entry.value);
      setJobProperty(entry.key, entry.value);
    }
  }

  /**
   * DSL method to specify job properties to set on the job. Specifying job properties causes lines
   * of the form key=val to be written to the job file.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set
   */
  void set(Map args) {
    if (args.containsKey("properties")) {
      Map<String, String> properties = args.properties;
      properties.each() { String name, String value ->
        setJobProperty(name, value);
      }
    }
  }

  /**
   * Sets the given job property. Setting a job property causes a line of the form key=val to be
   * written to the job file.
   *
   * @param name The job property to set
   * @param value The job property value
   */
  void setJobProperty(String name, String value) {
    jobProperties.put(name, value);
  }

  /**
   * Returns a string representation of the job.
   *
   * @return A string representation of the job
   */
  @Override
  String toString() {
    return "(Job: name = ${name})";
  }

  /**
   * Adds any properties set on the given BasePropertySet (that are not already set), so that the
   * final set of properties set on the job represents the union of the properties.
   *
   * @param propertySet The BasePropertySet to union to the job
   */
  void unionProperties(BasePropertySet propertySet) {
    propertySet.jobProperties.each() { String name, String value ->
      if (!jobProperties.containsKey(name)) {
        setJobProperty(name, value);
      }
    }
  }
}