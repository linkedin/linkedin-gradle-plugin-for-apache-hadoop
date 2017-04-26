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
import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.PropertySet;
import com.linkedin.gradle.hadoopdsl.Workflow;

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
  Map<String, Object> jobProperties;
  Map<String, String> reading;
  Map<String, String> writing;
  Set<String> requiredParameters;

  /**
   * Base constructor for a Job.
   *
   * @param jobName The job name
   */
  Job(String jobName) {
    dependencyNames = new LinkedHashSet<String>();
    jobProperties = new LinkedHashMap<String, Object>();
    name = jobName;
    reading = new LinkedHashMap<String, String>();
    writing = new LinkedHashMap<String, String>();
    requiredParameters = new LinkedHashSet<String>();
  }

  /**
   * DSL baseProperties method. Sets the name of the BasePropertySet from which to get the base
   * properties.
   *
   * @param propertySetName The name of the base PropertySet
   */
  @HadoopDslMethod
  void baseProperties(String propertySetName) {
    this.basePropertySetName = propertySetName;
  }

  /**
   * Helper method to construct the job file name to use in the dependencies property when
   * declaring a dependency for this job.
   *
   * @param targetName The target job name or child workflow name declared as a dependency of this job
   * @param parentScope The parent scope in which the job (and its dependency) is bound
   * @return The name of the job file to use in the dependencies property for this job
   */
  String buildDependencyFileName(String targetName, NamedScope parentScope) {
    Object dependency = parentScope.thisLevel.get(targetName);

    // In the Hadoop DSL we have the invariant that dependencies must name objects bound in scope
    // at the same level as the object declaring the dependency. However, if you are programming
    // against the API you could previously get around this invariant and we would simply emit your
    // named dependency. For backwards compatibility, preserve this behavior.
    if (dependency == null) {
      return buildFileName(parentScope, targetName);
    }

    // Otherwise if you name a dependency on a job or a child workflow, generate the correct .job
    // file name for that dependency.
    if (dependency instanceof Job) {
      return ((Job)dependency).buildFileName(parentScope);
    } else if (dependency instanceof Workflow) {
      Workflow dependencyFlow = (Workflow)dependency;

      if (dependencyFlow.isGrouping) {
        return dependencyFlow.subFlowJob.buildFileName(dependencyFlow.scope);
      } else {
        return dependencyFlow.launchJob.buildFileName(dependencyFlow.scope);
      }
    }

    // For backwards compatibility, if for some reason they used the API to name a dependency on
    // something else, just emit the dependency the same way we used to do it before.
    return buildFileName(parentScope, targetName);
  }

  /**
   * Helper method to construct the file name to use for the job file. In Azkaban, all job files
   * must have unique names.
   * <p>
   * See the other overload for this method for information about how the job file name is formed.
   *
   * @param parentScope The parent scope in which the job is bound
   * @return The name to use when generating the job file
   */
  String buildFileName(NamedScope parentScope) {
    return buildFileName(parentScope, name);
  }

  /**
   * Helper method to construct the file name to use for the given job name. In Azkaban, all job
   * files must have unique names.
   * <p>
   * We'll use the fully-qualified name to help us form the file name. However, to make the file
   * name more readable, we'll use underscores and drop the hadoop scope prefix from the file name.
   * <p>
   * As an example, if the job named "job1" is nested inside the workflow "testWorkflow" in Hadoop
   * scope, the file name will be "testWorkflow_job1".
   *
   * @param parentScope The parent scope in which the job is bound
   * @return The name to use when generating the job file
   */
  String buildFileName(NamedScope parentScope, String jobName) {
    return cleanFileName(getQualifiedName(parentScope, jobName));
  }

  /**
   * Builds the job properties that go into the generated job file.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jobProperties correctly.
   *
   * @param parentScope The parent scope in which to lookup the base properties
   * @return The job properties map that holds all the properties that will go into the built job file
   */
  Map<String, String> buildProperties(NamedScope parentScope) {
    if (basePropertySetName != null) {
      PropertySet propertySet = (PropertySet)parentScope.lookup(basePropertySetName);
      propertySet.fillProperties();    // The base property set looks up its base properties in its own scope
      unionProperties(propertySet);
    }

    Map<String, String> allProperties = new LinkedHashMap<String, String>();

    jobProperties.each { String key, Object val ->
      allProperties.put(key, val.toString());
    }

    if (dependencyNames.size() > 0) {
      allProperties["dependencies"] = dependencyNames.collect { String targetName -> buildDependencyFileName(targetName, parentScope) }.join(",");
    }

    return allProperties;
  }

  /**
   * Helper routine to improve job file name readability by dropping the hadoop prefix and
   * replacing dots with underscores.
   *
   * @param fileName The file name to clean up
   * @return The clean file name
   */
  String cleanFileName(String fileName) {
    return fileName.replaceFirst("hadoop.", "").replace('.', '_');
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
    cloneJob.reading.putAll(reading);
    cloneJob.writing.putAll(writing);
    cloneJob.requiredParameters.addAll(requiredParameters);
    return cloneJob;
  }

  /**
   * DSL depends method declares the targets on which this job depends.
   *
   * @param targetNames The variable-length targets on which this job depends
   */
  @HadoopDslMethod
  void depends(String... targetNames) {
    depends(targetNames.toList());
  }

  /**
   * DSL depends method declares the targets on which this job depends.
   *
   * @param targetNames The list of targets on which this job depends
   */
  @HadoopDslMethod
  void depends(List<String> targetNames) {
    depends(false, targetNames);
  }

  /**
   * DSL depends method declares the targets on which this job depends.
   *
   * @param clear Whether or not to clear the previously declared dependencies
   * @param targetNames The list of targets on which this job depends
   */
  @HadoopDslMethod
  void depends(boolean clear, List<String> targetNames) {
    if (clear) {
      dependencyNames.clear();
    }
    dependencyNames.addAll(targetNames);
  }

  /**
   * DSL depends method declares the targets on which this job depends.
   *
   * @param args Args whose optional key 'clear' specifies whether or not to clear the previously declared dependencies
   *                  and required key 'targetNames' specifies the list of targets on which this job depends
   */
  @HadoopDslMethod
  void depends(Map args) {
    boolean clear = args.containsKey("clear") ? args["clear"] : false;
    List<String> targetNames = (List<String>)args["targetNames"];
    depends(clear, targetNames);
  }

  /**
   * Returns the fully-qualified name for this job.
   *
   * @param parentScope The parent scope in which the job is bound
   * @return The fully-qualified name for the job
   */
  String getQualifiedName(NamedScope parentScope) {
    return getQualifiedName(parentScope, name);
  }

  /**
   * Helper method to get the fully-qualified name given a particular job name.
   *
   * @param jobName The job name for which to generate a fully-qualified name
   * @param parentScope The parent scope in which the job is bound
   * @return The fully-qualified name for the job
   */
  String getQualifiedName(NamedScope parentScope, String jobName) {
    return (parentScope == null) ? name : "${parentScope.getQualifiedName()}.${jobName}";
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
  @HadoopDslMethod
  void reads(Map args) {
    Map<String, String> files = args.files;
    for (Map.Entry<String, String> entry : files) {
      reading.put(entry.key, entry.value);
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
  @HadoopDslMethod
  void writes(Map args) {
    Map<String, String> files = args.files;
    for (Map.Entry<String, String> entry : files) {
      writing.put(entry.key, entry.value);
      setJobProperty(entry.key, entry.value);
    }
  }

  /**
   * DSL method to specify job properties to set on the job. Specifying job properties causes lines
   * of the form key=val to be written to the job file.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set
   */
  @HadoopDslMethod
  void set(Map args) {
    if (args.containsKey("properties")) {
      Map<String, Object> properties = args.properties;
      properties.each { String name, Object value ->
        setJobProperty(name, value);
      }
    }
  }

  /**
   * DSL method to specify required parameters for a job.
   * Certain parameters must be set explicitly in oder for the job to run correctly in production.
   * Specifying those parameters here provides a clear and explicit expectation for the users of the job.
   * For example, specifying "required parameters" in a job template,
   * and then enforcing the setting of those parameters in a job cloned from this template.
   * This helps avoid production errors in an early stage, such as those caused by default parameters.
   *
   * @param args Args whose key 'parameters' has a list of value specifying the required parameters
   */
  @HadoopDslMethod
  void required(Map args) {
    List<String> parameters = args.parameters;
    for (String entry : parameters) {
      requiredParameters.add(entry);
    }
  }
  /**
   * Sets the given job property. Setting a job property causes a line of the form key=val to be
   * written to the job file.
   *
   * @param name The job property to set
   * @param value The job property value
   */
  void setJobProperty(String name, Object value) {
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
    propertySet.jobProperties.each { String name, Object value ->
      if (!jobProperties.containsKey(name)) {
        setJobProperty(name, value);
      }
    }
  }
}
