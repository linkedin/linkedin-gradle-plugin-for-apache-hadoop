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

/**
 * Abstract base class for JavaProcessJob subclasses that are for Hadoop, such as HadoopJava, Pig
 * and Hive job types. This class contains common functionality between all of these concrete job
 * types. It is not intended to be instantiated by the user in DSL code.
 * <p>
 * All concrete classes that extend HadoopJavaProcessJob support the following syntax:
 * <pre>
 *   concreteHadoopJavaProcessJob('jobName') {
 *     caches files: [
 *       'foo.txt': '/user/bar/foo.txt'
 *     ]
 *     cachesArchives files: [
 *       'foo.zip': '/user/bar/foo.zip',
 *       'bazz.tgz': '/user/bar/bazz.tgz'
 *     ]
 *     set hadoopProperties: [
 *       'mapred.property1' : 'value1',
 *       'mapred.property2' : 'value2'
 *     ]
 *     depends 'job1'
 *     queue 'queueName'
 *   }
 * </pre>
 */
abstract class HadoopJavaProcessJob extends JavaProcessJob {
  Map<String, String> cacheArchives;
  Map<String, String> cacheFiles;
  Map<String, Object> confProperties;
  String queueName;

  /**
   * Constructor for a HadoopJavaProcessJob.
   *
   * @param jobName The job name
   */
  HadoopJavaProcessJob(String jobName) {
    super(jobName);
    this.cacheArchives = new LinkedHashMap<String, String>();
    this.cacheFiles = new LinkedHashMap<String, String>();
    this.confProperties = new LinkedHashMap<String, Object>();
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
  @Override
  Map<String, String> buildProperties(NamedScope parentScope) {
    if (cacheArchives.size() > 0) {
      String mrCacheArchives = cacheArchives.collect { symLink, pathName -> return "${pathName}#${symLink}"; }.join(",")
      setConfProperty("mapred.cache.archives", mrCacheArchives);
      setConfProperty("mapred.create.symlink", "yes");
    }

    if (cacheFiles.size() > 0) {
      String mrCacheFiles = cacheFiles.collect { symLink, pathName -> return "${pathName}#${symLink}"; }.join(",")
      setConfProperty("mapred.cache.files", mrCacheFiles);
      setConfProperty("mapred.create.symlink", "yes");
    }

    return super.buildProperties(parentScope);
  }

  /**
   * DSL method to specify files to add to Distributed Cache.
   * <p>
   * Using this DSL method sets the conf properties mapred.cache.files and
   * mapred.create.symlink=yes. These properties cause the specified files (which must already be
   * present on HDFS) to be added to DistributedCache and propagated to each of the tasks. A
   * symlink will be created in the task working directory that points to the file.
   *
   * @param args Args whose key 'files' has a map value specifying the files to cache
   */
  @HadoopDslMethod
  void caches(Map args) {
    Map<String, String> files = args.files;
    if (files.size() == 0) {
      return;
    }

    reads(args);

    for (Map.Entry<String, String> entry : files.entrySet()) {
      String symLink = entry.key;
      String pathName = entry.value;
      cacheFiles.put(symLink, pathName);
    }
  }

  /**
   * DSL method to specify archives to add to Distributed Cache. Valid archives to use with this
   * method are .zip, .tgz, .tar.gz, .tar and .jar. Any other file type will result in an
   * exception.
   * <p>
   * Using this DSL method sets the conf properties mapred.cache.archives and
   * mapred.create.symlink=yes. These properties cause the specified archives (which must already
   * be present on HDFS) to be added to DistributedCache and propagated to each of the tasks. A
   * symlink will be created in the task working directory that points to the exploded archive
   * directory.
   *
   * @param args Args whose key 'files' has a map value specifying the archives to cache
   */
  @HadoopDslMethod
  void cachesArchive(Map args) {
    Map<String, String> files = args.files;
    if (files.size() == 0) {
      return;
    }

    reads(args);
    List<String> archiveExt = Arrays.asList(".zip", ".tgz", ".tar.gz", ".tar", ".jar");

    for (Map.Entry<String, String> entry : files.entrySet()) {
      String symLink = entry.key;
      String pathName = entry.value;
      String lowerPath = pathName.toLowerCase();

      boolean found = archiveExt.any { String ext -> return lowerPath.endsWith(ext); };
      if (!found) {
        throw new Exception("File given to cachesArchive must be one of: " + archiveExt.toString());
      }
      cacheArchives.put(symLink, pathName);
    }
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  abstract HadoopJavaProcessJob clone();

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  HadoopJavaProcessJob clone(HadoopJavaProcessJob cloneJob) {
    cloneJob.cacheArchives.putAll(cacheArchives);
    cloneJob.cacheFiles.putAll(cacheFiles);
    cloneJob.confProperties.putAll(confProperties);
    cloneJob.queueName = queueName;
    return ((HadoopJavaProcessJob)super.clone(cloneJob));
  }

  /**
   * DSL queue method to declare the queue in which this job should run. This does the following:
   * <ul>
   *   <li>Sets the property mapred.job.queue.name in the job file</li>
   *   <li>Sets the property hadoop-inject.mapred.job.queue.name in the job file</li>
   *   <li>Adds "-D mapred.job.queue.name=val" to the property main.args</li>
   * </ul>
   *
   * @param queueName The name of the queue in which this job should run
   */
  @HadoopDslMethod
  void queue(String queueName) {
    this.queueName = queueName;
    setConfProperty("mapred.job.queue.name", queueName);
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
   * hadoop-inject.key=val to be written to the job file. To make this more clear, we've added the
   * syntax "set hadoopProperties: [ ... ]" as a synonym for setting confProperties.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set;
   *   or a key 'jvmProperties' with a map value that specifies the JVM properties to set;
   *   or a key 'confProperties' with a map value that specifies the Hadoop job configuration properties to set
   *   or a key 'hadoopProperties' that is a synonym for 'confProperties'
   */
  @HadoopDslMethod
  @Override
  void set(Map args) {
    super.set(args);

    if (args.containsKey("confProperties")) {
      Map<String, Object> confProperties = args.confProperties;
      confProperties.each { String name, Object value ->
        setConfProperty(name, value);
      }
    }
    if (args.containsKey("hadoopProperties")) {
      Map<String, Object> hadoopProperties = args.hadoopProperties;
      hadoopProperties.each { String name, Object value ->
        setConfProperty(name, value);
      }
    }
  }

  /**
   * Sets the given Hadoop job configuration property. For a given key and value, this method
   * causes the line hadoop-inject.key=val to be added to the job file.
   *
   * @param name The Hadoop job configuration property to set
   * @param value The Hadoop job configuration property value
   */
  void setConfProperty(String name, Object value) {
    confProperties.put(name, value);
    setJobProperty("hadoop-inject.${name}", value);
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

    propertySet.confProperties.each { String name, Object value ->
      if (!confProperties.containsKey(name)) {
        setConfProperty(name, value);
      }
    }
  }
}