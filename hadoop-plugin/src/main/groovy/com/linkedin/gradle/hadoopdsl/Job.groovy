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
package com.linkedin.gradle.hadoopdsl;

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
   * Helper method to construct the name to use with the job file. By default, the name constructed
   * is "${parentScope}_${name}", but subclasses can override this method if they need to customize
   * how the name is constructed.
   * <p>
   * As an example, if the job named "job1" is nested inside the workflow "testWorkflow", this
   * method will form the name "testWorkflow_job1" as the file name.
   *
   * @param name The job name
   * @param parentScope The fully-qualified name of the scope in which the job is bound
   * @return The name to use when generating the job file
   */
  String buildFileName(String name, String parentScope) {
    return parentScope == null ? name : "${parentScope}_${name}";
  }

  /**
   * Helper overload of the buildProperties method that specifically handles adding the job
   * dependencies to the job properties map. Subclasses can override this method if they want to
   * customize how the dependencies property is generated.
   *
   * @param allProperties The job properties map that holds all the job properties that will go into the built job file
   * @param parentScope The fully-qualified name of the scope in which the job is bound
   * @return The input job properties map
   */
  Map<String, String> buildProperties(Map<String, String> allProperties, String parentScope) {
    if (dependencyNames.size() > 0) {
      allProperties["dependencies"] = dependencyNames.collect() { String jobName -> return (parentScope == null) ? jobName : "${parentScope}_${jobName}"; }.join(",");
    }
    return buildProperties(allProperties);
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and jobProperties correctly.
   *
   * @param allProperties The job properties map that holds all the job properties that will go into the built job file
   * @return The input job properties map, with jobProperties and jvmProperties added
   */
  Map<String, String> buildProperties(Map<String, String> allProperties) {
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
}


/**
 * Job class for type=command jobs.
 * <p>
 * In the DSL, a CommandJob can be specified with:
 * <pre>
 *   def commands = ['echo "hello"', 'echo "This is how one runs a command job"', 'whoami']
 *
 *   commandJob('jobName') {
 *     uses 'echo "hello world"'  // Exactly one of uses or usesCommands is required
 *     usesCommands commands      // Exactly one of uses or usesCommands is required
 *   }
 * </pre>
 */
class CommandJob extends Job {
  String command;
  List<String> commands;

  /**
   * Constructor for a CommandJob.
   *
   * @param jobName The job name
   */
  CommandJob(String jobName) {
    super(jobName);
    setJobProperty("type", "command");
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and jobProperties correctly.
   *
   * @param allProperties The job properties map that holds all the job properties that will go into the built job file
   * @return The input job properties map, with jobProperties and jvmProperties added
   */
  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    super.buildProperties(allProperties);

    if (commands != null && commands.size() > 0) {
      allProperties["command"] = commands.get(0);

      for (int i = 1; i < commands.size(); i++) {
        allProperties["command.${i}"] = commands.get(i);
      }
    }

    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  CommandJob clone() {
    return clone(new CommandJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  @Override
  CommandJob clone(CommandJob cloneJob) {
    cloneJob.command = command;
    cloneJob.commands = commands;
    return super.clone(cloneJob);
  }

  /**
   * DSL method uses specifies the command for the job. This method causes the property
   * command=value to be added the job.
   * <p>
   * Only one of the methods uses or usesCommands can be specified with a CommandJob.
   *
   * @param command The command for the job
   */
  void uses(String command) {
    this.command = command;
    setJobProperty("command", command);
  }

  /**
   * DSL method usesCommands specifies the commands for the job. This method causes the properties
   * command.1=value1, command.2=value2, etc. to be added the job.
   * <p>
   * Only one of the methods uses or usesCommands can be specified with a CommandJob.
   *
   * @param command The command for the job
   */
  void usesCommands(List<String> commands) {
    this.commands = commands;
  }
}


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
 *     set confProperties: [
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
  Map<String, String> confProperties;
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
    this.confProperties = new LinkedHashMap<String, String>();
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and jobProperties correctly.
   *
   * @param allProperties The job properties map that holds all the job properties that will go into the built job file
   * @return The input job properties map, with jobProperties and jvmProperties added
   */
  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    if (cacheArchives.size() > 0) {
      String mrCacheArchives = cacheArchives.collect() { symLink, pathName -> return "${pathName}#${symLink}"; }.join(",")
      setConfProperty("mapred.cache.archives", mrCacheArchives);
      setConfProperty("mapred.create.symlink", "yes");
    }

    if (cacheFiles.size() > 0) {
      String mrCacheFiles = cacheFiles.collect() { symLink, pathName -> return "${pathName}#${symLink}"; }.join(",")
      setConfProperty("mapred.cache.files", mrCacheFiles);
      setConfProperty("mapred.create.symlink", "yes");
    }

    return super.buildProperties(allProperties);
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

      boolean found = archiveExt.any() { String ext -> return lowerPath.endsWith(ext); };
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
  @Override
  HadoopJavaProcessJob clone(HadoopJavaProcessJob cloneJob) {
    cloneJob.cacheArchives.putAll(cacheArchives);
    cloneJob.cacheFiles.putAll(cacheFiles);
    cloneJob.confProperties.putAll(confProperties);
    cloneJob.queueName = queueName;
    return super.clone(cloneJob);
  }

  /**
   * DSL queue method to declare the queue in which this job should run. This does the following:
   * <ul>
   *   <li>Sets the property mapred.job.queue.name in the job file</li>
   *   <li>Sets the property hadoop-conf.mapred.job.queue.name in the job file</li>
   *   <li>Adds "-D mapred.job.queue.name=val" to the property main.args</li>
   * </ul>
   *
   * @param queueName The name of the queue in which this job should run
   */
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
   * hadoop-conf.key=val to be written to the job file.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set;
   *   or a key 'jvmProperties' with a map value that specifies the JVM properties to set;
   *   or a key 'confProperties' with a map value that specifies the Hadoop job configuration properties to set
   */
  @Override
  void set(Map args) {
    super.set(args);

    if (args.containsKey("confProperties")) {
      Map<String, String> confProperties = args.confProperties;
      confProperties.each() { String name, String value ->
        setConfProperty(name, value);
      }
    }
  }

  /**
   * Sets the given Hadoop job configuration property. For a given key and value, this method
   * causes the line hadoop-conf.key=val to be added to the job file.
   *
   * @param name The Hadoop job configuration property to set
   * @param value The Hadoop job configuration property value
   */
  void setConfProperty(String name, String value) {
    confProperties.put(name, value);
    setJobProperty("hadoop-conf.${name}", value);
  }
}


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
 *     queue 'marathon
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
  Map<String, String> parameters;
  String script;

  /**
   * Constructor for a HiveJob.
   *
   * @param jobName The job name
   */
  HiveJob(String jobName) {
    super(jobName);
    this.parameters = new LinkedHashMap<String, String>();
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
  @Override
  HiveJob clone(HiveJob cloneJob) {
    cloneJob.parameters.putAll(parameters);
    cloneJob.script = script;
    return super.clone(cloneJob);
  }

  /**
   * DSL method to specify HDFS paths read by the job. In addition to the functionality of the base
   * class, for Hive jobs, using this method will cause a Hive parameter to be added to the job
   * properties for each HDFS path specified.
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job reads
   */
  @Override
  void reads(Map args) {
    super.reads(args);

    // For Hive jobs, additionally emit a Hive script parameter
    Map<String, String> files = args.files;
    files.each() { String name, String value ->
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
  @Override
  void writes(Map args) {
    super.writes(args);

    // For Hive jobs, additionally emit a Hive script parameter
    Map<String, String> files = args.files;
    files.each() { String name, String value ->
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
   * hadoop-conf.key=val to be written to the job file.
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
  @Override
  void set(Map args) {
    super.set(args);

    if (args.containsKey("parameters")) {
      Map<String, String> parameters = args.parameters;
      parameters.each() { String name, String value ->
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
  void setParameter(String name, String value) {
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
  @Override
  void uses(String script) {
    this.script = script;
    setJobProperty("hive.script", script);
  }
}


/**
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
 *     queue 'marathon
 *   }
 * </pre>
 */
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
  @Override
  JavaJob clone(JavaJob cloneJob) {
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
  Map<String, String> jvmProperties;
  Integer xms;
  Integer xmx;

  /**
   * Constructor for a JavaProcessJob.
   *
   * @param jobName The job name
   */
  JavaProcessJob(String jobName) {
    super(jobName);
    jvmProperties = new LinkedHashMap<String, String>();
    setJobProperty("type", "javaprocess");
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and jobProperties correctly.
   *
   * @param allProperties The job properties map that holds all the job properties that will go into the built job file
   * @return The input job properties map, with jobProperties and jvmProperties added
   */
  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    super.buildProperties(allProperties);

    if (jvmProperties.size() > 0) {
      String jvmArgs = jvmProperties.collect() { key, val -> return "-D${key}=${val}" }.join(" ");
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
  @Override
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
      Map<String, String> jvmProperties = args.jvmProperties;
      jvmProperties.each() { name, value ->
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
  void setJvmProperty(String name, String value) {
    jvmProperties.put(name, value);
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


/**
 * Job class for type=KafkaPushJob jobs.
 * <p>
 * These are documented internally at LinkedIn at https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Hadoop+to+Kafka+Bridge.
 * <p>
 * The code for this job is at http://svn.corp.linkedin.com/netrepo/hadoop-to-kafka-bridge/trunk/.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. Please check that these values are appropriate for your application. In the
 * DSL, a KafkaPushJob can be specified with:
 * <pre>
 *   kafkaPushJob('jobName') {
 *     usesInputPath '/data/databases/MEMBER2/MEMBER_PROFILE/#LATEST'  // Required
 *     usesTopic 'kafkatestpush'                                       // Required
 *     usesBatchNumBytes 1000000                                       // Optional
 *     usesDisableSchemaRegistration true                              // Optional
 *     usesKafkaUrl 'eat1-ei2-kafka-vip-c.stg.linkedin.com:10251'      // Optional
 *     usesNameNode 'hdfs://eat1-magicnn01.grid.linkedin.com:9000'     // Optional
 *     usesSchemaRegistryUrl 'http://eat1-app501.stg.linkedin.com:10252/schemaRegistry/schemas'  // Optional
 *   }
 * </pre>
 */
class KafkaPushJob extends HadoopJavaJob {
  // Required
  String inputPath;
  String topic;

  // Optional
  Integer batchNumBytes;
  Boolean disableSchemaRegistration;
  String kafkaUrl;
  String nameNode;
  String schemaRegistryUrl;

  /**
   * Constructor for a KafkaPushJob.
   *
   * @param jobName The job name
   */
  KafkaPushJob(String jobName) {
    super(jobName);
    setJobProperty("type", "KafkaPushJob");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  KafkaPushJob clone() {
    return clone(new KafkaPushJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  @Override
  KafkaPushJob clone(KafkaPushJob cloneJob) {
    cloneJob.inputPath = inputPath;
    cloneJob.topic = topic;
    cloneJob.batchNumBytes = batchNumBytes;
    cloneJob.kafkaUrl = kafkaUrl;
    cloneJob.nameNode = nameNode;
    cloneJob.schemaRegistryUrl = schemaRegistryUrl;
    return super.clone(cloneJob);
  }

  /**
   * DSL usesInputPath method causes input.path=value to be set in the job file.
   *
   * @param inputPath
   */
  void usesInputPath(String inputPath) {
    this.inputPath = inputPath;
    setJobProperty("input.path", inputPath);
  }

  /**
   * DSL usesTopic method causes topic=value to be set in the job file.
   *
   * @param topic
   */
  void usesTopic(String topic) {
    this.topic = topic;
    setJobProperty("topic", topic);
  }

  /**
   * DSL usesBatchNumBytes method causes batch.num.bytes=value to be set in the job file.
   *
   * @param batchNumBytes
   */
  void usesBatchNumBytes(Integer batchNumBytes) {
    this.batchNumBytes = batchNumBytes;
    setJobProperty("batch.num.bytes", batchNumBytes.toString());
  }

  /**
   * DSL usesDisableSchemaRegistration method causes disable.schema.registration=value to be set in
   * the job file.
   *
   * @param disableSchemaRegistration
   */
  void usesDisableSchemaRegistration(Boolean disableSchemaRegistration) {
    this.disableSchemaRegistration = disableSchemaRegistration;
    setJobProperty("disable.schema.registration", disableSchemaRegistration.toString());
  }

  /**
   * DSL usesKafkaUrl method causes kafka.url=value to be set in the job file.
   *
   * @param kafkaUrl
   */
  void usesKafkaUrl(String kafkaUrl) {
    this.kafkaUrl = kafkaUrl;
    setJobProperty("kafka.url", kafkaUrl);
  }

  /**
   * DSL usesNameNode method causes name.node=value to be set in the job file.
   *
   * @param nameNode
   */
  void usesNameNode(String nameNode) {
    this.nameNode = nameNode;
    setJobProperty("name.node", nameNode);
  }

  /**
   * DSL usesSchemaRegistryUrl method causes schemaregistry.rest.url=value to be set in the job
   * file.
   *
   * @param schemaRegistryUrl
   */
  void usesSchemaRegistryUrl(String schemaRegistryUrl) {
    this.schemaRegistryUrl = schemaRegistryUrl;
    setJobProperty("schemaregistry.rest.url", schemaRegistryUrl);
  }
}


/**
 * A LaunchJob is a special kind of NoOpJob that is used to launch a workflow. The name of the
 * generated LaunchJob file is simply the workflow name.
 * <p>
 * Launch jobs are not specified in the DSL. When you build a workflow, a LaunchJob (with
 * dependencies set to be the names of the jobs the workflow executes) is created for you.
 */
class LaunchJob extends NoOpJob {
  /**
   * Constructor for a LaunchJob.
   *
   * @param jobName The job name
   */
  LaunchJob(String jobName) {
    super(jobName);
  }

  /**
   * Override for LaunchJob. The name of the generated LaunchJob file is simply the workflow name.
   *
   * @param name The job name
   * @param parentScope The fully-qualified name of the scope in which the job is bound
   * @return The name to use when generating the job file
   */
  @Override
  String buildFileName(String name, String parentScope) {
    return name;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  LaunchJob clone() {
    return clone(new LaunchJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  @Override
  LaunchJob clone(LaunchJob cloneJob) {
    return super.clone(cloneJob);
  }
}


/**
 * Job class for type=noop jobs.
 * <p>
 * In the DSL, a NoOpJob can be specified with:
 * <pre>
 *   noOpJob('jobName') {
 *     depends 'job1', 'job2'  // Typically in a NoOpJob the only thing you will ever declare are job dependencies
 *   }
 * </pre>
 */
class NoOpJob extends Job {
  /**
   * Constructor for a NoOpJob.
   *
   * @param jobName The job name
   */
  NoOpJob(String jobName) {
    super(jobName);
    setJobProperty("type", "noop");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  NoOpJob clone() {
    return clone(new NoOpJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  @Override
  NoOpJob clone(NoOpJob cloneJob) {
    return super.clone(cloneJob);
  }
}


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
  Map<String, String> parameters;
  String script;

  /**
   * Constructor for a PigJob.
   *
   * @param jobName The job name
   */
  PigJob(String jobName) {
    super(jobName);
    parameters = new LinkedHashMap<String, String>();
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
  @Override
  PigJob clone(PigJob cloneJob) {
    cloneJob.parameters.putAll(parameters);
    cloneJob.script = script;
    return super.clone(cloneJob);
  }

  /**
   * DSL method to specify HDFS paths read by the job. In addition to the functionality of the base
   * class, for Pig jobs, using this method will cause a Pig parameter to be added to the job
   * properties for each HDFS path specified.
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job reads
   */
  @Override
  void reads(Map args) {
    super.reads(args);

    // For Pig jobs, additionally emit a Pig script parameter
    Map<String, String> files = args.files;
    files.each() { String name, String value ->
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
  @Override
  void writes(Map args) {
    super.writes(args);

    // For Pig jobs, additionally emit a Pig script parameter
    Map<String, String> files = args.files;
    files.each() { String name, String value ->
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
   * hadoop-conf.key=val to be written to the job file.
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
  @Override
  void set(Map args) {
    super.set(args);

    if (args.containsKey("parameters")) {
      Map<String, String> parameters = args.parameters;
      parameters.each() { String name, String value ->
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
  void setParameter(String name, String value) {
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
  @Override
  void uses(String script) {
    this.script = script;
    setJobProperty("pig.script", script);
  }
}


/**
 * Job class for type=VoldemortBuildandPush jobs.
 * <p>
 * These are documented internally at LinkedIn at https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Voldemort+Build+and+Push.
 * <p>
 * The code for this job is at https://github.com/voldemort/voldemort/blob/master/contrib/hadoop-store-builder/src/java/voldemort/store/readonly/mr/azkaban/VoldemortBuildAndPushJob.java.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. Please check that these values are appropriate for your application. In the
 * DSL, a VoldemortBuildandPush can be specified with:
 * <pre>
 *   voldemortBuildPushJob('jobName') {
 *     usesStoreName 'test-store'          // Required
 *     usesClusterName 'tcp://foo:10103'   // Required
 *     usesInputPath '/user/foo/input'     // Required
 *     usesOutputPath '/user/foo/output'   // Required
 *     usesStoreOwners 'foo@linkedin.com'  // Required
 *     usesStoreDesc 'Store for testing'   // Required
 *     usesTempDir '/tmp/foo'              // Optional
 *     usesRepFactor 2                     // Optional
 *     usesCompressValue false             // Optional
 *     usesKeySelection 'memberId'         // Optional
 *     usesValueSelection 'lastName'       // Optional
 *     usesNumChunks -1                    // Optional
 *     usesChunkSize 1073741824            // Optional
 *     usesKeepOutput false                // Optional
 *     usesPushHttpTimeoutSeconds 86400    // Optional
 *     usesPushNode 0                      // Optional
 *     usesBuildStore true                 // Optional
 *     usesPushStore true                  // Optional
 *     usesFetcherProtocol 'hftp'          // Optional
 *     usesFetcherPort '50070'             // Optional
 *     usesAvroSerializerVersioned false   // Optional
 *     usesAvroData false                  // Optional
 *     usesAvroKeyField 'memberId'         // Optional unless isAvroData is true
 *     usesAvroValueField 'firstName'      // Optional unless isAvroData is true
 *   }
 * </pre>
 */
class VoldemortBuildPushJob extends HadoopJavaJob {
  // Required
  String storeName;
  String clusterName;
  String buildInputPath;
  String buildOutputPath;
  String storeOwners;
  String storeDesc;

  // Optional
  String buildTempDir;
  Integer repFactor;
  Boolean compressValue;
  String keySelection;
  String valueSelection;
  Integer numChunks;
  Integer chunkSize;
  Boolean keepOutput;
  Integer pushHttpTimeoutSeconds;
  Integer pushNode;
  Boolean buildStore;
  Boolean pushStore;
  String fetcherProtocol;
  String fetcherPort;
  Boolean isAvroSerializerVersioned;
  Boolean isAvroData;
  String avroKeyField;    // Required if isAvroData is true
  String avroValueField;  // Required if isAvroData is true

  /**
   * Constructor for a VoldemortBuildPushJob.
   *
   * @param jobName The job name
   */
  VoldemortBuildPushJob(String jobName) {
    super(jobName);
    setJobProperty("type", "VoldemortBuildandPush");
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and jobProperties correctly.
   *
   * @param allProperties The job properties map that holds all the job properties that will go into the built job file
   * @return The input job properties map, with jobProperties and jvmProperties added
   */
  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    super.buildProperties(allProperties);
    if (isAvroData != null && isAvroData == true) {
      allProperties["avro.key.field"] = avroKeyField;
      allProperties["avro.value.field"] = avroValueField;
    }
    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  VoldemortBuildPushJob clone() {
    return clone(new VoldemortBuildPushJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  @Override
  VoldemortBuildPushJob clone(VoldemortBuildPushJob cloneJob) {
    cloneJob.storeName = storeName;
    cloneJob.clusterName = clusterName;
    cloneJob.buildInputPath = buildInputPath;
    cloneJob.buildOutputPath = buildOutputPath;
    cloneJob.storeOwners = storeOwners;
    cloneJob.storeDesc = storeDesc;
    cloneJob.buildTempDir = buildTempDir;
    cloneJob.repFactor = repFactor;
    cloneJob.compressValue = compressValue;
    cloneJob.keySelection = keySelection;
    cloneJob.valueSelection = valueSelection;
    cloneJob.numChunks = numChunks;
    cloneJob.chunkSize = chunkSize;
    cloneJob.keepOutput = keepOutput;
    cloneJob.pushHttpTimeoutSeconds = pushHttpTimeoutSeconds;
    cloneJob.pushNode = pushNode;
    cloneJob.buildStore = buildStore;
    cloneJob.pushStore = pushStore;
    cloneJob.fetcherProtocol = fetcherProtocol;
    cloneJob.fetcherPort = fetcherPort;
    cloneJob.isAvroSerializerVersioned = isAvroSerializerVersioned;
    cloneJob.isAvroData = isAvroData;
    cloneJob.avroKeyField = avroKeyField;
    cloneJob.avroValueField = avroValueField;
    return super.clone(cloneJob);
  }

  /**
   * DSL usesStoreName method causes push.store.name=value to be set in the job file.
   *
   * @param storeName
   */
  void usesStoreName(String storeName) {
    this.storeName = storeName;
    setJobProperty("push.store.name", storeName);
  }

  /**
   * DSL usesClusterName method causes push.cluster=value to be set in the job file.
   *
   * @param clusterName
   */
  void usesClusterName(String clusterName) {
    this.clusterName = clusterName;
    setJobProperty("push.cluster", clusterName);
  }

  /**
   * DSL usesInputPath method causes build.input.path=value to be set in the job file.
   *
   * @param buildInputPath
   */
  void usesInputPath(String buildInputPath) {
    this.buildInputPath = buildInputPath;
    setJobProperty("build.input.path", buildInputPath);
  }

  /**
   * DSL usesOutputPath method causes build.output.dir=value to be set in the job file.
   *
   * @param buildOutputPath
   */
  void usesOutputPath(String buildOutputPath) {
    this.buildOutputPath = buildOutputPath;
    setJobProperty("build.output.dir", buildOutputPath);
  }

  /**
   * DSL usesStoreOwners method causes push.store.owners=value to be set in the job file.
   *
   * @param storeOwners
   */
  void usesStoreOwners(String storeOwners) {
    this.storeOwners = storeOwners;
    setJobProperty("push.store.owners", storeOwners);
  }

  /**
   * DSL usesStoreDesc method causes push.store.description=value to be set in the job file.
   *
   * @param storeDesc
   */
  void usesStoreDesc(String storeDesc) {
    this.storeDesc = storeDesc;
    setJobProperty("push.store.description", storeDesc);
  }

  /**
   * DSL usesTempDir method causes build.temp.dir=value to be set in the job file.
   *
   * @param buildTempDir
   */
  void usesTempDir(String buildTempDir) {
    this.buildTempDir = buildTempDir;
    setJobProperty("build.temp.dir", buildTempDir);
  }

  /**
   * DSL usesRepFactor method causes build.replication.factor=value to be set in the job file.
   *
   * @param repFactor
   */
  void usesRepFactor(Integer repFactor) {
    this.repFactor = repFactor;
    setJobProperty("build.replication.factor", repFactor);
  }

  /**
   * DSL usesCompressValue method causes build.compress.value=value to be set in the job file.
   *
   * @param compressValue
   */
  void usesCompressValue(Boolean compressValue) {
    this.compressValue = compressValue;
    setJobProperty("build.compress.value", compressValue);
  }

  /**
   * DSL usesKeySelection method causes key.selection=value to be set in the job file.
   *
   * @param keySelection
   */
  void usesKeySelection(String keySelection) {
    this.keySelection = keySelection;
    setJobProperty("key.selection", keySelection);
  }

  /**
   * DSL usesValueSelection method causes value.selection=value to be set in the job file.
   *
   * @param valueSelection
   */
  void usesValueSelection(String valueSelection) {
    this.valueSelection = valueSelection;
    setJobProperty("value.selection", valueSelection);
  }

  /**
   * DSL usesNumChunks method causes num.chunks=value to be set in the job file.
   *
   * @param numChunks
   */
  void usesNumChunks(Integer numChunks) {
    this.numChunks = numChunks;
    setJobProperty("num.chunks", numChunks);
  }

  /**
   * DSL usesChunkSize method causes build.chunk.size=value to be set in the job file.
   *
   * @param chunkSize
   */
  void usesChunkSize(Integer chunkSize) {
    this.chunkSize = chunkSize;
    setJobProperty("build.chunk.size", chunkSize);
  }

  /**
   * DSL usesKeepOutput method causes build.output.keep=value to be set in the job file.
   *
   * @param keepOutput
   */
  void usesKeepOutput(Boolean keepOutput) {
    this.keepOutput = keepOutput;
    setJobProperty("build.output.keep", keepOutput);
  }

  /**
   * DSL usesPushHttpTimeoutSeconds method causes push.http.timeout.seconds=value to be set in the
   * job file.
   *
   * @param pushHttpTimeoutSeconds
   */
  void usesPushHttpTimeoutSeconds(Integer pushHttpTimeoutSeconds) {
    this.pushHttpTimeoutSeconds = pushHttpTimeoutSeconds;
    setJobProperty("push.http.timeout.seconds", pushHttpTimeoutSeconds);
  }

  /**
   * DSL usesPushNode method causes push.node=value to be set in the job file.
   *
   * @param pushNode
   */
  void usesPushNode(Integer pushNode) {
    this.pushNode = pushNode;
    setJobProperty("push.node", pushNode);
  }

  /**
   * DSL usesBuildStore method causes build=value to be set in the job file.
   *
   * @param buildStore
   */
  void usesBuildStore(Boolean buildStore) {
    this.buildStore = buildStore;
    setJobProperty("build", buildStore);
  }

  /**
   * DSL usesPushStore method causes push=value to be set in the job file.
   *
   * @param pushStore
   */
  void usesPushStore(Boolean pushStore) {
    this.pushStore = pushStore;
    setJobProperty("push", pushStore);
  }

  /**
   * DSL usesFetcherProtocol method causes voldemort.fetcher.protocol=value to be set in the job
   * file.
   *
   * @param fetcherProtocol
   */
  void usesFetcherProtocol(String fetcherProtocol) {
    this.fetcherProtocol = fetcherProtocol;
    setJobProperty("voldemort.fetcher.protocol", fetcherProtocol);
  }

  /**
   * DSL usesFetcherPort method causes voldemort.fetcher.port=value to be set in the job file.
   *
   * @param fetcherPort
   */
  void usesFetcherPort(String fetcherPort) {
    this.fetcherPort = fetcherPort;
    setJobProperty("voldemort.fetcher.port", fetcherPort);
  }

  /**
   * DSL usesAvroSerializerVersioned method causes avro.serializer.versioned=value to be set in the
   * job file.
   *
   * @param isAvroSerializerVersioned
   */
  void usesAvroSerializerVersioned(Boolean isAvroSerializerVersioned) {
    this.isAvroSerializerVersioned = isAvroSerializerVersioned;
    setJobProperty("avro.serializer.versioned", isAvroSerializerVersioned);
  }

  /**
   * DSL usesAvroData method causes build.type.avro=value to be set in the job file.
   *
   * @param isAvroData
   */
  void usesAvroData(Boolean isAvroData) {
    this.isAvroData = isAvroData;
    setJobProperty("build.type.avro", isAvroData);
  }

  /**
   * DSL usesAvroKeyField method causes avro.key.field=value to be set in the job file. NOTE: this
   * property is only added to the job file if isAvroData is set to true.
   *
   * @param avroKeyField
   */
  void usesAvroKeyField(String avroKeyField) {
    this.avroKeyField = avroKeyField;
    // Only set the job property at build time if isAvroData is true
  }

  /**
   * DSL usesAvroValueField method causes avro.value.field=value to be set in the job file. NOTE:
   * this property is only added to the job file if isAvroData is set to true.
   *
   * @param avroValueField
   */
  void usesAvroValueField(String avroValueField) {
    this.avroValueField = avroValueField;
    // Only set the job property at build time if isAvroData is true
  }
}