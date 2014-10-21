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
 *   job('jobName) {
 *     reads files: [
 *       'foo' : '/data/databases/foo',
 *       'bar' : '/data/databases/bar',
 *     ]
 *     writes files: [
 *       'bazz' : '/user/bazz/foobar'
 *     ]
 *     caches files: [
 *       'foo.jar' : '/user/bazz/foo.jar'
 *     ]
 *     cachesArchive files: [
 *       'foobar' : '/user/bazz/foobar.zip'
 *     ]
 *     set properties: [
 *       'propertyName1' : 'propertyValue1'
 *     ]
 *     set jvmProperties: [
 *       'jvmPropertyName1' : 'jvmPropertyValue1',
 *       'jvmPropertyName2' : 'jvmPropertyValue2'
 *     ]
 *     depends 'job1'
 *     queue 'marathon
 *   }
 * </pre>
 */
class Job {
  String name;
  Set<String> dependencyNames;
  Map<String, String> jobProperties;
  Map<String, String> jvmProperties;
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
    jvmProperties = new LinkedHashMap<String, String>();
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
    if (jvmProperties.size() > 0) {
      String jvmArgs = jvmProperties.collect() { key, val -> return "-D${key}=${val}" }.join(" ");
      allProperties["jvm.args"] = jvmArgs;
    }

    jobProperties.each() { key, value ->
      allProperties[key] = value;
    }

    return allProperties;
  }

  /**
   * DSL method to specify files to cache. Using this DSL method sets the JVM properties
   * mapred.cache.files and mapred.create.symlink=yes. These properties cause the specified files
   * to be added to DistributedCache and propagated to each of the tasks. A symlink will be created
   * in the task working directory that points to the file.
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
      String cacheLink = "${pathName}#${symLink}";

      if (jvmProperties.containsKey("mapred.cache.files")) {
        setJvmProperty("mapred.cache.files", jvmProperties["mapred.cache.files"] + "," + cacheLink);
      }
      else {
        setJvmProperty("mapred.cache.files", cacheLink);
      }
    }

    setJvmProperty("mapred.create.symlink", "yes");
  }

  /**
   * DSL method to specify archives to cache. Valid archives to use with this method are .zip,
   * .tgz, .tar.gz, .tar and .jar. Any other file type will result in an exception. Using this DSL
   * method sets the JVM properties mapred.cache.archives and mapred.create.symlink=yes. These
   * properties cause the specified archives to be added to DistributedCache and propagated to each
   * of the tasks. A symlink will be created in the task working directory that points to the
   * exploded archive directory.
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
      String cacheLink = "${pathName}#${symLink}";

      boolean found = false;
      String lowerPath = pathName.toLowerCase();

      archiveExt.each() { String ext ->
        found = found || lowerPath.endsWith(ext);
      }

      if (!found) {
        throw new Exception("File given to cachesArchive must be one of: " + archiveExt.toString());
      }

      if (jvmProperties.containsKey("mapred.cache.archives")) {
        setJvmProperty("mapred.cache.archives", jvmProperties["mapred.cache.archives"] + "," + cacheLink);
      }
      else {
        setJvmProperty("mapred.cache.archives", cacheLink);
      }
    }

    setJvmProperty("mapred.create.symlink", "yes");
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
    cloneJob.jvmProperties.putAll(jvmProperties);
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
   * DSL queue method declares the queue in which this job should run. This sets the JVM property
   * mapred.job.queue.name.
   *
   * @param queueName The name of the queue in which this job should run
   */
  void queue(String queueName) {
    jvmProperties.put("mapred.job.queue.name", queueName);
  }

  /**
   * DSL method to specify HDFS paths read by the job. When you use this method, the static checker
   * will verify that this job is dependent or transitively dependent on any jobs that write paths
   * that this job reads. This is an important race condition in workflows that can be completely
   * eliminated with this static check.
   * <p>
   * Using this method additionally causes lines of the form form key=hdfsPath to be written to
   * the job file (i.e. the keys you use are available as job parameters).
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job reads
   */
  void reads(Map args) {
    Map<String, String> files = args.files;
    for (Map.Entry<String, String> entry : files) {
      setJobProperty(entry.key, entry.value);
      reading.add(entry.value);
    }
  }

  /**
   * DSL method to specify the HDFS paths written by the job. When you use this method, the static
   * checker will verify that any jobs that read paths this job writes are dependent or transitively
   * dependent on this job. This is an important race condition in workflows that can be completely
   * eliminated with this static check.
   * <p>
   * Using this method additionally causes lines of the form form key=hdfsPath to be written to
   * the job file (i.e. the keys you use are available as job parameters).
   *
   * @param args Args whose key 'files' has a map value specifying the HDFS paths this job writes
   */
  void writes(Map args) {
    Map<String, String> files = args.files;
    for (Map.Entry<String, String> entry : files) {
      setJobProperty(entry.key, entry.value);
      writing.add(entry.value);
    }
  }

  /**
   * DSL method to specify job and JVM properties to set on the job. Specifying job properties
   * causes lines of the form key=val to be written to the job file, while specifying JVM
   * properties causes a line of the form jvm.args=-Dkey1=val1 ... -DkeyN=valN to be written to the
   * job file.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set, or a key 'jvmProperties' with a map value that specifies the JVM properties to set
   */
  void set(Map args) {
    if (args.containsKey("properties")) {
      Map<String, String> properties = args.properties;
      this.jobProperties.putAll(properties);
    }
    if (args.containsKey("jvmProperties")) {
      Map<String, String> jvmProperties = args.jvmProperties;
      this.jvmProperties.putAll(jvmProperties);
    }
  }

  /**
   * Sets the given job property.
   *
   * @param name The job property to set
   * @param value The job property value
   */
  void setJobProperty(String name, String value) {
    jobProperties.put(name, value)
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
   * Returns a string representation of the job.
   *
   * @return A string representation of the job
   */
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
  }

  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "command";

    if (command != null) {
      allProperties["command"] = command;
    }
    if (commands != null && commands.size() > 0) {
      allProperties["command"] = commands.get(0);

      for (int i = 1; i < commands.size(); i++) {
        allProperties["command.${i}"] = commands.get(i);
      }
    }

    return super.buildProperties(allProperties);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  CommandJob clone() {
    return clone(new CommandJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
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
 *   }
 * </pre>
 */
class HadoopJavaJob extends Job {
  String jobClass;

  /**
   * Constructor for a HadoopJavaJob.
   *
   * @param jobName The job name
   */
  HadoopJavaJob(String jobName) {
    super(jobName);
  }

  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "hadoopJava";
    allProperties["job.class"] = jobClass;
    return super.buildProperties(allProperties);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  HadoopJavaJob clone() {
    return clone(new HadoopJavaJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
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
  void uses(String jobClass) {
    this.jobClass = jobClass;
  }
}

/**
 * Job class for type=hive jobs.
 * <p>
 * In the DSL, a HiveJob can be specified with:
 * <pre>
 *   def queries = ["show tables", "describe myTable"]
 *
 *   hiveJob('jobName') {
 *     usesQueries queries
 *     usesQuery "show tables"  // Exactly one of usesQueries, usesQuery or usesQueryFile is required
 *     usesQueryFile "hello.q"  // Exactly one of usesQueries, usesQuery or usesQuery is required
 *   }
 * </pre>
 */
class HiveJob extends Job {
  // Exactly one of queries, query or queryFile must be set
  List<String> queries;
  String query;
  String queryFile;

  /**
   * Constructor for a HiveJob.
   *
   * @param jobName The job name
   */
  HiveJob(String jobName) {
    super(jobName);
  }

  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "hive";
    allProperties["azk.hive.action"] = "execute.query";

    if (queries != null && queries.size() > 0) {
      for (int i = 1; i <= queries.size(); i++) {
        if (i <= 9) {
          // Multi-queries have to be indexed from 01. See http://azkaban.github.io/azkaban/docs/2.5/#hive-type.
          allProperties["hive.query.0${i}"] = queries.get(i - 1);
        }
        else {
          allProperties["hive.query.${i}"] = queryFile;
        }
      }
    }
    if (query != null) {
      allProperties["hive.query"] = query;
    }
    if (queryFile != null) {
      allProperties["hive.query.file"] = queryFile;
    }

    return super.buildProperties(allProperties);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
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
    cloneJob.queries = queries;
    cloneJob.query = query;
    cloneJob.queryFile = queryFile;
    return super.clone(cloneJob);
  }

  /**
   * DSL method usesQueries specifies the queries for the job. This method causes the properties
   * hive.query.01=value1, hive.query.02=value2, etc. to be added the job.
   * <p>
   * Note that you can specify a maximum of 99 queries for the job. If there are more than 99
   * queries, the job will fail the static checker.
   * <p>
   * Only one of the methods usesQueries, usesQuery or usesQueryFile can be specified with a
   * HiveJob.
   *
   * @param queries The Hive queries for the job
   */
  void usesQueries(List<String> queries) {
    this.queries = queries;
  }

  /**
   * DSL method usesQuery specifies the query for the job. This method causes the property
   * hive.query=value to be added the job.
   * <p>
   * Only one of the methods usesQueries, usesQuery or usesQueryFile can be specified with a
   * HiveJob.
   *
   * @param query The Hive query for the job
   */
  void usesQuery(String query) {
    this.query = query;
  }

  /**
   * DSL method usesQueryFile specifies the query file for the job. This method causes the property
   * hive.query.file=value to be added the job.
   * <p>
   * Only one of the methods usesQueries, usesQuery or usesQueryFile can be specified with a
   * HiveJob.
   *
   * @param queryFile The Hive query file for the job
   */
  void usesQueryFile(String queryFile) {
    this.queryFile = queryFile;
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
 *   }
 * </pre>
 */
class JavaJob extends Job {
  String jobClass;

  /**
   * Constructor for a JavaJob.
   *
   * @param jobName The job name
   */
  JavaJob(String jobName) {
    super(jobName);
  }

  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "java";
    allProperties["job.class"] = jobClass;
    return super.buildProperties(allProperties);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
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
    return super.clone(cloneJob);
  }

  /**
   * DSL method uses specifies the Java class for the job. This method causes the property
   * job.class=value to be added the job. This method is required to build the job.
   *
   * @param jobClass The Java class for the job
   */
  void uses(String jobClass) {
    this.jobClass = jobClass;
  }
}


/**
 * Job class for type=javaprocess jobs.
 * <p>
 * According to the Azkaban documentation at http://azkaban.github.io/azkaban/docs/2.5/#job-types,
 * this is the job type you should use for Java-only jobs that do not need to acquire a secure
 * token to your Hadoop cluster.
 * <p>
 * In the DSL, a JavaProcessJob can be specified with:
 * <pre>
 *   javaProcessJob('jobName') {
 *     uses 'com.linkedin.foo.HelloJavaProcessJob'  // Required
 *   }
 * </pre>
 */
class JavaProcessJob extends Job {
  String javaClass;

  /**
   * Constructor for a JavaProcessJob.
   *
   * @param jobName The job name
   */
  JavaProcessJob(String jobName) {
    super(jobName);
  }

  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "javaprocess";
    allProperties["java.class"] = javaClass;
    return super.buildProperties(allProperties);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
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
    return super.clone(cloneJob);
  }

  /**
   * DSL method uses specifies the Java class for the job. This method causes the property
   * java.class=value to be added the job. This method is required to build the job.
   *
   * @param javaClass The Java class for the job
   */
  void uses(String javaClass) {
    this.javaClass = javaClass;
  }
}


/**
 * Job class for type=KafkaPushJob jobs.
 * <p>
 * These are documentated internally at LinkedIn at https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Hadoop+to+Kafka+Bridge.
 * <p>
 * The code for this job is at http://svn.corp.linkedin.com/netrepo/hadoop-to-kafka-bridge/trunk/.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. In the DSL, a KafkaPushJob can be specified with:
 * <pre>
 *   kafkaPushJob('jobName') {
 *     usesInputPath '/data/databases/MEMBER2/MEMBER_PROFILE/#LATEST'  // Required
 *     usesTopic 'kafkatestpush'                                       // Required
 *     usesBatchNumBytes 1000000                                       // Optional
 *     usesDisableSchemaRegistration true                              // Optional
 *     usesKafkaUrl 'eat1-ei2-kafka-vip-c.stg.linkedin.com:10251'      // Optional
 *     usesNameNode 'hdfs://eat1-magicnn01.grid.linkedin.com:9000'     // Optional
 *     usesSchemaRegistryUrl 'http://eat1-app501:10252/schemaRegistry/schemas'  // Optional
 *   }
 * </pre>
 */
class KafkaPushJob extends Job {
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
  }

  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "KafkaPushJob";
    allProperties["input.path"] = inputPath;
    allProperties["topic"] = topic;
    if (batchNumBytes != null) {
      allProperties["batch.num.bytes"] = batchNumBytes.toString();
    }
    if (disableSchemaRegistration != null) {
      allProperties["disable.schema.registration"] = disableSchemaRegistration.toString();
    }
    if (kafkaUrl != null) {
      allProperties["kafka.url"] = kafkaUrl;
    }
    if (nameNode != null) {
      allProperties["name.node"] = nameNode;
    }
    if (schemaRegistryUrl != null) {
      allProperties["schemaregistry.rest.url"] = schemaRegistryUrl;
    }
    return super.buildProperties(allProperties);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  KafkaPushJob clone() {
    return clone(new KafkaPushJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
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
  }

  /**
   * DSL usesTopic method causes topic=value to be set in the job file.
   *
   * @param topic
   */
  void usesTopic(String topic) {
    this.topic = topic;
  }

  /**
   * DSL usesBatchNumBytes method causes batch.num.bytes=value to be set in the job file.
   *
   * @param batchNumBytes
   */
  void usesBatchNumBytes(Integer batchNumBytes) {
    this.batchNumBytes = batchNumBytes;
  }

  /**
   * DSL usesDisableSchemaRegistration method causes disable.schema.registration=value to be set in
   * the job file.
   *
   * @param disableSchemaRegistration
   */
  void usesDisableSchemaRegistration(Boolean disableSchemaRegistration) {
    this.disableSchemaRegistration = disableSchemaRegistration;
  }

  /**
   * DSL usesKafkaUrl method causes kafka.url=value to be set in the job file.
   *
   * @param kafkaUrl
   */
  void usesKafkaUrl(String kafkaUrl) {
    this.kafkaUrl = kafkaUrl;
  }

  /**
   * DSL usesNameNode method causes name.node=value to be set in the job file.
   *
   * @param nameNode
   */
  void usesNameNode(String nameNode) {
    this.nameNode = nameNode;
  }

  /**
   * DSL usesSchemaRegistryUrl method causes schemaregistry.rest.url=value to be set in the job
   * file.
   *
   * @param schemaRegistryUrl
   */
  void usesSchemaRegistryUrl(String schemaRegistryUrl) {
    this.schemaRegistryUrl = schemaRegistryUrl;
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
  LaunchJob clone() {
    return clone(new LaunchJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
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
  }

  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "noop";
    return super.buildProperties(allProperties);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  NoOpJob clone() {
    return clone(new NoOpJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
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
 *     set parameters: [
 *       'param1' : 'val1',
 *       'param2' : 'val2'
 *     ]
 *   }
 * </pre>
 */
class PigJob extends Job {
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
  }

  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "pig";
    allProperties["pig.script"] = script;
    parameters.each() { key, value ->
      allProperties["param.${key}"] = "${value}";
    }
    return super.buildProperties(allProperties);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
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
    return super.clone(cloneJob);
  }

  /**
   * DSL parameter method sets Pig parameters for the job. When the job file is built, job
   * properties of the form param.name=value are added to the job file. With your parameters
   * set this way, you can use $name in your Pig script and get the associated value.
   *
   * @param name The Pig parameter name
   * @param value The Pig parameter value
   */
  void parameter(String name, String value) {
    parameters.put(name, value);
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

    for (Map.Entry<String, String> entry : files) {
      parameter(entry.key, entry.value);
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

    for (Map.Entry<String, String> entry : files) {
      parameter(entry.key, entry.value);
    }
  }

  /**
   * DSL method to specify job and JVM properties to set on the job. For Pig jobs, additionally
   * allows you to specify Pig parameters by using the syntax "set parameters: [ ... ]".
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set; a key 'jvmProperties' with a map value that specifies the JVM properties to set; or a key 'parameters' with a map value that specifies the Pig parameters to set.
   */
  @Override
  void set(Map args) {
    super.set(args);
    if (args.containsKey("parameters")) {
      Map<String, String> parameters = args.parameters;
      this.parameters.putAll(parameters);
    }
  }

  /**
   * DSL method uses specifies the Pig script for the job. The specified value can be either an
   * absolute or relative path to the script file. This method causes the property pig.script=value
   * to be added the job. This method is required to build the job.
   *
   * @param script The Pig script for the job
   */
  void uses(String script) {
    this.script = script;
  }
}


/**
 * Job class for type=VoldemortBuildandPush jobs.
 * <p>
 * These are documentated internally at LinkedIn at https://iwww.corp.linkedin.com/wiki/cf/display/ENGS/Voldemort+Build+and+Push.
 * <p>
 * The code for this job is at https://github.com/voldemort/voldemort/blob/master/contrib/hadoop-store-builder/src/java/voldemort/store/readonly/mr/azkaban/VoldemortBuildAndPushJob.java.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. In the DSL, a VoldemortBuildandPush can be specified with:
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
class VoldemortBuildPushJob extends Job {
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
  }

  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    // Required
    allProperties["type"] = "VoldemortBuildandPush";
    allProperties["push.store.name"] = storeName;
    allProperties["push.cluster"] = clusterName;
    allProperties["build.input.path"] = buildInputPath;
    allProperties["build.output.dir"] = buildOutputPath;
    allProperties["push.store.owners"] = storeOwners;
    allProperties["push.store.description"] = storeDesc;

    // Optional
    if (buildTempDir != null) {
      allProperties["build.temp.dir"] = buildTempDir;
    }
    if (repFactor != null) {
      allProperties["build.replication.factor"] = repFactor;
    }
    if (compressValue != null) {
      allProperties["build.compress.value"] = compressValue;
    }
    if (keySelection != null) {
      allProperties["key.selection"] = keySelection;
    }
    if (valueSelection != null) {
      allProperties["value.selection"] = valueSelection;
    }
    if (numChunks != null) {
      allProperties["num.chunks"] = numChunks;
    }
    if (chunkSize != null) {
      allProperties["build.chunk.size"] = chunkSize;
    }
    if (keepOutput != null) {
      allProperties["build.output.keep"] = keepOutput;
    }
    if (pushHttpTimeoutSeconds != null) {
      allProperties["push.http.timeout.seconds"] = pushHttpTimeoutSeconds;
    }
    if (pushNode != null) {
      allProperties["push.node"] = pushNode;
    }
    if (buildStore != null) {
      allProperties["build"] = buildStore;
    }
    if (pushStore != null) {
      allProperties["push"] = pushStore;
    }
    if (fetcherProtocol != null) {
      allProperties["voldemort.fetcher.protocol"] = fetcherProtocol;
    }
    if (fetcherPort != null) {
      allProperties["voldemort.fetcher.port"] = fetcherPort;
    }
    if (isAvroSerializerVersioned != null) {
      allProperties["avro.serializer.versioned"] = isAvroSerializerVersioned;
    }
    if (isAvroData != null) {
      allProperties["build.type.avro"] = isAvroData;
    }
    if (isAvroData != null && isAvroData == true) {
      allProperties["avro.key.field"] = avroKeyField;
      allProperties["avro.value.field"] = avroValueField;
    }

    return super.buildProperties(allProperties);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  VoldemortBuildPushJob clone() {
    return clone(new VoldemortBuildPushJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
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
  }

  /**
   * DSL usesClusterName method causes push.cluster=value to be set in the job file.
   *
   * @param clusterName
   */
  void usesClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  /**
   * DSL usesInputPath method causes build.input.path=value to be set in the job file.
   *
   * @param buildInputPath
   */
  void usesInputPath(String buildInputPath) {
    this.buildInputPath = buildInputPath;
  }

  /**
   * DSL usesOutputPath method causes build.output.dir=value to be set in the job file.
   *
   * @param buildOutputPath
   */
  void usesOutputPath(String buildOutputPath) {
    this.buildOutputPath = buildOutputPath;
  }

  /**
   * DSL usesStoreOwners method causes push.store.owners=value to be set in the job file.
   *
   * @param storeOwners
   */
  void usesStoreOwners(String storeOwners) {
    this.storeOwners = storeOwners;
  }

  /**
   * DSL usesStoreDesc method causes push.store.description=value to be set in the job file.
   *
   * @param storeDesc
   */
  void usesStoreDesc(String storeDesc) {
    this.storeDesc = storeDesc;
  }

  /**
   * DSL usesTempDir method causes build.temp.dir=value to be set in the job file.
   *
   * @param buildTempDir
   */
  void usesTempDir(String buildTempDir) {
    this.buildTempDir = buildTempDir;
  }

  /**
   * DSL usesRepFactor method causes build.replication.factor=value to be set in the job file.
   *
   * @param repFactor
   */
  void usesRepFactor(Integer repFactor) {
    this.repFactor = repFactor;
  }

  /**
   * DSL usesCompressValue method causes build.compress.value=value to be set in the job file.
   *
   * @param compressValue
   */
  void usesCompressValue(Boolean compressValue) {
    this.compressValue = compressValue;
  }

  /**
   * DSL usesKeySelection method causes key.selection=value to be set in the job file.
   *
   * @param keySelection
   */
  void usesKeySelection(String keySelection) {
    this.keySelection = keySelection;
  }

  /**
   * DSL usesValueSelection method causes value.selection=value to be set in the job file.
   *
   * @param valueSelection
   */
  void usesValueSelection(String valueSelection) {
    this.valueSelection = valueSelection;
  }

  /**
   * DSL usesNumChunks method causes num.chunks=value to be set in the job file.
   *
   * @param numChunks
   */
  void usesNumChunks(Integer numChunks) {
    this.numChunks = numChunks;
  }

  /**
   * DSL usesChunkSize method causes build.chunk.size=value to be set in the job file.
   *
   * @param chunkSize
   */
  void usesChunkSize(Integer chunkSize) {
    this.chunkSize = chunkSize;
  }

  /**
   * DSL usesKeepOutput method causes build.output.keep=value to be set in the job file.
   *
   * @param keepOutput
   */
  void usesKeepOutput(Boolean keepOutput) {
    this.keepOutput = keepOutput;
  }

  /**
   * DSL usesPushHttpTimeoutSeconds method causes push.http.timeout.seconds=value to be set in the
   * job file.
   *
   * @param pushHttpTimeoutSeconds
   */
  void usesPushHttpTimeoutSeconds(Integer pushHttpTimeoutSeconds) {
    this.pushHttpTimeoutSeconds = pushHttpTimeoutSeconds;
  }

  /**
   * DSL usesPushNode method causes push.node=value to be set in the job file.
   *
   * @param pushNode
   */
  void usesPushNode(Integer pushNode) {
    this.pushNode = pushNode;
  }

  /**
   * DSL usesBuildStore method causes build=value to be set in the job file.
   *
   * @param buildStore
   */
  void usesBuildStore(Boolean buildStore) {
    this.buildStore = buildStore;
  }

  /**
   * DSL usesPushStore method causes push=value to be set in the job file.
   *
   * @param pushStore
   */
  void usesPushStore(Boolean pushStore) {
    this.pushStore = pushStore;
  }

  /**
   * DSL usesFetcherProtocol method causes voldemort.fetcher.protocol=value to be set in the job
   * file.
   *
   * @param fetcherProtocol
   */
  void usesFetcherProtocol(String fetcherProtocol) {
    this.fetcherProtocol = fetcherProtocol;
  }

  /**
   * DSL usesFetcherPort method causes voldemort.fetcher.port=value to be set in the job file.
   *
   * @param fetcherPort
   */
  void usesFetcherPort(String fetcherPort) {
    this.fetcherPort = fetcherPort;
  }

  /**
   * DSL usesAvroSerializerVersioned method causes avro.serializer.versioned=value to be set in the
   * job file.
   *
   * @param isAvroSerializerVersioned
   */
  void usesAvroSerializerVersioned(Boolean isAvroSerializerVersioned) {
    this.isAvroSerializerVersioned = isAvroSerializerVersioned;
  }

  /**
   * DSL usesAvroData method causes build.type.avro=value to be set in the job file.
   *
   * @param isAvroData
   */
  void usesAvroData(Boolean isAvroData) {
    this.isAvroData = isAvroData;
  }

  /**
   * DSL usesAvroKeyField method causes avro.key.field=value to be set in the job file. NOTE: this
   * property is only added to the job file if isAvroData is set to true.
   *
   * @param avroKeyField
   */
  void usesAvroKeyField(String avroKeyField) {
    this.avroKeyField = avroKeyField;
  }

  /**
   * DSL usesAvroValueField method causes avro.value.field=value to be set in the job file. NOTE:
   * this property is only added to the job file if isAvroData is set to true.
   *
   * @param avroValueField
   */
  void usesAvroValueField(String avroValueField) {
    this.avroValueField = avroValueField;
  }
}