package com.linkedin.gradle.hadoop;

import bsh.This;

/**
 * Base class for all Azkaban job types.
 */
class AzkabanJob {
  String name;
  Set<AzkabanJob> dependencies;
  Set<String> dependencyNames;
  Map<String, String> jobProperties;
  Map<String, String> jvmProperties;
  List<String> reading;
  List<String> writing;

  AzkabanJob(String jobName) {
    dependencies = new LinkedHashSet<AzkabanJob>();
    dependencyNames = new LinkedHashSet<String>();
    jobProperties = new LinkedHashMap<String, String>();
    jvmProperties = new LinkedHashMap<String, String>();
    name = jobName;
    reading = new ArrayList<String>();
    writing = new ArrayList<String>();
  }

  void build(String directory, String parentName) throws IOException {
    // Use a LinkedHashMap so that the properties will be enumerated in the
    // order in which we add them.
    Map<String, String> allProperties = buildProperties(new LinkedHashMap<String, String>());

    String fileName = parentName == null ? name : "${parentName}-${name}";
    File file = new File(directory, "${fileName}.job");

    file.withWriter { out ->
      allProperties.each() { key, value ->
        out.writeLine("${key}=${value}");
      }
    }
  }

  // Subclasses should override this method to add their own properties, and
  // then call this base class method to add properties common to all jobs.
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    if (dependencies.size() > 0) {
      String dependencyNames = dependencies.collect() { job -> return job.name }.join(",");
      allProperties["dependencies"] = dependencyNames;
    }

    if (jvmProperties.size() > 0) {
      String jvmArgs = jvmProperties.collect() { key, val -> return "-D${key}=${val}" }.join(" ");
      allProperties["jvm.args"] = jvmArgs;
    }

    jobProperties.each() { key, value ->
      allProperties[key] = value;
    }

    return allProperties;
  }

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

  // If cloning more than one job, it's up to external callers to clear
  // job.dependencies and rebuild it if necessary.
  AzkabanJob clone() {
    AzkabanJob cloneJob = new AzkabanJob(name);
    return clone(cloneJob);
  }

  // Helper method to clone a job, intended to make it easier for subclasses
  // to override the clone method and call this helper method.
  AzkabanJob clone(AzkabanJob cloneJob) {
    cloneJob.dependencyNames.addAll(dependencyNames);
    cloneJob.dependencies.addAll(dependencies);
    cloneJob.jobProperties.putAll(jobProperties);
    cloneJob.jvmProperties.putAll(jvmProperties);
    cloneJob.reading.addAll(reading);
    cloneJob.writing.addAll(writing);
    return cloneJob;
  }

  void depends(String... jobNames) {
    dependencyNames.addAll(jobNames.toList());
  }

  // Setting the queue is common enough that we give it its own method
  void queue(String queueName) {
    jvmProperties.put("mapred.job.queue.name", queueName);
  }

  // Override this to handle subclass-specific handling of the file key
  void reads(Map args) {
    Map<String, String> files = args.files;
    for (String pathName : files.values()) {
      reading.add(pathName);
    }
  }

  // Override this to handle subclass-specific handling of the file key
  void writes(Map args) {
    Map<String, String> files = args.files;
    for (String pathName : files.values()) {
      writing.add(pathName);
    }
  }

  void set(Map args) {
    if (args.containsKey("properties")) {
      Map<String, String> properties = args.properties;
      jobProperties.putAll(properties);
    }
    if (args.containsKey("jvmProperties")) {
      Map<String, String> jvmProperties = args.jvmProperties;
      jvmProperties.putAll(jvmProperties);
    }
  }

  void setJobProperty(String name, String value) {
    jobProperties.put(name, value)
  }

  void setJvmProperty(String name, String value) {
    jvmProperties.put(name, value);
  }

  // Tell the job to update its job dependencies from its named dependencies.
  // It is necessary to do this before we build the job.
  void updateDependencies(NamedScope scope) {
    dependencyNames.each { jobName ->
      if (!scope.contains(jobName)) {
        throw new Exception("Dependency ${jobName} for job ${this.name} not defined");
      }
      Object val = scope.lookup(jobName);
      if (!(val instanceof AzkabanJob)) {
        throw new Exception("Dependency ${jobName} for job ${this.name} resolves to an object that is not an AzkabanJob");
      }
      dependencies.add((AzkabanJob)val);
    }
  }
}


class CommandJob extends AzkabanJob {
  String command;

  CommandJob(String jobName) {
    super(jobName);
  }

  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "command";
    allProperties["command"] = command;
    return super.buildProperties(allProperties);
  }

  CommandJob clone() {
    CommandJob cloneJob = new CommandJob(name);
    cloneJob.command = command;
    return clone(cloneJob);
  }

  void uses(String command) {
    this.command = command;
  }
}


class HiveJob extends AzkabanJob {
  // Exactly one of query or queryFile must be set
  String query;
  String queryFile;

  HiveJob(String jobName) {
    super(jobName);
  }

  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "hive";
    allProperties["azk.hive.action"] = "execute.query";
    if (query) {
      allProperties["hive.query"] = query;
    }
    if (queryFile) {
      allProperties["hive.query.file"] = queryFile;
    }
    return super.buildProperties(allProperties);
  }

  HiveJob clone() {
    HiveJob cloneJob = new HiveJob(name);
    cloneJob.query = query;
    cloneJob.queryFile = queryFile;
    return clone(cloneJob);
  }

  void usesQuery(String query) {
    this.query = query;
  }

  void usesQueryFile(String queryFile) {
    this.queryFile = queryFile;
  }
}


class JavaJob extends AzkabanJob {
  String jobClass;

  JavaJob(String jobName) {
    super(jobName);
  }

  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "java";
    allProperties["job.class"] = jobClass;
    return super.buildProperties(allProperties);
  }

  JavaJob clone() {
    JavaJob cloneJob = new JavaJob(name);
    cloneJob.jobClass = jobClass;
    return clone(cloneJob);
  }

  void uses(String jobClass) {
    this.jobClass = jobClass;
  }
}


class JavaProcessJob extends AzkabanJob {
  String javaClass;

  JavaProcessJob(String jobName) {
    super(jobName);
  }

  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "javaprocess";
    allProperties["java.class"] = javaClass;
    return super.buildProperties(allProperties);
  }

  JavaProcessJob clone() {
    JavaProcessJob cloneJob = new JavaProcessJob(name);
    cloneJob.javaClass = javaClass;
    return clone(cloneJob);
  }

  void uses(String javaClass) {
    this.javaClass = javaClass;
  }
}


class KafkaPushJob extends AzkabanJob {
  String inputPath;
  String topic;

  // Optional
  Integer batchNumBytes;
  Boolean disableSchemaRegistration;
  String kafkaUrl;
  String nameNode;
  String schemaRegistryUrl;

  KafkaPushJob(String jobName) {
    super(jobName);
  }

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

  KafkaPushJob clone() {
    KafkaPushJob cloneJob = new KafkaPushJob(name);
    cloneJob.inputPath = inputPath;
    cloneJob.topic = topic;
    cloneJob.batchNumBytes = batchNumBytes;
    cloneJob.kafkaUrl = kafkaUrl;
    cloneJob.nameNode = nameNode;
    cloneJob.schemaRegistryUrl = schemaRegistryUrl;
    return clone(cloneJob);
  }

  void usesInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  void usesTopic(String topic) {
    this.topic = topic;
  }

  void usesBatchNumBytes(Integer batchNumBytes) {
    this.batchNumBytes = batchNumBytes;
  }

  void usesDisableSchemaRegistration(Boolean disableSchemaRegistration) {
    this.disableSchemaRegistration = disableSchemaRegistration;
  }

  void usesKafkaUrl(String kafkaUrl) {
    this.kafkaUrl = kafkaUrl;
  }

  void usesNameNode(String nameNode) {
    this.nameNode = nameNode;
  }

  void usesSchemaRegistryUrl(String schemaRegistryUrl) {
    this.schemaRegistryUrl = schemaRegistryUrl;
  }
}


class NoopJob extends AzkabanJob {
  NoopJob(String jobName) {
    super(jobName);
  }

  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "noop";
    return super.buildProperties(allProperties);
  }

  NoopJob clone() {
    NoopJob cloneJob = new NoopJob(name);
    return clone(cloneJob);
  }
}


class PigJob extends AzkabanJob {
  Map<String, String> parameters;
  String script;

  PigJob(String jobName) {
    super(jobName);
    parameters = new LinkedHashMap<String, String>();
  }

  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "pig";
    allProperties["pig.script"] = script;
    parameters.each() { key, value ->
      allProperties["param.${key}"] = "${value}";
    }
    return super.buildProperties(allProperties);
  }

  PigJob clone() {
    PigJob cloneJob = new PigJob(name);
    cloneJob.parameters.putAll(parameters);
    cloneJob.script = script;
    return clone(cloneJob);
  }

  void parameter(String name, String value) {
    parameters.put(name, value);
  }

  void reads(Map args) {
    super.reads(args);
    Map<String, String> files = args.files;
    for (Map.Entry<String, String> entry : files) {
      parameter(entry.key, entry.value);
    }
  }

  void set(Map args) {
    super.set(args);
    if (args.containsKey("parameters")) {
      Map<String, String> parameters = args.parameters;
      this.parameters.putAll(parameters);
    }
  }

  void uses(String script) {
    this.script = script;
  }

  void writes(Map args) {
    super.reads(args);
    Map<String, String> files = args.files;
    for (Map.Entry<String, String> entry : files) {
      parameter(entry.key, entry.value);
    }
  }
}


class VoldemortBuildPushJob extends AzkabanJob {
  String storeDesc;
  String storeName;
  String storeOwners;
  String buildInputPath;
  String buildOutputPath;
  Integer repFactor;
  boolean isAvroData = false;
  String avroKeyField;    // Required if isAvroData is true
  String avroValueField;  // Required if isAvroData is true

  VoldemortBuildPushJob(String jobName) {
    super(jobName);
  }

  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "VoldemortBuildandPush";
    allProperties["push.store.description"] = storeDesc;
    allProperties["push.store.name"] = storeName;
    allProperties["push.store.owners"] = storeOwners;
    allProperties["build.input.path"] = buildInputPath;
    allProperties["build.output.dir"] = buildOutputPath;
    allProperties["build.replication.factor"] = repFactor;
    allProperties["build.type.avro"] = isAvroData;
    if (isAvroData == true) {
      allProperties["avro.key.field"] = avroKeyField;
      allProperties["avro.value.field"] = avroValueField;
    }
    return super.buildProperties(allProperties);
  }

  VoldemortBuildPushJob clone() {
    VoldemortBuildPushJob cloneJob = new VoldemortBuildPushJob(name);
    cloneJob.storeDesc = storeDesc;
    cloneJob.storeName = storeName;
    cloneJob.storeOwners = storeOwners;
    cloneJob.buildInputPath = buildInputPath;
    cloneJob.buildOutputPath = buildOutputPath;
    cloneJob.repFactor = repFactor;
    cloneJob.isAvroData = isAvroData;
    cloneJob.avroKeyField = avroKeyField;
    cloneJob.avroValueField = avroValueField;
    return clone(cloneJob);
  }

  void usesStoreDesc(String storeDesc) {
    this.storeDesc = storeDesc;
  }

  void usesStoreName(String storeName) {
    this.storeName = storeName;
  }

  void usesStoreOwners(String storeOwners) {
    this.storeOwners = storeOwners;
  }

  void usesInputPath(String buildInputPath) {
    this.buildInputPath = buildInputPath;
  }

  void usesOutputPath(String buildOutputPath) {
    this.buildOutputPath = buildOutputPath;
  }

  void usesRepFactor(Integer repFactor) {
    this.repFactor = repFactor;
  }

  void usesAvroData(boolean isAvroData) {
    this.isAvroData = isAvroData;
  }

  void usesAvroKeyField(String avroKeyField) {
    this.avroKeyField = avroKeyField;
  }

  void usesAvroValueField(String avroValueField) {
    this.avroValueField = avroValueField;
  }
}