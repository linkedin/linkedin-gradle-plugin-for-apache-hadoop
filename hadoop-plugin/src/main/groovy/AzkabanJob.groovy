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
    jobProperties = new HashMap<String, String>();
    jvmProperties = new HashMap<String, String>();
    name = jobName;
    reading = new ArrayList<String>();
    writing = new ArrayList<String>();
  }

  void build(String directory, String workflowName) throws IOException {
    // Use a LinkedHashMap so that the properties will be enumerated in the
    // order in which we add them.
    Map<String, String> allProperties = buildProperties(new LinkedHashMap<String, String>());

    File file = new File(directory, "${workflowName}-${name}.job");
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

  void caches(String pathName, Map<String, String> options) {
    reads(pathName, null);
    String symLink = options["as"];
    String cacheLink = "${pathName}#${symLink}";

    if (jvmProperties.containsKey("mapred.cache.files")) {
      setJvm("mapred.cache.files", jvmProperties["mapred.cache.files"] + "," + cacheLink);
    }
    else {
      setJvm("mapred.cache.files", cacheLink);
    }

    setJvm("mapred.create.symlink", "yes");
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

  // Override this to handle subclass-specific options
  void reads(String pathName, Map<String, String> options) {
    reading.add(pathName);
  }

  // Override this to handle subclass-specific options
  void writes(String pathName, Map<String, String> options) {
    writing.add(pathName);
  }

  void setJob(String name, String value) {
    jobProperties.put(name, value)
  }

  void setJvm(String name, String value) {
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
  // Only one of query or queryFile should be set
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
    parameters = new HashMap<String, String>();
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

  void reads(String pathName, Map<String, String> options) {
    super.reads(pathName, options);
    if (options != null && options.containsKey("as")) {
      String param = options["as"];
      parameter(param, pathName);
    }
  }

  void uses(String script) {
    this.script = script;
  }
}

class VoldemortBuildPushJob extends AzkabanJob {
  VoldemortBuildPushJob(String jobName) {
    super(jobName);
  }

  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties["type"] = "VoldemortBuildandPush";
    return super.buildProperties(allProperties);
  }

  VoldemortBuildPushJob clone() {
    VoldemortBuildPushJob cloneJob = new VoldemortBuildPushJob(name);
    return clone(cloneJob);
  }
}