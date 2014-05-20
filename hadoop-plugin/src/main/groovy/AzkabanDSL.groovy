import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.gradle.api.Project;

/**
 * AzkabanExtension will be the class that exposes the DSL to users. To use
 * the DSL, users should add the
 *
 * azkaban {
 *   ...
 * }
 *
 * configuration block to their build.gradle file.
 */
class AzkabanExtension {
  Map<String, String> properties;
  List<AzkabanWorkflow> workflows;

  // The directory in which to build the job files defaults to ./conf/jobs but
  // may be set in the DSL.
  String jobConfDir = "./conf/jobs";

  static Project project;

  AzkabanExtension(Project gradleProject) {
    project = gradleProject;
    properties = new HashMap<String, String>();
    workflows = new ArrayList<AzkabanJob>();
  }

  void build() throws IOException {
    File file = new File(jobConfDir);
    if (!file.isDirectory() || !file.exists()) {
      throw new IOException("Directory ${jobConfDir} does not exist or is not a directory");
    }

    workflows.each() { workflow ->
      workflow.build(jobConfDir);
    }

    buildProperties();
  }

  // TODO figure out how to name the .properties file
  void buildProperties() {
    if (properties.size() > 0) {
      File file = new File(jobConfDir, "common.properties");
      file.withWriter { out ->
        properties.each() { key, value ->
          out.writeLine("${key}=${value}");
        }
      }
    }
  }

  AzkabanWorkflow workflow(String name, Closure configure) {
    println "AzkabanExtension workflow: " + name
    AzkabanWorkflow flow = new AzkabanWorkflow(name, project);
    project.configure(flow, configure);
    workflows.add(flow);
    return flow;
  }
}

class AzkabanWorkflow {
  Set<String> dependencyNames;
  String name;
  Project project;

  // This will allow jobs to be referred to by name (e.g. when declaring
  // dependencies). This also implicitly provides scoping for job names.
  Map<String, AzkabanJob> nameJobMap;

  // The final job of the workflow (that will be used to launch the workflow
  // in Azkaban). Built from the dependencyNames for the workflow.
  NoopJob workflowJob;

  AzkabanWorkflow(String name, Project project) {
    this.dependencyNames = new LinkedHashSet<String>();
    this.name = name;
    this.nameJobMap = new HashMap<String, AzkabanJob>();
    this.project = project;
    this.workflowJob = null;
  }

  void build(String directory) throws IOException {
    workflowJob = new NoopJob(name);
    workflowJob.dependencyNames.addAll(dependencyNames);

    List<AzkabanJob> jobList = buildJobList(new ArrayList<AzkabanJob>(), workflowJob);

    jobList.each() { job ->
      job.build(directory, name);
    }
  }

  List<AzkabanJob> buildJobList(List<AzkabanJob> jobList, AzkabanJob job) {
    // Tell the job to lookup its named job dependencies in this workflow
    job.updateDependencies(nameJobMap);

    // Let's do this breath first, so add all the child first
    for (AzkabanJob childJob : job.dependencies) {
      jobList.add(childJob);
    }

    // Then do the recursion
    for (AzkabanJob childJob : job.dependencies) {
      buildJobList(jobList, childJob);
    }

    return jobList;
  }

  AzkabanJob configureAndAdd(AzkabanJob job, Closure configure) {
    project.configure(job, configure);
    if (nameJobMap.containsKey(job.name)) {
      throw new Exception("Found two jobs with the name ${job.name} in workflow ${this.name}");
    }
    nameJobMap.put(job.name, job);
    return job;
  }

  void depends(String... jobNames) {
    dependencyNames.addAll(jobNames.toList());
  }

  AzkabanJob azkabanJob(String name, Closure configure) {
    return configureAndAdd(new AzkabanJob(name), configure);
  }

  CommandJob commandJob(String name, Closure configure) {
    return configureAndAdd(new CommandJob(name), configure);
  }

  HiveJob hiveJob(String name, Closure configure) {
    return configureAndAdd(new HiveJob(name), configure);
  }

  JavaJob javaJob(String name, Closure configure) {
    return configureAndAdd(new JavaJob(name), configure);
  }

  JavaProcessJob javaProcessJob(String name, Closure configure) {
    return configureAndAdd(new JavaProcessJob(name), configure);
  }

  PigJob pigJob(String name, Closure configure) {
    return configureAndAdd(new PigJob(name), configure);
  }

  VoldemortBuildPushJob voldemortBuildPushJob(String name, Closure configure) {
    return configureAndAdd(new VoldemortBuildPushJob(name), configure);
  }
}

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
  void updateDependencies(Map<String, AzkabanJob> nameJobMap) {
    dependencyNames.each { jobName ->
      if (!nameJobMap.containsKey(jobName)) {
        throw new Exception("Dependency ${jobName} for job ${this.name} not defined");
      }
      dependencies.add(nameJobMap.get(jobName));
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
}
