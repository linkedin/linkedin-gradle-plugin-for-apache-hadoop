package com.linkedin.gradle.hadoop;

import org.gradle.api.Project;

/**
 * The azkaban { ... } block consists of a series of Azkaban Workflows,
 * declared as follows:
 *
 * azkaban {
 *   workflow('workflowName') {
 *     ...
 *   }
 * }
 */
class AzkabanWorkflow implements NamedScopeContainer {
  String name;
  Project project;
  List<AzkabanProperties> properties;

  // We keep track of all of the jobs declared in the workflow, even if they
  // are not transitive parents of the launch job.
  List<AzkabanJob> jobs;

  // The final job of the workflow (that will be used to launch the workflow
  // in Azkaban). Built from the launch job dependencies for the workflow.
  NoopJob launchJob;
  Set<String> launchJobDependencies;

  // This will allow jobs to be referred to by name (e.g. when declaring
  // dependencies). This also implicitly provides scoping for job names.
  NamedScope workflowScope;

  AzkabanWorkflow(String name) {
    this(name, null);
  }

  AzkabanWorkflow(String name, Project project) {
    this(name, project, null);
  }

  AzkabanWorkflow(String name, Project project, NamedScope nextLevel) {
    this.jobs = new ArrayList<AzkabanJob>();
    this.launchJob = new NoopJob(name);
    this.launchJobDependencies = new LinkedHashSet<String>();
    this.name = name;
    this.project = project;
    this.properties = new ArrayList<AzkabanProperties>();
    this.workflowScope = new NamedScope(name, nextLevel);
  }

  @Override
  public NamedScope getScope() {
    return workflowScope;
  }

  AzkabanJob addAndConfigureJob(AzkabanJob job, Closure configure) {
    jobs.add(job);
    workflowScope.bind(job.name, job);
    project.configure(job, configure);
    return job;
  }

  AzkabanJob addJob(String name, Closure configure) {
    AzkabanJob job = workflowScope.lookup(name);
    if (job == null) {
      throw new Exception("Could not find job ${name} in call to addJob");
    }
    AzkabanJob clone = job.clone();
    return addAndConfigureJob(clone, configure);
  }

  AzkabanJob addJob(String name, String rename, Closure configure) {
    AzkabanJob job = workflowScope.lookup(name);
    if (job == null) {
      throw new Exception("Could not find job ${name} in call to addJob");
    }
    AzkabanJob clone = job.clone();
    clone.name = rename;
    return addAndConfigureJob(clone, configure);
  }

  AzkabanProperties addPropertyFile(String name, Closure configure) {
    AzkabanProperties props = workflowScope.lookup(name);
    if (props == null) {
      throw new Exception("Could not find property set ${name} in call to addPropertyFile");
    }
    AzkabanProperties clone = props.clone();
    workflowScope.bind(name, clone);
    project.configure(clone, configure);
    properties.add(clone);
    return clone;
  }

  AzkabanProperties addPropertyFile(String name, String rename, Closure configure) {
    AzkabanProperties props = workflowScope.lookup(name);
    if (props == null) {
      throw new Exception("Could not find property set ${name} in call to addPropertyFile");
    }
    AzkabanProperties clone = props.clone();
    clone.name = rename;
    workflowScope.bind(rename, clone);
    project.configure(clone, configure);
    properties.add(clone);
    return clone;
  }

  void build(String directory) throws IOException {
    launchJob.dependencyNames.addAll(launchJobDependencies);
    List<AzkabanJob> jobList = buildJobList(launchJob, new ArrayList<AzkabanJob>());

    // In the special "default" workflow, don't prefix job file names with the workflow name.
    String parentName = "default".equals(name) ? null : name;

    jobList.each() { job ->
      job.build(directory, parentName);
    }

    properties.each() { props ->
      props.build(directory, parentName);
    }
  }

  // Topologically generate the list of jobs to build for this workflow by
  // asking the given job to lookup its named dependencies in the current scope
  // and add them to the job list.
  List<AzkabanJob> buildJobList(AzkabanJob job, List<AzkabanJob> jobList) {
    job.updateDependencies(workflowScope);

    // Add the children of this job in a breadth-first manner
    for (AzkabanJob childJob : job.dependencies) {
      jobList.add(childJob);
    }

    for (AzkabanJob childJob : job.dependencies) {
      buildJobList(childJob, jobList);
    }

    return jobList;
  }

  AzkabanWorkflow clone() {
    AzkabanWorkflow workflow = new AzkabanWorkflow(name, project, null);
    workflow.launchJob = launchJob.clone();
    workflow.launchJobDependencies.addAll(launchJobDependencies);
    workflow.workflowScope = workflowScope.clone();

    // Clear the scope for the cloned workflow. Then clone all the jobs
    // declared in the original workflow and use them to rebuild the scope.
    workflow.workflowScope.thisLevel.clear();

    for (AzkabanJob job : jobs) {
      AzkabanJob jobClone = job.clone();
      jobClone.dependencies.clear();
      workflow.jobs.add(jobClone);
      workflow.workflowScope.bind(job.name, job);
    }

    return workflow;
  }

  void depends(String... jobNames) {
    launchJobDependencies.addAll(jobNames.toList());
  }

  Object lookup(String name) {
    return workflowScope.lookup(name);
  }

  Object lookup(String name, Closure configure) {
    Object boundObject = workflowScope.lookup(name);
    if (boundObject == null) {
      return null;
    }
    project.configure(boundObject, configure);
    return boundObject;
  }

  AzkabanProperties propertyFile(String name, Closure configure) {
    AzkabanProperties props = new AzkabanProperties(name);
    workflowScope.bind(name, props);
    project.configure(props, configure);
    properties.add(props);
    return props;
  }

  AzkabanJob azkabanJob(String name, Closure configure) {
    return addAndConfigureJob(new AzkabanJob(name), configure);
  }

  CommandJob commandJob(String name, Closure configure) {
    return addAndConfigureJob(new CommandJob(name), configure);
  }

  HiveJob hiveJob(String name, Closure configure) {
    return addAndConfigureJob(new HiveJob(name), configure);
  }

  JavaJob javaJob(String name, Closure configure) {
    return addAndConfigureJob(new JavaJob(name), configure);
  }

  JavaProcessJob javaProcessJob(String name, Closure configure) {
    return addAndConfigureJob(new JavaProcessJob(name), configure);
  }

  KafkaPushJob kafkaPushJob(String name, Closure configure) {
    return addAndConfigureJob(new KafkaPushJob(name), configure);
  }

  PigJob pigJob(String name, Closure configure) {
    return addAndConfigureJob(new PigJob(name), configure);
  }

  VoldemortBuildPushJob voldemortBuildPushJob(String name, Closure configure) {
    return addAndConfigureJob(new VoldemortBuildPushJob(name), configure);
  }
}