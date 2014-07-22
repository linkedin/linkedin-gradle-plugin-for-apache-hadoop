package com.linkedin.gradle.azkaban;

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
  AzkabanFactory azkabanFactory;
  String name;
  Project project;
  List<AzkabanProperties> properties;

  // We keep track of all of the jobs declared in the workflow, even if they
  // are not transitive parents of the launch job.
  List<AzkabanJob> jobs;

  // The final job of the workflow (that will be used to launch the workflow
  // in Azkaban). Built from the launch job dependencies for the workflow.
  NoOpJob launchJob;
  Set<String> launchJobDependencies;

  // This will allow jobs to be referred to by name (e.g. when declaring
  // dependencies). This also implicitly provides scoping for job names.
  NamedScope workflowScope;

  AzkabanWorkflow(String name, Project project) {
    this(name, project, null);
  }

  AzkabanWorkflow(String name, Project project, NamedScope nextLevel) {
    this.azkabanFactory = project.extensions.azkabanFactory;
    this.jobs = new ArrayList<AzkabanJob>();
    this.launchJob = azkabanFactory.makeNoOpJob(name);
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

  void build(String directory) throws IOException {
    if ("default".equals(name)) {
      buildDefault(directory);
      return;
    }

    launchJob.dependencyNames.addAll(launchJobDependencies);
    List<AzkabanJob> jobList = buildJobList(launchJob, new ArrayList<AzkabanJob>());

    // If there was more than one launch dependency, build the launch job, otherwise do not.
    if (launchJobDependencies.size() > 1) {
      launchJob.build(directory, null);
    }

    jobList.each() { job ->
      job.build(directory, name);
    }

    properties.each() { props ->
      props.build(directory, name);
    }
  }

  // In the special "default" workflow, just build all the jobs as they are, with no launch job.
  // In this workflow, don't prefix job file names with the workflow name.
  void buildDefault(String directory) throws IOException {
    if (!"default".equals(name)) {
      throw new Exception("You cannot buildDefault except on the 'default' workflow");
    }

    jobs.each() { job ->
      job.build(directory, null);
    }

    properties.each() { props ->
      props.build(directory, null);
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

  // Helper method to configure AzkabanJob in the DSL. Can be called by subclasses to configure
  // custom AzkabanJob subclass types.
  AzkabanJob configureJob(AzkabanJob job, Closure configure) {
    AzkabanMethods.configureJob(project, job, configure, workflowScope);
    jobs.add(job);
    return job;
  }

  // Helper method to configure AzkabanProperties in the DSL. Can be called by subclasses to
  // configure custom AzkabanProperties subclass types.
  AzkabanProperties configureProperties(AzkabanProperties props, Closure configure) {
    AzkabanMethods.configureProperties(project, props, configure, workflowScope);
    properties.add(props);
    return props;
  }

  String toString() {
    return "(AzkabanWorkflow: name = ${name})";
  }

  void depends(String... jobNames) {
    launchJobDependencies.addAll(jobNames.toList());
  }

  Object lookup(String name) {
    return AzkabanMethods.lookup(name, workflowScope);
  }

  Object lookup(String name, Closure configure) {
    return AzkabanMethods.lookup(project, name, workflowScope, configure);
  }

  AzkabanJob addJob(String name, Closure configure) {
    return configureJob(AzkabanMethods.cloneJob(name, workflowScope), configure);
  }

  AzkabanJob addJob(String name, String rename, Closure configure) {
    return configureJob(AzkabanMethods.cloneJob(name, rename, workflowScope), configure);
  }

  AzkabanProperties addPropertyFile(String name, Closure configure) {
    return configureProperties(AzkabanMethods.clonePropertyFile(name, workflowScope), configure);
  }

  AzkabanProperties addPropertyFile(String name, String rename, Closure configure) {
    return configureProperties(AzkabanMethods.clonePropertyFile(name, rename, workflowScope), configure);
  }

  AzkabanJob azkabanJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeAzkabanJob(name), configure);
  }

  CommandJob commandJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeCommandJob(name), configure);
  }

  HadoopJavaJob hadoopJavaJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeHadoopJavaJob(name), configure);
  }

  HiveJob hiveJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeHiveJob(name), configure);
  }

  JavaJob javaJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeJavaJob(name), configure);
  }

  JavaProcessJob javaProcessJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeJavaProcessJob(name), configure);
  }

  KafkaPushJob kafkaPushJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeKafkaPushJob(name), configure);
  }

  NoOpJob noOpJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeNoOpJob(name), configure);
  }

  PigJob pigJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makePigJob(name), configure);
  }

  VoldemortBuildPushJob voldemortBuildPushJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeVoldemortBuildPushJob(name), configure);
  }

  AzkabanProperties propertyFile(String name, Closure configure) {
    return configureProperties(azkabanFactory.makeAzkabanProperties(name), configure);
  }
}