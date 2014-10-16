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
package com.linkedin.gradle.azkaban;

import org.gradle.api.Project;

/**
 * An AzkabanWorkflow is a collection of jobs and properties that represent a logical workflow,
 * i.e. jobs with dependencies that form a DAG.
 * <p>
 * In the DSL, a workflow can be specified with:
 * <pre>
 *   workflow('workflowName') {
 *     // Declare jobs and properties
 *     ...
 *     // Declare the job names the workflow executes
 *     executes 'jobName1', 'jobName2'
 *   }
 * </pre>
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
  LaunchJob launchJob;
  Set<String> launchJobDependencies;

  // This will allow jobs to be referred to by name (e.g. when declaring
  // dependencies). This also implicitly provides scoping for job names.
  NamedScope workflowScope;

  /**
   * Base constructor for an AzkabanWorkflow.
   *
   * @param name The workflow name
   * @param project The Gradle project
   */
  AzkabanWorkflow(String name, Project project) {
    this(name, project, null);
  }

  /**
   * Constructor for an AzkabanWorkflow given a parent scope.
   *
   * @param name The workflow name
   * @param project The Gradle project
   * @param nextLevel The parent scope
   */
  AzkabanWorkflow(String name, Project project, NamedScope nextLevel) {
    this.azkabanFactory = project.extensions.azkabanFactory;
    this.jobs = new ArrayList<AzkabanJob>();
    this.launchJob = azkabanFactory.makeLaunchJob(name);
    this.launchJobDependencies = new LinkedHashSet<String>();
    this.name = name;
    this.project = project;
    this.properties = new ArrayList<AzkabanProperties>();
    this.workflowScope = new NamedScope(name, nextLevel);
  }

  /**
   * Returns the scope at this level.
   *
   * @return The scope at this level
   */
  @Override
  public NamedScope getScope() {
    return workflowScope;
  }

  /**
   * Builds the workflow.
   * <p>
   * NOTE: not all jobs in the workflow are built by default. Only those jobs that can be found
   * from a transitive walk starting from the jobs the workflow executes actually get built.
   *
   * @param directory The directory in which to build the workflow files
   * @param parentScope The fully-qualified name of the scope in which the workflow is bound
   */
  void build(String directory, String parentScope) throws IOException {
    if ("default".equals(name)) {
      buildDefault(directory, parentScope);
      return;
    }

    // Build the list of jobs to build for the workflow.
    Set<AzkabanJob> jobsToBuild = buildJobList();

    // Build the all the jobs and properties in the workflow.
    parentScope = parentScope == null ? name : "${parentScope}_${name}";

    jobsToBuild.each() { job ->
      job.build(directory, parentScope);
    }

    properties.each() { props ->
      props.build(directory, parentScope);
    }
  }

  /**
   * Helper method to build the special default workflow, which is a workflow with the special name
   * "default". In the default workflow, the fully-qualified name of the parent scope is not added
   * to the file names; and all jobs in the workflow are built (rather than only those jobs that
   * can be found from a transitive walk of the jobs the workflow executes).
   *
   * @param directory The directory in which to build the workflow files
   * @param parentScope The fully-qualified name of the scope in which the workflow is bound
   */
  void buildDefault(String directory, String parentScope) throws IOException {
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

  /**
   * Generate the list of jobs to build for this workflow by performing a transitive (breadth-
   * first) walk of the jobs in the workflow, starting from the jobs the workflow executes.
   * <p>
   * NOTE: this means that users can declare jobs in a workflow that are not built, if there is no
   * transitive path from the jobs the workflow executes to a declared job. This capability is by
   * design. In this case, the static checker will display a warning message.
   *
   * @return The list (as a LinkedHashSet) of jobs to build for the workflow
   */
  Set<AzkabanJob> buildJobList() {
    Map<String, AzkabanJob> jobMap = buildJobMap();
    Queue<AzkabanJob> queue = new LinkedList<AzkabanJob>();
    Set<AzkabanJob> jobsToBuild = new LinkedHashSet<AzkabanJob>();

    launchJob.dependencyNames.addAll(launchJobDependencies);
    queue.add(launchJob);

    while (!queue.isEmpty()) {
      AzkabanJob job = queue.remove();

      if (!jobsToBuild.contains(job)) {
        jobsToBuild.add(job);

        // Add the parents of this job to the queue in a breadth-first manner.
        for (String parentJob : job.dependencyNames) {
          queue.add(jobMap.get(parentJob));
        }
      }
    }
    return jobsToBuild;
  }

  /**
   * Helper function to return a map of the job names to jobs in the workflow. This does not
   * include the launch job that is implicitly added when the workflow is built.
   *
   * @return A map of the job names to jobs in the workflow
   */
  Map<String, AzkabanJob> buildJobMap() {
    Map<String, AzkabanJob> jobMap = new HashMap<String, AzkabanJob>();

    jobs.each() { AzkabanJob job ->
      jobMap.put(job.name, job);
    }

    return jobMap;
  }

  /**
   * Clones the workflow.
   *
   * @return The cloned workflow
   */
  AzkabanWorkflow clone() {
    return clone(new AzkabanWorkflow(name, project, null));
  }

  /**
   * Helper method to set the properties on a cloned workflow.
   *
   * @param workflow The workflow being cloned
   * @return The cloned workflow
   */
  AzkabanWorkflow clone(AzkabanWorkflow workflow) {
    workflow.launchJob = launchJob.clone();
    workflow.launchJobDependencies.addAll(launchJobDependencies);
    workflow.workflowScope = workflowScope.clone();

    // Clear the scope for the cloned workflow. Then clone all the jobs
    // declared in the original workflow and use them to rebuild the scope.
    workflow.workflowScope.thisLevel.clear();

    for (AzkabanJob job : jobs) {
      AzkabanJob jobClone = job.clone();
      workflow.jobs.add(jobClone);
      workflow.workflowScope.bind(job.name, job);
    }

    return workflow;
  }

  /**
   * Helper method to configure AzkabanJob in the DSL. Can be called by subclasses to configure
   * custom AzkabanJob subclass types.
   *
   * @param job The job to configure
   * @param configure The configuration closure
   * @return The input job, which is now configured
   */
  AzkabanJob configureJob(AzkabanJob job, Closure configure) {
    AzkabanMethods.configureJob(project, job, configure, workflowScope);
    jobs.add(job);
    return job;
  }

  /**
   * Helper method to configure AzkabanProperties in the DSL. Can be called by subclasses to
   * configure custom AzkabanProperties subclass types.
   *
   * @param props The properties to configure
   * @param configure The configuration closure
   * @return The input properties, which is now configured
   */
  AzkabanProperties configureProperties(AzkabanProperties props, Closure configure) {
    AzkabanMethods.configureProperties(project, props, configure, workflowScope);
    properties.add(props);
    return props;
  }

  /**
   * Returns a string representation of the scope.
   *
   * @return A string representation of the scope
   */
  String toString() {
    return "(AzkabanWorkflow: name = ${name})";
  }

  /**
   * @deprecated This method has been deprecated in favor of the targets method.
   * DSL method that declares the jobs on which this workflow depends.
   * </p>
   * The depends method has been deprecated in favor of executes, so that workflow and job
   * dependencies can more easily visually distinguished.
   *
   * @param jobNames The list of job names on which this workflow depends
   */
  @Deprecated
  void depends(String... jobNames) {
    project.logger.lifecycle("The AzkabanWorkflow executes method is deprecated. Please use the targets method to declare that the workflow ${name} targets the jobs ${jobNames}.")
    launchJobDependencies.addAll(jobNames.toList());
  }

  /**
   * @deprecated This method has been deprecated in favor of the targets method.
   * DSL method that declares the jobs this workflow executes.
   *
   * @param jobNames The list of job names this workflow executes
   */
  @Deprecated
  void executes(String... jobNames) {
    project.logger.lifecycle("The AzkabanWorkflow executes method is deprecated. Please use the targets method to declare that the workflow ${name} targets the jobs ${jobNames}.")
    launchJobDependencies.addAll(jobNames.toList());
  }

  /**
   * DSL method that declares the target jobs for the workflow.
   *
   * @param jobNames The list of target job for the workflow
   */
  void targets(String... jobNames) {
    launchJobDependencies.addAll(jobNames.toList());
  }

  /**
   * DSL lookup method. Looks up an object in workflow scope.
   *
   * @param name The name to lookup
   * @return The object that is bound in workflow scope to the given name, or null if no such name is bound in workflow scope
   */
  Object lookup(String name) {
    return AzkabanMethods.lookup(name, workflowScope);
  }

  /**
   * DSL lookup method. Looks up an object in workflow scope and then applies the given configuration
   * closure.
   *
   * @param name The name to lookup
   * @param configure The configuration closure
   * @return The object that is bound in workflow scope to the given name, or null if no such name is bound in workflow scope
   */
  Object lookup(String name, Closure configure) {
    return AzkabanMethods.lookup(project, name, workflowScope, configure);
  }

  /**
   * DSL addJob method. Looks up the job with given name, clones it, configures the clone with the
   * given configuration closure and adds the clone to the workflow.
   *
   * @param name The job name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured job that was added to the workflow
   */
  AzkabanJob addJob(String name, Closure configure) {
    return configureJob(AzkabanMethods.cloneJob(name, workflowScope), configure);
  }

  /**
   * DSL addJob method. Looks up the job with given name, clones it, renames the clone to the
   * specified name, configures the clone with the given configuration closure and adds the clone
   * to the workflow.
   *
   * @param name The job name to lookup
   * @param rename The new name to give the cloned job
   * @param configure The configuration closure
   * @return The cloned, renamed and configured job that was added to the workflow
   */
  AzkabanJob addJob(String name, String rename, Closure configure) {
    return configureJob(AzkabanMethods.cloneJob(name, rename, workflowScope), configure);
  }

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, configures the
   * clone with the given configuration closure and adds the clone to the workflow.
   *
   * @param name The properties name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured properties object that was added to the workflow
   */
  AzkabanProperties addPropertyFile(String name, Closure configure) {
    return configureProperties(AzkabanMethods.clonePropertyFile(name, workflowScope), configure);
  }

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, renames the
   * clone to the specified name, configures the clone with the given configuration closure and
   * adds the clone to the workflow.
   *
   * @param name The properties name to lookup
   * @param rename The new name to give the cloned properties object
   * @param configure The configuration closure
   * @return The cloned, renamed and configured properties object that was added to the workflow
   */
  AzkabanProperties addPropertyFile(String name, String rename, Closure configure) {
    return configureProperties(AzkabanMethods.clonePropertyFile(name, rename, workflowScope), configure);
  }

  /**
   * DSL azkabanJob method. Creates an AzkabanJob in workflow scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  AzkabanJob azkabanJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeAzkabanJob(name), configure);
  }

  /**
   * DSL commandJob method. Creates a CommandJob in workflow scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  CommandJob commandJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeCommandJob(name), configure);
  }

  /**
   * DSL hadoopJavaJob method. Creates a HadoopJavaJob in workflow scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  HadoopJavaJob hadoopJavaJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeHadoopJavaJob(name), configure);
  }

  /**
   * DSL hiveJob method. Creates a HiveJob in workflow scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  HiveJob hiveJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeHiveJob(name), configure);
  }

  /**
   * @deprecated JavaJob has been deprecated in favor of HadoopJavaJob or JavaProcessJob.
   * DSL javaJob method. Creates a JavaJob in workflow scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @Deprecated
  JavaJob javaJob(String name, Closure configure) {
    project.logger.lifecycle("JavaJob has been deprecated in favor of HadoopJavaJob or JavaProcessJob. Please change the job ${name} to one of these classes.");
    return configureJob(azkabanFactory.makeJavaJob(name), configure);
  }

  /**
   * DSL javaProcessJob method. Creates a JavaProcessJob in workflow scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  JavaProcessJob javaProcessJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeJavaProcessJob(name), configure);
  }

  /**
   * DSL kafkaPushJob method. Creates a KafkaPushJob in workflow scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  KafkaPushJob kafkaPushJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeKafkaPushJob(name), configure);
  }

  /**
   * DSL noOpJob method. Creates a NoOpJob in workflow scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  NoOpJob noOpJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeNoOpJob(name), configure);
  }

  /**
   * DSL pigJob method. Creates a PigJob in workflow scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  PigJob pigJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makePigJob(name), configure);
  }

  /**
   * DSL voldemortBuildPushJob method. Creates a VoldemortBuildPushJob in workflow scope with the
   * given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  VoldemortBuildPushJob voldemortBuildPushJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeVoldemortBuildPushJob(name), configure);
  }

  /**
   * DSL propertyFile method. Creates an AzkabanProperties object in workflow scope with the given
   * name and configuration.
   *
   * @param name The properties name
   * @param configure The configuration closure
   * @return The new properties object
   */
  AzkabanProperties propertyFile(String name, Closure configure) {
    return configureProperties(azkabanFactory.makeAzkabanProperties(name), configure);
  }
}
