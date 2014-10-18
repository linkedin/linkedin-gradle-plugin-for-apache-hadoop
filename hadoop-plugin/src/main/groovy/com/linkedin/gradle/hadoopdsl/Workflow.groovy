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

import org.gradle.api.Project;

/**
 * A Workflow is a collection of jobs and properties that represent a logical workflow, i.e. jobs
 * with dependencies that form a DAG.
 * <p>
 * In the DSL, a workflow can be specified with:
 * <pre>
 *   workflow('workflowName') {
 *     // Declare jobs and properties
 *     ...
 *     // Declare the job targets for the workflow
 *     targets 'jobName1', 'jobName2'
 *   }
 * </pre>
 */
class Workflow implements NamedScopeContainer {
  HadoopDslFactory factory;
  String name;
  Project project;
  List<Properties> properties;

  // We keep track of all of the jobs declared in the workflow, even if they are not transitive
  // parents of the launch job.
  List<Job> jobs;

  // The final job of the workflow (that will be used to launch the workflow). Built from the
  // launch job dependencies for the workflow.
  LaunchJob launchJob;
  Set<String> launchJobDependencies;

  // This will allow jobs to be referred to by name (e.g. when declaring dependencies). This also
  // implicitly provides scoping for job names.
  NamedScope scope;

  /**
   * Base constructor for a Workflow.
   *
   * @param name The workflow name
   * @param project The Gradle project
   */
  Workflow(String name, Project project) {
    this(name, project, null);
  }

  /**
   * Constructor for a Workflow given a parent scope.
   *
   * @param name The workflow name
   * @param project The Gradle project
   * @param parentScope The parent scope
   */
  Workflow(String name, Project project, NamedScope parentScope) {
    this.factory = project.extensions.hadoopDslFactory;
    this.jobs = new ArrayList<Job>();
    this.launchJob = factory.makeLaunchJob(name);
    this.launchJobDependencies = new LinkedHashSet<String>();
    this.name = name;
    this.project = project;
    this.properties = new ArrayList<Properties>();
    this.scope = new NamedScope(name, parentScope);
  }

  /**
   * Returns the scope at this level.
   *
   * @return The scope at this level
   */
  @Override
  public NamedScope getScope() {
    return scope;
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
  Set<Job> buildJobList() {
    Map<String, Job> jobMap = buildJobMap();
    Queue<Job> queue = new LinkedList<Job>();
    Set<Job> jobsToBuild = new LinkedHashSet<Job>();

    launchJob.dependencyNames.addAll(launchJobDependencies);
    queue.add(launchJob);

    while (!queue.isEmpty()) {
      Job job = queue.remove();

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
  Map<String, Job> buildJobMap() {
    Map<String, Job> jobMap = new HashMap<String, Job>();

    jobs.each() { Job job ->
      jobMap.put(job.name, job);
    }

    return jobMap;
  }

  /**
   * Clones the workflow.
   *
   * @return The cloned workflow
   */
  Workflow clone() {
    return clone(new Workflow(name, project, null));
  }

  /**
   * Helper method to set the properties on a cloned workflow.
   *
   * @param workflow The workflow being cloned
   * @return The cloned workflow
   */
  Workflow clone(Workflow workflow) {
    workflow.launchJob = launchJob.clone();
    workflow.launchJobDependencies.addAll(launchJobDependencies);
    workflow.scope = scope.clone();

    // Clear the scope for the cloned workflow. Then clone all the jobs
    // declared in the original workflow and use them to rebuild the scope.
    workflow.scope.thisLevel.clear();

    for (Job job : jobs) {
      Job jobClone = job.clone();
      workflow.jobs.add(jobClone);
      workflow.scope.bind(job.name, job);
    }

    return workflow;
  }

  /**
   * Helper method to configure a Job in the DSL. Can be called by subclasses to configure custom
   * Job subclass types.
   *
   * @param job The job to configure
   * @param configure The configuration closure
   * @return The input job, which is now configured
   */
  Job configureJob(Job job, Closure configure) {
    Methods.configureJob(project, job, configure, scope);
    jobs.add(job);
    return job;
  }

  /**
   * Helper method to configure a Properties object in the DSL. Can be called by subclasses to
   * configure custom Properties subclass types.
   *
   * @param props The properties to configure
   * @param configure The configuration closure
   * @return The input properties, which is now configured
   */
  Properties configureProperties(Properties props, Closure configure) {
    Methods.configureProperties(project, props, configure, scope);
    properties.add(props);
    return props;
  }

  /**
   * Returns a string representation of the workflow.
   *
   * @return A string representation of the workflow
   */
  String toString() {
    return "(Workflow: name = ${name})";
  }

  /**
   * @deprecated This method has been deprecated in favor of the targets method.
   *
   * DSL method that declares the jobs on which this workflow depends.
   * <p>
   * The depends method has been deprecated in favor of executes, so that workflow and job
   * dependencies can more easily visually distinguished.
   *
   * @param jobNames The list of job names on which this workflow depends
   */
  @Deprecated
  void depends(String... jobNames) {
    project.logger.lifecycle("The Workflow executes method is deprecated. Please use the targets method to declare that the workflow ${name} targets the jobs ${jobNames}.")
    launchJobDependencies.addAll(jobNames.toList());
  }

  /**
   * @deprecated This method has been deprecated in favor of the targets method.
   *
   * DSL method that declares the jobs this workflow executes.
   *
   * @param jobNames The list of job names this workflow executes
   */
  @Deprecated
  void executes(String... jobNames) {
    project.logger.lifecycle("The Workflow executes method is deprecated. Please use the targets method to declare that the workflow ${name} targets the jobs ${jobNames}.")
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
   * DSL lookup method. Looks up an object in scope.
   *
   * @param name The name to lookup
   * @return The object that is bound in workflow scope to the given name, or null if no such name is bound in scope
   */
  Object lookup(String name) {
    return Methods.lookup(name, scope);
  }

  /**
   * DSL lookup method. Looks up an object in scope and then applies the given configuration
   * closure.
   *
   * @param name The name to lookup
   * @param configure The configuration closure
   * @return The object that is bound in scope to the given name, or null if no such name is bound in scope
   */
  Object lookup(String name, Closure configure) {
    return Methods.lookup(project, name, scope, configure);
  }

  /**
   * DSL addJob method. Looks up the job with given name, clones it, configures the clone with the
   * given configuration closure and adds the clone to the workflow.
   *
   * @param name The job name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured job that was added to the workflow
   */
  Job addJob(String name, Closure configure) {
    return configureJob(Methods.cloneJob(name, scope), configure);
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
  Job addJob(String name, String rename, Closure configure) {
    return configureJob(Methods.cloneJob(name, rename, scope), configure);
  }

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, configures the
   * clone with the given configuration closure and adds the clone to the workflow.
   *
   * @param name The properties name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured properties object that was added to the workflow
   */
  Properties addPropertyFile(String name, Closure configure) {
    return configureProperties(Methods.clonePropertyFile(name, scope), configure);
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
  Properties addPropertyFile(String name, String rename, Closure configure) {
    return configureProperties(Methods.clonePropertyFile(name, rename, scope), configure);
  }

  /**
   * DSL job method. Creates a Job in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  Job job(String name, Closure configure) {
    return configureJob(factory.makeJob(name), configure);
  }

  /**
   * DSL commandJob method. Creates a CommandJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  CommandJob commandJob(String name, Closure configure) {
    return configureJob(factory.makeCommandJob(name), configure);
  }

  /**
   * DSL hadoopJavaJob method. Creates a HadoopJavaJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  HadoopJavaJob hadoopJavaJob(String name, Closure configure) {
    return configureJob(factory.makeHadoopJavaJob(name), configure);
  }

  /**
   * DSL hiveJob method. Creates a HiveJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  HiveJob hiveJob(String name, Closure configure) {
    return configureJob(factory.makeHiveJob(name), configure);
  }

  /**
   * @deprecated JavaJob has been deprecated in favor of HadoopJavaJob or JavaProcessJob.
   *
   * DSL javaJob method. Creates a JavaJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @Deprecated
  JavaJob javaJob(String name, Closure configure) {
    project.logger.lifecycle("JavaJob has been deprecated in favor of HadoopJavaJob or JavaProcessJob. Please change the job ${name} to one of these classes.");
    return configureJob(factory.makeJavaJob(name), configure);
  }

  /**
   * DSL javaProcessJob method. Creates a JavaProcessJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  JavaProcessJob javaProcessJob(String name, Closure configure) {
    return configureJob(factory.makeJavaProcessJob(name), configure);
  }

  /**
   * DSL kafkaPushJob method. Creates a KafkaPushJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  KafkaPushJob kafkaPushJob(String name, Closure configure) {
    return configureJob(factory.makeKafkaPushJob(name), configure);
  }

  /**
   * DSL noOpJob method. Creates a NoOpJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  NoOpJob noOpJob(String name, Closure configure) {
    return configureJob(factory.makeNoOpJob(name), configure);
  }

  /**
   * DSL pigJob method. Creates a PigJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  PigJob pigJob(String name, Closure configure) {
    return configureJob(factory.makePigJob(name), configure);
  }

  /**
   * DSL voldemortBuildPushJob method. Creates a VoldemortBuildPushJob in scope with the given name
   * and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  VoldemortBuildPushJob voldemortBuildPushJob(String name, Closure configure) {
    return configureJob(factory.makeVoldemortBuildPushJob(name), configure);
  }

  /**
   * DSL propertyFile method. Creates a Properties object in scope with the given name and
   * configuration.
   *
   * @param name The properties name
   * @param configure The configuration closure
   * @return The new properties object
   */
  Properties propertyFile(String name, Closure configure) {
    return configureProperties(factory.makeProperties(name), configure);
  }
}