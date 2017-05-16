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
package com.linkedin.gradle.hadoopdsl;

import com.linkedin.gradle.hadoopdsl.job.CommandJob;
import com.linkedin.gradle.hadoopdsl.job.GobblinJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopJavaJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopShellJob;
import com.linkedin.gradle.hadoopdsl.job.HdfsToEspressoJob;
import com.linkedin.gradle.hadoopdsl.job.HdfsToTeradataJob;
import com.linkedin.gradle.hadoopdsl.job.HiveJob;
import com.linkedin.gradle.hadoopdsl.job.JavaJob;
import com.linkedin.gradle.hadoopdsl.job.JavaProcessJob;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.KafkaPushJob;
import com.linkedin.gradle.hadoopdsl.job.NoOpJob;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.hadoopdsl.job.PinotBuildAndPushJob;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;
import com.linkedin.gradle.hadoopdsl.job.SqlJob;
import com.linkedin.gradle.hadoopdsl.job.TeradataToHdfsJob;
import com.linkedin.gradle.hadoopdsl.job.VoldemortBuildPushJob;

import org.gradle.api.Project;

/**
 * Base class for DSL elements that create a new scope.
 */
@SuppressWarnings("deprecation")
abstract class BaseNamedScopeContainer implements NamedScopeContainer {
  HadoopDslFactory factory;
  NamedScope scope;
  Project project;

  // DSL elements that can be added in scope
  List<Job> jobs;
  List<Namespace> namespaces;
  List<Properties> properties;
  List<PropertySet> propertySets;
  List<Workflow> workflows;

  /**
   * Constructor for the BaseNamedScopeContainer.
   * <p>
   * This overload is intended to be called from the HadoopDslPlugin constructor, which does not
   * yet have the Gradle project available. Once the plugin's apply method is called, it will go
   * back and set the project-related properties.
   *
   * @param parentScope The parent scope
   * @param scopeName The scope name
   */
  BaseNamedScopeContainer(NamedScope parentScope, String scopeName) {
    this.factory = null;
    this.scope = new NamedScope(scopeName, parentScope);
    this.project = null;
    this.jobs = new ArrayList<Job>();
    this.namespaces = new ArrayList<Namespace>();
    this.properties = new ArrayList<Properties>();
    this.propertySets = new ArrayList<PropertySet>();
    this.workflows = new ArrayList<Workflow>();
  }

  /**
   * Constructor for the BaseNamedScopeContainer.
   *
   * @param project The Gradle project
   * @param parentScope The parent scope
   * @param scopeName The scope name
   */
  BaseNamedScopeContainer(Project project, NamedScope parentScope, String scopeName) {
    this.factory = project.extensions.hadoopDslFactory;
    this.scope = new NamedScope(scopeName, parentScope);
    this.project = project;
    this.jobs = new ArrayList<Job>();
    this.namespaces = new ArrayList<Namespace>();
    this.properties = new ArrayList<Properties>();
    this.propertySets = new ArrayList<PropertySet>();
    this.workflows = new ArrayList<Workflow>();
  }

  /**
   * Clones the scope container given its new parent scope. DSL elements that extend
   * BaseNamedScopeContainer must provide an actual clone method implementation.
   *
   * @param parentScope The new parent scope
   * @return The cloned scope container
   */
  protected abstract BaseNamedScopeContainer clone(NamedScope parentScope);

  /**
   * Helper method to set the properties on a cloned scope container.
   *
   * @param container The scope container being cloned
   * @return The cloned scope container
   */
  protected BaseNamedScopeContainer clone(BaseNamedScopeContainer container) {
    for (Job job : jobs) {
      Job clone = job.clone();
      container.jobs.add(clone);
      container.scope.bind(clone.name, clone);
    }

    for (Namespace namespace : namespaces) {
      Namespace clone = namespace.clone(container.getScope());
      container.namespaces.add(clone);
      container.scope.bind(clone.name, clone);
    }

    for (Properties props : properties) {
      Properties clone = props.clone();
      container.properties.add(clone);
      container.scope.bind(clone.name, clone);
    }

    for (PropertySet propertySet : propertySets) {
      PropertySet clone = propertySet.clone(container.getScope());
      container.propertySets.add(clone);
      container.scope.bind(clone.name, clone);
    }

    for (Workflow workflow : workflows) {
      Workflow clone = workflow.clone(container.getScope());
      container.workflows.add(clone);
      container.scope.bind(clone.name, clone);
    }

    return container;
  }

  /**
   * Helper method to clone a Job in the DSL.
   *
   * @param name The name of the object to clone
   * @return The cloned object
   */
  protected Job cloneJob(String name) {
    Job job = (Job)scope.lookup(name);
    if (job == null) {
      throw new Exception("Could not find job ${name} from scope ${scope.levelName}");
    }
    return job.clone();
  }

  /**
   * Helper method to clone a Job in the DSL and give it a new name.
   *
   * @param name The name of the object to clone
   * @param rename The new name to give the object
   * @return The cloned object
   */
  protected Job cloneJob(String name, String rename) {
    Job job = cloneJob(name);
    job.name = rename;
    return job;
  }

  /**
   * Helper method to clone a Namespace in the DSL.
   *
   * @param name The name of the object to clone
   * @return The cloned object
   */
  protected Namespace cloneNamespace(String name) {
    Namespace namespace = (Namespace)scope.lookup(name);
    if (namespace == null) {
      throw new Exception("Could not find namespace ${name} from scope ${scope.levelName}");
    }
    return namespace.clone(scope);
  }

  /**
   * Helper method to clone a Namespace in the DSL and give it a new name.
   *
   * @param name The name of the object to clone
   * @param rename The new name to give the object
   * @return The cloned object
   */
  protected Namespace cloneNamespace(String name, String rename) {
    Namespace namespace = cloneNamespace(name);
    namespace.name = rename;
    namespace.scope.levelName = rename;
    return namespace;
  }

  /**
   * Helper method to clone a Properties object in the DSL.
   *
   * @param name The name of the object to clone
   * @return The cloned object
   */
  protected Properties clonePropertyFile(String name) {
    Properties props = (Properties)scope.lookup(name);
    if (props == null) {
      throw new Exception("Could not find propertyFile ${name} from scope ${scope.levelName}");
    }
    return props.clone();
  }

  /**
   * Helper method to clone a Properties object in the DSL and give it a new name.
   *
   * @param name The name of the object to clone
   * @param rename The new name to give the object
   * @return The cloned object
   */
  protected Properties clonePropertyFile(String name, String rename) {
    Properties props = clonePropertyFile(name)
    props.name = rename;
    return props;
  }

  /**
   * Helper method to clone a PropertySet in the DSL.
   *
   * @param name The name of the object to clone
   * @return The cloned object
   */
  protected PropertySet clonePropertySet(String name) {
    PropertySet propertySet = (PropertySet)scope.lookup(name);
    if (propertySet == null) {
      throw new Exception("Could not find PropertySet ${name} from scope ${scope.levelName}");
    }
    return propertySet.clone(scope);
  }

  /**
   * Helper method to clone a PropertySet in the DSL and give it a new name.
   *
   * @param name The name of the object to clone
   * @param rename The new name to give the object
   * @return The cloned object
   */
  protected PropertySet clonePropertySet(String name, String rename) {
    PropertySet propertySet = clonePropertySet(name)
    propertySet.name = rename;
    return propertySet;
  }

  /**
   * Helper method to clone a Workflow in the DSL.
   *
   * @param name The name of the object to clone
   * @return The cloned object
   */
  protected Workflow cloneWorkflow(String name) {
    Workflow workflow = (Workflow)scope.lookup(name);
    if (workflow == null) {
      throw new Exception("Could not find workflow ${name} from scope ${scope.levelName}");
    }
    return workflow.clone(scope);
  }

  /**
   * Helper method to clone a Workflow in the DSL and give it a new name.
   *
   * @param name The name of the object to clone
   * @param rename The new name to give the object
   * @return The cloned object
   */
  protected Workflow cloneWorkflow(String name, String rename) {
    Workflow workflow = cloneWorkflow(name);
    workflow.name = rename;
    workflow.scope.levelName = rename;
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
  protected Job configureJob(Job job, Closure configure) {
    scope.bind(job.name, job);
    project.configure(job, configure);
    jobs.add(job);
    return job;
  }

  /**
   * Helper method to configure a Namespace in the DSL. Can be called by subclasses to configure
   * custom Namespace subclass types.
   *
   * @param namespace The namespace to configure
   * @param configure The configuration closure
   * @return The input namespace, which is now configured
   */
  protected Namespace configureNamespace(Namespace namespace, Closure configure) {
    scope.bind(namespace.name, namespace);
    project.configure(namespace, configure);
    namespaces.add(namespace);
    return namespace;
  }

  /**
   * Helper method to configure Properties objects in the DSL. Can be called by subclasses to
   * configure custom Properties subclass types.
   *
   * @param props The properties to configure
   * @param configure The configuration closure
   * @return The input properties, which is now configured
   */
  protected Properties configureProperties(Properties props, Closure configure) {
    scope.bind(props.name, props);
    project.configure(props, configure);
    properties.add(props);
    return props;
  }

  /**
   * Helper method to configure PropertySet objects in the DSL. Can be called by subclasses to
   * configure custom PropertySet subclass types.
   *
   * @param propertySet The PropertySet to configure
   * @param configure The configuration closure
   * @return The input PropertySet, which is now configured
   */
  protected PropertySet configurePropertySet(PropertySet propertySet, Closure configure) {
    scope.bind(propertySet.name, propertySet);
    project.configure(propertySet, configure);
    propertySets.add(propertySet);
    return propertySet;
  }

  /**
   * Helper method to configure a Workflow in the DSL. Can be called by subclasses to configure
   * custom Workflow subclass types.
   *
   * @param workflow The workflow to configure
   * @param configure The configuration closure
   * @return The input workflow, which is now configured
   */
  protected Workflow configureWorkflow(Workflow workflow, Closure configure) {
    scope.bind(workflow.name, workflow);
    project.configure(workflow, configure);
    workflows.add(workflow);
    return workflow;
  }

  /**
   * DSL addJob method. Looks up the job with given name, clones it, configures the clone with the
   * given configuration closure and adds the clone to the workflow.
   *
   * @param name The job name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured job that was added to the workflow
   */
  @HadoopDslMethod
  Job addJob(String name, @DelegatesTo(Job) Closure configure) {
    return configureJob(cloneJob(name), configure);
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
  @HadoopDslMethod
  Job addJob(String name, String rename, @DelegatesTo(Job) Closure configure) {
    return configureJob(cloneJob(name, rename), configure);
  }

  /**
   * DSL addNamespace method. Looks up the namespace with given name, clones it, configures the
   * clone with the given configuration closure and binds the clone in scope.
   *
   * @param name The namespace name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured namespace that was bound in scope
   */
  @HadoopDslMethod
  Namespace addNamespace(String name, @DelegatesTo(Namespace) Closure configure) {
    return configureNamespace(cloneNamespace(name), configure);
  }

  /**
   * DSL addNamespace method. Looks up the namespace with given name, clones it, renames the clone
   * to the specified name, configures the clone with the given configuration closure and binds the
   * clone in scope.
   *
   * @param name The namespace name to lookup
   * @param rename The new name to give the cloned namespace
   * @param configure The configuration closure
   * @return The cloned, renamed and configured namespace that was bound in scope
   */
  @HadoopDslMethod
  Namespace addNamespace(String name, String rename, @DelegatesTo(Namespace) Closure configure) {
    return configureNamespace(cloneNamespace(name, rename), configure);
  }

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, configures the
   * clone with the given configuration closure and binds the clone in scope.
   *
   * @param name The properties name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured properties object that was bound in scope
   */
  @HadoopDslMethod
  Properties addPropertyFile(String name, @DelegatesTo(Properties) Closure configure) {
    return configureProperties(clonePropertyFile(name), configure);
  }

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, renames the
   * clone to the specified name, configures the clone with the given configuration closure and
   * binds the clone in scope.
   *
   * @param name The properties name to lookup
   * @param rename The new name to give the cloned properties object
   * @param configure The configuration closure
   * @return The cloned, renamed and configured properties object that was bound in scope
   */
  @HadoopDslMethod
  Properties addPropertyFile(String name, String rename, @DelegatesTo(Properties) Closure configure) {
    return configureProperties(clonePropertyFile(name, rename), configure);
  }

  /**
   * DSL addPropertySet method. Looks up the PropertySet with given name, clones it, configures the
   * clone with the given configuration closure and binds the clone in scope.
   *
   * @param name The PropertySet name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured PropertySet that was bound in scope
   */
  @HadoopDslMethod
  PropertySet addPropertySet(String name, @DelegatesTo(PropertySet) Closure configure) {
    return configurePropertySet(clonePropertySet(name), configure);
  }

  /**
   * DSL addPropertySet method. Looks up the PropertySet with given name, clones it, renames the
   * clone to the specified name, configures the clone with the given configuration closure and
   * binds the clone in scope.
   *
   * @param name The PropertySet name to lookup
   * @param rename The new name to give the cloned PropertySet
   * @param configure The configuration closure
   * @return The cloned, renamed and configured PropertySet that was bound in scope
   */
  @HadoopDslMethod
  PropertySet addPropertySet(String name, String rename, @DelegatesTo(PropertySet) Closure configure) {
    return configurePropertySet(clonePropertySet(name, rename), configure);
  }

  /**
   * DSL addWorkflow method. Looks up the workflow with given name, clones it, configures the clone
   * with the given configuration closure and binds the clone in scope.
   *
   * @param name The workflow name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured workflow that was bound in scope
   */
  @HadoopDslMethod
  Workflow addWorkflow(String name, @DelegatesTo(Workflow) Closure configure) {
    return configureWorkflow(cloneWorkflow(name), configure);
  }

  /**
   * DSL addWorkflow method. Looks up the workflow with given name, clones it, renames the clone to
   * the specified name, configures the clone with the given configuration closure and binds the
   * clone in scope.
   *
   * @param name The workflow name to lookup
   * @param rename The new name to give the cloned workflow
   * @param configure The configuration closure
   * @return The cloned, renamed and configured workflow that was bound in scope
   */
  @HadoopDslMethod
  Workflow addWorkflow(String name, String rename, @DelegatesTo(Workflow) Closure configure) {
    return configureWorkflow(cloneWorkflow(name, rename), configure);
  }

  /**
   * DSL evalHadoopClosure method. Evaluates the specified hadoopClosure against the specified
   * definition set and target.
   *
   * @param args A map whose required key "name" specifies the named hadoopClosure to evaluate
   *             and optional key "defs" specifies the definition set name to use as the current definition set before evaluating the closure
   *             and whose optional key "targetName" specifies the name of the Hadoop DSL object to set as the closure delegate before evaluating the closure.
   *             If the definition set is not specified, the default definition set is used, and if the target name is not specified, this object is used as the specified delegate target.
   */
  @HadoopDslMethod
  void evalHadoopClosure(Map args) {
    String closureName = args.name;
    String definitionSetName = args.containsKey("defs") ? args.defs : "default";
    String targetName = args.containsKey("targetName") ? args.targetName : null;
    Object target = (targetName != null) ? lookup(targetName) : this;
    project.extensions.hadoopDslPlugin.evalHadoopClosure(closureName, definitionSetName, target);
  }

  /**
   * DSL evalHadoopClosures method. Evaluates all the anonymous hadoopClosure closures against the
   * default definition set and using this object as the specified delegate target.
   */
  @HadoopDslMethod
  void evalHadoopClosures() {
    evalHadoopClosures("default");
  }

  /**
   * DSL evalHadoopClosures method. Evaluates all the anonymous hadoopClosure closures against the
   * specified definition set and using this object as the specified delegate target.
   *
   * @param definitionSetName The definition set name to use as the current definition set before evaluating the closures
   */
  @HadoopDslMethod
  void evalHadoopClosures(String definitionSetName) {
    project.extensions.hadoopDslPlugin.evalHadoopClosures(definitionSetName, this);
  }

  /**
   * DSL evalHadoopClosures method. Evaluates all the anonymous hadoopClosure closures against the
   * specified definition set and target.
   *
   * @param args A map whose optional key "defs" specifies the definition set name to use as the current definition set before evaluating the closures
   *             and whose optional key "targetName" specifies the name of the Hadoop DSL object to set as the closure delegate before evaluating the closure.
   *             If the definition set is not specified, the default definition set is used, and if the target name is not specified, this object is used as the specified delegate target.
   */
  @HadoopDslMethod
  void evalHadoopClosures(Map args) {
    String definitionSetName = args.containsKey("defs") ? args.defs : "default";
    String targetName = args.containsKey("targetName") ? args.targetName : null;
    Object target = (targetName != null) ? lookup(targetName) : this;
    project.extensions.hadoopDslPlugin.evalHadoopClosures(definitionSetName, target);
  }

  /**
   * Returns the scope at this level.
   *
   * @return The scope at this level
   */
  @Override
  NamedScope getScope() {
    return scope;
  }

  /**
   * DSL lookup method. Looks up an object in scope.
   *
   * @param name The name to lookup
   * @return The object that is bound in scope to the given name
   * @throws Exception If the given name is not bound in scope
   */
  @HadoopDslMethod
  Object lookup(String name) {
    Object entry = scope.lookup(name);
    if (entry == null) {
      throw new Exception("The name ${name} is not bound from the scope ${scope.levelName}")
    }
    return entry;
  }

  /**
   * DSL lookup method. Looks up an object in scope and then applies the given configuration
   * closure.
   *
   * @param name The name to lookup
   * @param configure The configuration closure
   * @return The object that is bound in scope to the given name, with the configuration applied
   * @throws Exception If the given name is not bound in scope
   */
  @HadoopDslMethod
  Object lookup(String name, Closure configure) {
    Object boundObject = lookup(name);
    project.configure(boundObject, configure);
    return boundObject;
  }

  /**
   * DSL lookupRef method. Looks up the scope binding reference for an object.
   *
   * @param name The name to lookup
   * @return The NamedScopeReference for the binding, or null if the given name is not bound in scope
   */
  @HadoopDslMethod
  NamedScopeReference lookupRef(String name) {
    return scope.lookupRef(name);
  }

  /**
   * DSL namespace method. Creates a Namespace in scope with the given name and configuration.
   * <p>
   * For ease of organizing the user's Gradle scripts, namespaces can be redeclared at the same
   * scope level. If the namespace already exists, it will simply be configured with the given
   * closure configuration.
   *
   * @param name The namespace name
   * @param configure The configuration closure
   * @return The namespace
   */
  @HadoopDslMethod
  Namespace namespace(String name, @DelegatesTo(Namespace) Closure configure) {
    // Check if the namespace is in scope at this level
    Object boundObject = scope.thisLevel.get(name);
    if (boundObject == null) {
      return configureNamespace(factory.makeNamespace(name, project, scope), configure);
    }
    if (boundObject instanceof Namespace) {
      Namespace namespace = (Namespace)boundObject;
      project.configure(namespace, configure);
      return namespace;
    }
    throw new Exception("An object with the name ${name} is already declared in the scope ${scope.levelName}");
  }

  /**
   * DSL propertyFile method. Creates a Properties object in scope with the given name and
   * configuration.
   *
   * @param name The properties name
   * @param configure The configuration closure
   * @return The new properties object
   */
  @HadoopDslMethod
  Properties propertyFile(String name, @DelegatesTo(Properties) Closure configure) {
    return configureProperties(factory.makeProperties(name), configure);
  }

  /**
   * DSL propertySet method. Creates a PropertySet object in scope with the given name and
   * configuration.
   *
   * @param name The PropertySet name
   * @param configure The configuration closure
   * @return The new PropertySet object
   */
  @HadoopDslMethod
  PropertySet propertySet(String name, @DelegatesTo(PropertySet) Closure configure) {
    return configurePropertySet(factory.makePropertySet(name, scope), configure);
  }

  /**
   * DSL workflow method. Creates a Workflow in scope with the given name and configuration.
   *
   * @param name The workflow name
   * @param configure The configuration closure
   * @return The new workflow
   */
  @HadoopDslMethod
  Workflow workflow(String name, @DelegatesTo(Workflow) Closure configure) {
    return configureWorkflow(factory.makeWorkflow(name, project, scope), configure);
  }

  /**
   * DSL job method. Creates an Job in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  Job job(String name, @DelegatesTo(Job) Closure configure) {
    return configureJob(factory.makeJob(name), configure);
  }

  /**
   * DSL commandJob method. Creates a CommandJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  CommandJob commandJob(String name, @DelegatesTo(CommandJob) Closure configure) {
    return ((CommandJob)configureJob(factory.makeCommandJob(name), configure));
  }

  /**
   * DSL hadoopJavaJob method. Creates a HadoopJavaJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  HadoopJavaJob hadoopJavaJob(String name, @DelegatesTo(HadoopJavaJob) Closure configure) {
    return ((HadoopJavaJob)configureJob(factory.makeHadoopJavaJob(name), configure));
  }

  /**
   * DSL hadoopShellJob method. Creates a HadoopShellJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure.
   * @return The new job
   */
  @HadoopDslMethod
  HadoopShellJob hadoopShellJob(String name, @DelegatesTo(HadoopShellJob) Closure configure) {
    return ((HadoopShellJob)configureJob(factory.makeHadoopShellJob(name), configure));
  }

  /**
   * DSL hiveJob method. Creates a HiveJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  HiveJob hiveJob(String name, @DelegatesTo(HiveJob) Closure configure) {
    return ((HiveJob)configureJob(factory.makeHiveJob(name), configure));
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
  @HadoopDslMethod
  JavaJob javaJob(String name, @DelegatesTo(JavaJob) Closure configure) {
    project.logger.lifecycle("JavaJob has been deprecated in favor of HadoopJavaJob or JavaProcessJob. Please change the job ${name} to one of these classes.");
    return ((JavaJob)configureJob(factory.makeJavaJob(name), configure));
  }

  /**
   * DSL javaProcessJob method. Creates a JavaProcessJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  JavaProcessJob javaProcessJob(String name, @DelegatesTo(JavaProcessJob) Closure configure) {
    return ((JavaProcessJob)configureJob(factory.makeJavaProcessJob(name), configure));
  }

  /**
   * DSL kafkaPushJob method. Creates a KafkaPushJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  KafkaPushJob kafkaPushJob(String name, @DelegatesTo(KafkaPushJob) Closure configure) {
    return ((KafkaPushJob)configureJob(factory.makeKafkaPushJob(name), configure));
  }

  /**
   * DSL noOpJob method. Creates a NoOpJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  NoOpJob noOpJob(String name, @DelegatesTo(NoOpJob) Closure configure) {
    return ((NoOpJob)configureJob(factory.makeNoOpJob(name), configure));
  }

  /**
   * DSL pigJob method. Creates a PigJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  PigJob pigJob(String name, @DelegatesTo(PigJob) Closure configure) {
    return ((PigJob)configureJob(factory.makePigJob(name), configure));
  }

  /**
   * DSL pinotBuildPushJob method. Creates a pinotBuildPushJob in scope with the given name
   * and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  PinotBuildAndPushJob pinotBuildAndPushJob(String name, @DelegatesTo(PinotBuildAndPushJob) Closure configure) {
    return ((PinotBuildAndPushJob)configureJob(factory.makePinotBuildAndPushJob(name), configure));
  }

  /**
   * DSL sparkJob method. Creates a SparkJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  SparkJob sparkJob(String name, @DelegatesTo(SparkJob) Closure configure) {
    return ((SparkJob)configureJob(factory.makeSparkJob(name), configure));
  }

  /**
   * DSL voldemortBuildPushJob method. Creates a VoldemortBuildPushJob in scope with the given name
   * and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  VoldemortBuildPushJob voldemortBuildPushJob(String name, @DelegatesTo(VoldemortBuildPushJob) Closure configure) {
    return ((VoldemortBuildPushJob)configureJob(factory.makeVoldemortBuildPushJob(name), configure));
  }

  /**
   * DSL hdfsToTeradataJob method. Creates a HdfsToTeradataJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  HdfsToTeradataJob hdfsToTeradataJob(String name, @DelegatesTo(HdfsToTeradataJob) Closure configure) {
    return ((HdfsToTeradataJob)configureJob(factory.makeHdfsToTeradataJob(name), configure));
  }

  /**
   * DSL teradataToHdfsJob method. Creates a TeradataToHdfsJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  TeradataToHdfsJob teradataToHdfsJob(String name, @DelegatesTo(TeradataToHdfsJob) Closure configure) {
    return ((TeradataToHdfsJob)configureJob(factory.makeTeradataToHdfsJob(name), configure));
  }

  /**
   * DSL hdfsToEspressoJob method. Creates a HdfsToEspressoJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  HdfsToEspressoJob hdfsToEspressoJob(String name, @DelegatesTo(HdfsToEspressoJob) Closure configure) {
    return ((HdfsToEspressoJob)configureJob(factory.makeHdfsToEspressoJob(name), configure));
  }

  /**
   * DSL gobblinJob method. Creates a GobblinJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  GobblinJob gobblinJob(String name, @DelegatesTo(GobblinJob) Closure configure) {
    return ((GobblinJob)configureJob(factory.makeGobblinJob(name), configure));
  }

  /**
   * DSL sqlJob method. Creates a SqlJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  SqlJob sqlJob(String name, @DelegatesTo(SqlJob) Closure configure) {
    return ((SqlJob)configureJob(factory.makeSqlJob(name), configure));
  }
}
