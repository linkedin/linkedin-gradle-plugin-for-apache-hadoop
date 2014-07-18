package com.linkedin.gradle.azkaban;

import org.gradle.api.Project;

/**
 * Static helper methods used to implement the Azkaban DSL.
 *
 * People extending the Azkaban DSL by subclassing should generally not need to call methods in
 * this class, as they should already be called appropriately by AzkabanPlugin, AzkabanExtension
 * and AzkabanWorkflow, but you can call them if you need to do so.
 */
class AzkabanMethods {

  static AzkabanJob cloneJob(String name, NamedScope scope) {
    AzkabanJob job = scope.lookup(name);
    if (job == null) {
      throw new Exception("Could not find job ${name} from scope ${scope.levelName}");
    }
    return job.clone();
  }

  static AzkabanJob cloneJob(String name, String rename, NamedScope scope) {
    AzkabanJob job = cloneJob(name, scope);
    job.name = rename;
    return job;
  }

  static AzkabanProperties clonePropertyFile(String name, NamedScope scope) {
    AzkabanProperties props = scope.lookup(name);
    if (props == null) {
      throw new Exception("Could not find propertyFile ${name} from scope ${scope.levelName}");
    }
    return props.clone();
  }

  static AzkabanProperties clonePropertyFile(String name, String rename, NamedScope scope) {
    AzkabanProperties props = clonePropertyFile(name, scope)
    props.name = rename;
    return props;
  }

  static AzkabanWorkflow cloneWorkflow(String name, NamedScope scope) {
    AzkabanWorkflow workflow = scope.lookup(name);
    if (workflow == null) {
      throw new Exception("Could not find workflow ${name} from scope ${scope.levelName}");
    }
    AzkabanWorkflow clone = workflow.clone();
    clone.workflowScope.nextLevel = scope;
    return clone;
  }

  static AzkabanWorkflow cloneWorkflow(String name, String rename, NamedScope scope) {
    AzkabanWorkflow workflow = cloneWorkflow(name, scope);
    workflow.name = rename;
    return workflow;
  }

  static AzkabanJob configureJob(Project project, AzkabanJob job, Closure configure, NamedScope scope) {
    scope.bind(job.name, job);
    project.configure(job, configure);
    return job;
  }

  static AzkabanProperties configureProperties(Project project, AzkabanProperties props, Closure configure, NamedScope scope) {
    scope.bind(props.name, props);
    project.configure(props, configure);
    return props;
  }

  static AzkabanWorkflow configureWorkflow(Project project, AzkabanWorkflow workflow, Closure configure, NamedScope scope) {
    scope.bind(workflow.name, workflow);
    project.configure(workflow, configure);
    return workflow;
  }

  static Object global(Object object, NamedScope scope) {
    if (scope.contains(object.name)) {
      throw new Exception("An object with name ${object.name} requested to be global is already bound in scope ${scope.levelName}");
    }
    scope.bind(object.name, object);
    return object;
  }

  static Object lookup(String name, NamedScope scope) {
    return scope.lookup(name);
  }

  static Object lookup(Project project, String name, NamedScope scope, Closure configure) {
    Object boundObject = lookup(name, scope);
    if (boundObject == null) {
      return null;
    }
    project.configure(boundObject, configure);
    return boundObject;
  }
}