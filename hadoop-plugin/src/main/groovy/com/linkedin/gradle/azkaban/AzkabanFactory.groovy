package com.linkedin.gradle.azkaban;

import org.gradle.api.Project;

/**
 * Factory class that makes it easy to provide custom subclasses of the standard DSL classes.
 */
class AzkabanFactory {

  // Factory method to return the Azkaban extension that can be overridden by subclasses.
  AzkabanExtension makeAzkabanExtension(Project project, NamedScope globalScope) {
    return new AzkabanExtension(project, globalScope);
  }

  // Factory method to return the Azkaban DSL checker that can be overridden by subclasses.
  AzkabanChecker makeAzkabanChecker() {
    return new AzkabanChecker();
  }

  // Factory method to build AzkabanJob that can be overridden by subclasses.
  AzkabanJob makeAzkabanJob(String name) {
    return new AzkabanJob(name);
  }

  // Factory method to build CommandJob that can be overridden by subclasses.
  CommandJob makeCommandJob(String name) {
    return new CommandJob(name);
  }

  // Factory method to build HadoopJavaJob that can be overridden by subclasses.
  HadoopJavaJob makeHadoopJavaJob(String name) {
    return new HadoopJavaJob(name);
  }

  // Factory method to build HiveJob that can be overridden by subclasses.
  HiveJob makeHiveJob(String name) {
    return new HiveJob(name);
  }

  // Factory method to build JavaJob that can be overridden by subclasses.
  JavaJob makeJavaJob(String name) {
    return new JavaJob(name);
  }

  // Factory method to build JavaProcessJob that can be overridden by subclasses.
  JavaProcessJob makeJavaProcessJob(String name) {
    return new JavaProcessJob(name);
  }

  // Factory method to build KafkaPushJob that can be overridden by subclasses.
  KafkaPushJob makeKafkaPushJob(String name) {
    return new KafkaPushJob(name);
  }

  // Factory method to build LaunchJob that can be overridden by subclasses.
  LaunchJob makeLaunchJob(String name) {
    return new LaunchJob(name);
  }

  // Factory method to build NoopJob that can be overridden by subclasses.
  NoOpJob makeNoOpJob(String name) {
    return new NoOpJob(name);
  }

  // Factory method to build PigJob that can be overridden by subclasses.
  PigJob makePigJob(String name) {
    return new PigJob(name);
  }

  // Factory method to build VoldemortBuildPushJob that can be overridden by subclasses.
  VoldemortBuildPushJob makeVoldemortBuildPushJob(String name) {
    return new VoldemortBuildPushJob(name);
  }

  // Factory method to build AzkabanProperties that can be overridden by subclasses.
  AzkabanProperties makeAzkabanProperties(String name) {
    return new AzkabanProperties(name);
  }

  // Factory method to build AzkabanWorkflow that can be overridden by subclasses.
  AzkabanWorkflow makeAzkabanWorkflow(String name, Project project, NamedScope nextLevel) {
    return new AzkabanWorkflow(name, project, nextLevel);
  }
}