package com.linkedin.gradle.azkaban;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

class AzkabanPlugin implements Plugin<Project> {
  AzkabanExtension azkabanExtension;
  AzkabanFactory azkabanFactory;
  NamedScope globalScope = new NamedScope("");
  Project project;

  @Override
  public void apply(Project project) {
    this.azkabanFactory = makeAzkabanFactory();
    project.extensions.add("azkabanFactory", azkabanFactory);

    this.azkabanExtension = azkabanFactory.makeAzkabanExtension(project, globalScope);
    this.project = project;

    // Add the extensions that expose the DSL to users.
    project.extensions.add("azkaban", azkabanExtension);
    project.extensions.add("globalScope", globalScope);
    project.extensions.add("global", this.&global);
    project.extensions.add("lookup", this.&lookup);
    project.extensions.add("propertyFile", this.&propertyFile);
    project.extensions.add("workflow", this.&workflow);

    project.extensions.add("azkabanJob", this.&azkabanJob);
    project.extensions.add("commandJob", this.&commandJob);
    project.extensions.add("hadoopJavaJob", this.&hadoopJavaJob);
    project.extensions.add("hiveJob", this.&hiveJob);
    project.extensions.add("javaJob", this.&javaJob);
    project.extensions.add("javaProcessJob", this.&javaProcessJob);
    project.extensions.add("kafkaPushJob", this.&kafkaPushJob);
    project.extensions.add("noOpJob", this.&noOpJob);
    project.extensions.add("pigJob", this.&pigJob);
    project.extensions.add("voldemortBuildPushJob", this.&voldemortBuildPushJob);

    // Add the Gradle task that checks and evaluates the DSL. Plugin users
    // should have their build tasks depend on this task.
    project.tasks.create("buildAzkabanFlows") {
      description = "Builds Azkaban job files from the Azkaban DSL. Have your build task depend on this task.";
      group = "Hadoop Plugin";

      doLast {
        AzkabanChecker checker = azkabanFactory.makeAzkabanChecker();
        if (!checker.checkAzkabanExtension(azkabanExtension)) {
          throw new Exception("AzkabanDslChecker FAILED");
        }

        logger.lifecycle("AzkabanDslChecker PASSED");
        azkabanExtension.build();
      }
    }
  }

  // Helper method to configure AzkabanJob in the DSL. Can be called by subclasses to configure
  // custom AzkabanJob subclass types.
  AzkabanJob configureJob(AzkabanJob job, Closure configure) {
    return AzkabanMethods.configureJob(project, job, configure, globalScope);
  }

  // Helper method to configure AzkabanProperties in the DSL. Can be called by subclasses to
  // configure custom AzkabanProperties subclass types.
  AzkabanProperties configureProperties(AzkabanProperties props, Closure configure) {
    return AzkabanMethods.configureProperties(project, props, configure, globalScope);
  }

  // Helper method to configure AzkabanWorkflow in the DSL. Can be called by subclasses to
  // configure custom AzkabanWorkflow subclass types.
  AzkabanWorkflow configureWorkflow(AzkabanWorkflow workflow, Closure configure) {
    return AzkabanMethods.configureWorkflow(project, workflow, configure, globalScope);
  }

  // Factory method to return the AzkabanFactory that can be overridden by subclasses.
  AzkabanFactory makeAzkabanFactory() {
    return new AzkabanFactory();
  }

  Object global(Object object) {
    return AzkabanMethods.global(object, globalScope);
  }

  Object lookup(String name) {
    return AzkabanMethods.lookup(name, globalScope);
  }

  Object lookup(String name, Closure configure) {
    return AzkabanMethods.lookup(project, name, globalScope, configure);
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

  AzkabanWorkflow workflow(String name, Closure configure) {
    return configureWorkflow(azkabanFactory.makeAzkabanWorkflow(name, project, globalScope), configure);
  }
}