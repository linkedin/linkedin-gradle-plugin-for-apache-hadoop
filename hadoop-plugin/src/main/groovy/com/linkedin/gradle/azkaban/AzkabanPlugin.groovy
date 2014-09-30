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

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * AzkabanPlugin implements features for Azkaban.
 */
class AzkabanPlugin implements Plugin<Project> {
  AzkabanExtension azkabanExtension;
  AzkabanFactory azkabanFactory;
  NamedScope globalScope = new NamedScope("");
  Project project;

  @Override
  void apply(Project project) {
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

  /**
   * Helper method to configure AzkabanJob in the DSL. Can be called by subclasses to configure
   * custom AzkabanJob subclass types.
   *
   * @param job The job to configure
   * @param configure The configuration closure
   * @return The input job, which is now configured
   */
  AzkabanJob configureJob(AzkabanJob job, Closure configure) {
    return AzkabanMethods.configureJob(project, job, configure, globalScope);
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
    return AzkabanMethods.configureProperties(project, props, configure, globalScope);
  }

  /**
   * Helper method to configure AzkabanWorkflow in the DSL. Can be called by subclasses to
   * configure custom AzkabanWorkflow subclass types.
   *
   * @param workflow The workflow to configure
   * @param configure The configuration closure
   * @return The input workflow, which is now configured
   */
  AzkabanWorkflow configureWorkflow(AzkabanWorkflow workflow, Closure configure) {
    return AzkabanMethods.configureWorkflow(project, workflow, configure, globalScope);
  }

  /**
   * Factory method to return the AzkabanFactory. Can be overridden by subclasses that wish to
   * provide their own AzkabanFactory.
   *
   * @return The AzkabanFactory to use
   */
  AzkabanFactory makeAzkabanFactory() {
    return new AzkabanFactory();
  }

  /**
   * DSL global method. Binds the object in global scope.
   *
   * @param object The object to bind in global scope
   * @return The object, now bound in global scope
   */
  Object global(Object object) {
    return AzkabanMethods.global(object, globalScope);
  }

  /**
   * DSL lookup method. Looks up an object in global scope.
   *
   * @param name The name to lookup
   * @return The object that is bound in global scope to the given name, or null if no such name is
   *         bound in global scope.
   */
  Object lookup(String name) {
    return AzkabanMethods.lookup(name, globalScope);
  }

  /**
   * DSL lookup method. Looks up an object in global scope and then applies the given configuration
   * closure.
   *
   * @param name The name to lookup
   * @param configure The configuration closure
   * @return The object that is bound in global scope to the given name, or null if no such name is
   *         bound in global scope.
   */
  Object lookup(String name, Closure configure) {
    return AzkabanMethods.lookup(project, name, globalScope, configure);
  }

  /**
   * DSL azkabanJob method. Creates an AzkabanJob in global scope with the given name and
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
   * DSL commandJob method. Creates a CommandJob in global scope with the given name and
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
   * DSL hadoopJavaJob method. Creates a HadoopJavaJob in global scope with the given name and
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
   * DSL hiveJob method. Creates a HiveJob in global scope with the given name and configuration.
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
   * DSL javaJob method. Creates a JavaJob in global scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @Deprecated
  JavaJob javaJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeJavaJob(name), configure);
  }

  /**
   * DSL javaProcessJob method. Creates a JavaProcessJob in global scope with the given name and
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
   * DSL kafkaPushJob method. Creates a KafkaPushJob in global scope with the given name and
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
   * DSL noOpJob method. Creates a NoOpJob in global scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  NoOpJob noOpJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makeNoOpJob(name), configure);
  }

  /**
   * DSL pigJob method. Creates a PigJob in global scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  PigJob pigJob(String name, Closure configure) {
    return configureJob(azkabanFactory.makePigJob(name), configure);
  }

  /**
   * DSL voldemortBuildPushJob method. Creates a VoldemortBuildPushJob in global scope with the
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
   * DSL propertyFile method. Creates an AzkabanProperties object in global scope with the given
   * name and configuration.
   *
   * @param name The properties name
   * @param configure The configuration closure
   * @return The new properties object
   */
  AzkabanProperties propertyFile(String name, Closure configure) {
    return configureProperties(azkabanFactory.makeAzkabanProperties(name), configure);
  }

  /**
   * DSL workflow method. Creates an AzkabanWorkflow in global scope with the given name and
   * configuration.
   *
   * @param name The workflow name
   * @param configure The configuration closure
   * @return The new workflow
   */
  AzkabanWorkflow workflow(String name, Closure configure) {
    return configureWorkflow(azkabanFactory.makeAzkabanWorkflow(name, project, globalScope), configure);
  }
}
