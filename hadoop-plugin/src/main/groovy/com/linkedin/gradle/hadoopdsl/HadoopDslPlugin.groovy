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

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * HadoopDslPlugin implements features for the Hadoop DSL.
 */
class HadoopDslPlugin implements Plugin<Project> {
  HadoopDslExtension extension;
  HadoopDslFactory factory;
  NamedScope globalScope = new NamedScope("");
  Project project;

  @Override
  void apply(Project project) {
    this.factory = makeFactory();
    project.extensions.add("hadoopDslFactory", factory);

    // You must have the hadoopDslFactory extension set before you can make the HadoopDslExtension
    this.extension = factory.makeExtension(project, globalScope);
    this.project = project;

    // Add the extensions that expose the DSL to users.
    project.extensions.add("hadoop", extension);
    project.extensions.add("globalScope", globalScope);

    project.extensions.add("global", this.&global);
    project.extensions.add("lookup", this.&lookup);
    project.extensions.add("propertyFile", this.&propertyFile);
    project.extensions.add("workflow", this.&workflow);

    project.extensions.add("commandJob", this.&commandJob);
    project.extensions.add("hadoopJavaJob", this.&hadoopJavaJob);
    project.extensions.add("hiveJob", this.&hiveJob);
    project.extensions.add("javaJob", this.&javaJob);
    project.extensions.add("javaProcessJob", this.&javaProcessJob);
    project.extensions.add("job", this.&job);
    project.extensions.add("kafkaPushJob", this.&kafkaPushJob);
    project.extensions.add("noOpJob", this.&noOpJob);
    project.extensions.add("pigJob", this.&pigJob);
    project.extensions.add("voldemortBuildPushJob", this.&voldemortBuildPushJob);

    // Add the Gradle task that checks and evaluates the DSL. Plugin users
    // should have their build tasks depend on this task.
    project.tasks.create("checkHadoopDsl") {
      description = "Applies the static checker to the Hadoop DSL.";
      group = "Hadoop Plugin";

      doLast {
        HadoopDslChecker checker = factory.makeChecker(project);
        checker.checkHadoopDsl(extension);

        if (checker.failedCheck()) {
          throw new Exception("Hadoop DSL static checker FAILED");
        }
        else {
          logger.lifecycle("Hadoop DSL static checker PASSED");
        }
      }
    }
  }

  /**
   * Helper method to configure a Job in the DSL. Can be called by subclasses to configure
   * custom Job subclass types.
   *
   * @param job The job to configure
   * @param configure The configuration closure
   * @return The input job, which is now configured
   */
  Job configureJob(Job job, Closure configure) {
    return Methods.configureJob(project, job, configure, globalScope);
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
    return Methods.configureProperties(project, props, configure, globalScope);
  }

  /**
   * Helper method to configure a Workflow object in the DSL. Can be called by subclasses to
   * configure custom Workflow subclass types.
   *
   * @param workflow The workflow to configure
   * @param configure The configuration closure
   * @return The input workflow, which is now configured
   */
  Workflow configureWorkflow(Workflow workflow, Closure configure) {
    return Methods.configureWorkflow(project, workflow, configure, globalScope);
  }

  /**
   * Factory method to return the Hadoop DSL Factory. Can be overridden by subclasses that wish to
   * provide their own factory.
   *
   * @return The factory to use
   */
  HadoopDslFactory makeFactory() {
    return new HadoopDslFactory();
  }

  /**
   * DSL global method. Binds the object in global scope.
   *
   * @param object The object to bind in global scope
   * @return The object, now bound in global scope
   */
  Object global(Object object) {
    return Methods.global(object, globalScope);
  }

  /**
   * DSL lookup method. Looks up an object in scope.
   *
   * @param name The name to lookup
   * @return The object that is bound in scope to the given name, or null if no such name is bound in scope
   */
  Object lookup(String name) {
    return Methods.lookup(name, globalScope);
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
    return Methods.lookup(project, name, globalScope, configure);
  }

  /**
   * DSL job method. Creates an Job in scope with the given name and configuration.
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

  /**
   * DSL workflow method. Creates a Workflow object in scope with the given name and configuration.
   *
   * @param name The workflow name
   * @param configure The configuration closure
   * @return The new workflow
   */
  Workflow workflow(String name, Closure configure) {
    return configureWorkflow(factory.makeWorkflow(name, project, globalScope), configure);
  }
}