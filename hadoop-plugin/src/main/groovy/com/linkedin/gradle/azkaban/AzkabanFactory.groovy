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
 * Factory class to create instances of DSL objects. Subclasses can override these methods to
 * provide instances of their own custom subclasses of standard DSL classes.
 */
class AzkabanFactory {
  /**
   * Factory method to build an AzkabanExtension.
   *
   * @param project The Gradle project
   * @param globalScope Reference to the global scope
   * @return The AzkabanExtension
   */
  AzkabanExtension makeAzkabanExtension(Project project, NamedScope globalScope) {
    return new AzkabanExtension(project, globalScope);
  }

  /**
   * Factory method to build the Azkaban DSL checker.
   *
   * @return The AzkabanChecker
   */
  AzkabanChecker makeAzkabanChecker() {
    return new AzkabanChecker();
  }

  /**
   * Factory method to build an AzkabanJob.
   *
   * @param name The job name
   * @return The job
   */
  AzkabanJob makeAzkabanJob(String name) {
    return new AzkabanJob(name);
  }

  /**
   * Factory method to build a CommandJob.
   *
   * @param name The job name
   * @return The job
   */
  CommandJob makeCommandJob(String name) {
    return new CommandJob(name);
  }

  /**
   * Factory method to build a HadoopJavaJob.
   *
   * @param name The job name
   * @return The job
   */
  HadoopJavaJob makeHadoopJavaJob(String name) {
    return new HadoopJavaJob(name);
  }

  /**
   * Factory method to build a HiveJob.
   *
   * @param name The job name
   * @return The job
   */
  HiveJob makeHiveJob(String name) {
    return new HiveJob(name);
  }

  /**
   * @deprecated JavaJob has been deprecated in favor of HadoopJavaJob or JavaProcessJob.
   * Factory method to build a JavaJob.
   *
   * @param name The job name
   * @return The job
   */
  @Deprecated
  JavaJob makeJavaJob(String name) {
    return new JavaJob(name);
  }

  /**
   * Factory method to build JavaProcessJob.
   *
   * @param name The job name
   * @return The job
   */
  JavaProcessJob makeJavaProcessJob(String name) {
    return new JavaProcessJob(name);
  }

  /**
   * Factory method to build a KafkaPushJob.
   *
   * @param name The job name
   * @return The job
   */
  KafkaPushJob makeKafkaPushJob(String name) {
    return new KafkaPushJob(name);
  }

  /**
   * Factory method to build a LaunchJob.
   *
   * @param name The job name
   * @return The job
   */
  LaunchJob makeLaunchJob(String name) {
    return new LaunchJob(name);
  }

  /**
   * Factory method to build a NoOpJob.
   *
   * @param name The job name
   * @return The job
   */
  NoOpJob makeNoOpJob(String name) {
    return new NoOpJob(name);
  }

  /**
   * Factory method to build a PigJob.
   *
   * @param name The job name
   * @return The job
   */
  PigJob makePigJob(String name) {
    return new PigJob(name);
  }

  /**
   * Factory method to build a VoldemortBuildPushJob
   *
   * @param name The job name
   * @return The job
   */
  VoldemortBuildPushJob makeVoldemortBuildPushJob(String name) {
    return new VoldemortBuildPushJob(name);
  }

  /**
   * Factory method to build an AzkabanProperties object.
   *
   * @param name The properties name
   * @return The properties object
   */
  AzkabanProperties makeAzkabanProperties(String name) {
    return new AzkabanProperties(name);
  }

  /**
   * Factory method to build an AzkabanWorkflow.
   *
   * @param name The workflow name
   * @param project The Gradle project
   * @param nextLevel Reference to the parent scope
   * @return The workflow
   */
  AzkabanWorkflow makeAzkabanWorkflow(String name, Project project, NamedScope nextLevel) {
    return new AzkabanWorkflow(name, project, nextLevel);
  }
}
