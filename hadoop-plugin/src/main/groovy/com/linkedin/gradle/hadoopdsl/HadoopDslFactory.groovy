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

import com.linkedin.gradle.hadoopdsl.job.CommandJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopJavaJob;
import com.linkedin.gradle.hadoopdsl.job.HiveJob;
import com.linkedin.gradle.hadoopdsl.job.JavaJob;
import com.linkedin.gradle.hadoopdsl.job.JavaProcessJob;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.KafkaPushJob;
import com.linkedin.gradle.hadoopdsl.job.LaunchJob;
import com.linkedin.gradle.hadoopdsl.job.NoOpJob;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.hadoopdsl.job.VoldemortBuildPushJob;

import org.gradle.api.Project;

/**
 * Factory class to create instances of DSL objects. Subclasses can override these methods to
 * provide instances of their own custom subclasses of standard DSL classes.
 */
class HadoopDslFactory {
  /**
   * Factory method to build a HadoopDslExtension.
   *
   * @param project The Gradle project
   * @param scope Reference to the global scope
   * @return The HadoopDslExtension
   */
  HadoopDslExtension makeExtension(Project project, NamedScope scope) {
    return new HadoopDslExtension(project, scope);
  }

  /**
   * Factory method to build the Hadoop DSL checker.
   *
   * @param project The Gradle project
   * @return The HadoopDslChecker
   */
  HadoopDslChecker makeChecker(Project project) {
    return new HadoopDslChecker(project);
  }

  /**
   * Factory method to build a Hadoop DSL job.
   *
   * @param name The job name
   * @return The job
   */
  Job makeJob(String name) {
    return new Job(name);
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
   * Factory method to build a Properties object.
   *
   * @param name The Properties name
   * @return The Properties object
   */
  Properties makeProperties(String name) {
    return new Properties(name);
  }

  /**
   * Factory method to build a PropertySet.
   *
   * @param name The PropertySet name
   * @return The PropertySet object
   */
  PropertySet makePropertySet(String name) {
    return new PropertySet(name);
  }

  /**
   * Factory method to build a Workflow object;
   *
   * @param name The workflow name
   * @param project The Gradle project
   * @param parentScope Reference to the parent scope
   * @return The workflow
   */
  Workflow makeWorkflow(String name, Project project, NamedScope parentScope) {
    return new Workflow(name, project, parentScope);
  }
}