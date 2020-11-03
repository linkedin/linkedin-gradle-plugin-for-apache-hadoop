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
package com.linkedin.gradle.hadoopdsl


import com.linkedin.gradle.hadoopdsl.job.CarbonJob;
import com.linkedin.gradle.hadoopdsl.job.CommandJob;
import com.linkedin.gradle.hadoopdsl.job.GobblinJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopJavaJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopShellJob;
import com.linkedin.gradle.hadoopdsl.job.HdfsToEspressoJob;
import com.linkedin.gradle.hadoopdsl.job.HdfsToTeradataJob;
import com.linkedin.gradle.hadoopdsl.job.HdfsWaitJob;
import com.linkedin.gradle.hadoopdsl.job.HiveJob;
import com.linkedin.gradle.hadoopdsl.job.JavaJob;
import com.linkedin.gradle.hadoopdsl.job.JavaProcessJob;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.KabootarJob;
import com.linkedin.gradle.hadoopdsl.job.KafkaPushJob;
import com.linkedin.gradle.hadoopdsl.job.KubernetesJob;
import com.linkedin.gradle.hadoopdsl.job.LaunchJob;
import com.linkedin.gradle.hadoopdsl.job.NoOpJob;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.hadoopdsl.job.PinotBuildAndPushJob;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;
import com.linkedin.gradle.hadoopdsl.job.SqlJob;
import com.linkedin.gradle.hadoopdsl.job.StartJob;
import com.linkedin.gradle.hadoopdsl.job.SubFlowJob;
import com.linkedin.gradle.hadoopdsl.job.TableauJob;
import com.linkedin.gradle.hadoopdsl.job.TensorFlowJob
import com.linkedin.gradle.hadoopdsl.job.TensorFlowSparkJob
import com.linkedin.gradle.hadoopdsl.job.TeradataToHdfsJob
import com.linkedin.gradle.hadoopdsl.job.TonyJob;
import com.linkedin.gradle.hadoopdsl.job.VenicePushJob;
import com.linkedin.gradle.hadoopdsl.job.VoldemortBuildPushJob;
import com.linkedin.gradle.hadoopdsl.job.WormholePushJob;
import com.linkedin.gradle.hadoopdsl.job.WormholePushJob2;
import com.linkedin.gradle.test.AssertionWorkflowExtension;
import com.linkedin.gradle.test.TestExtension;

import org.gradle.api.Project;

/**
 * Factory class to create instances of DSL objects. Subclasses can override these methods to
 * provide instances of their own custom subclasses of standard DSL classes.
 */
@SuppressWarnings("deprecation")
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
   * Factory method to build a HadoopShellJob
   *
   * @param name The job name
   * @return The job
   */
  HadoopShellJob makeHadoopShellJob(String name) {
    return new HadoopShellJob(name);
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
   * Factory method to build a SubFlowJob.
   *
   * @param name The job name
   * @return The job
   */
  SubFlowJob makeSubFlowJob(String name) {
    return new SubFlowJob(name);
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
   * Factory method to build a PinotBuildAndPushJob.
   *
   * @param name The job name
   * @return The job
   */
  PinotBuildAndPushJob makePinotBuildAndPushJob(String name) {
    return new PinotBuildAndPushJob(name);
  }

  /**
   * Factory method to build a SparkJob
   *
   * @param name The job name
   * @return The job
   */
  SparkJob makeSparkJob(String name) {
    return new SparkJob(name);
  }
  /**
   * Factory method to build a StartJob.
   *
   * @param name The job name
   * @return The job
   */
  StartJob makeStartJob(String name) {
    return new StartJob(name);
  }

  /**
   * Factory method to build a VenicePushJob
   *
   * @param name The job name
   * @return The job
   */
  VenicePushJob makeVenicePushJob(String name) {
    return new VenicePushJob(name);
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
   * Factory method to build a HdfsToTeradataJob
   *
   * @param name The job name
   * @return The job
   */
  HdfsToTeradataJob makeHdfsToTeradataJob(String name) {
    return new HdfsToTeradataJob(name);
  }

  /**
   * Factory method to build a TableauJob
   *
   * @param name The job name
   * @return The job
   */
  TableauJob makeTableauJob(String name) {
    return new TableauJob(name);
  }

  /**
   * Factory method to build a TeradataToHdfsJob
   *
   * @param name The job name
   * @return The job
   */
  TeradataToHdfsJob makeTeradataToHdfsJob(String name) {
    return new TeradataToHdfsJob(name);
  }

  /**
   * Factory method to build a HdfsToEspressoJob
   *
   * @param name The job name
   * @return The job
   */
  HdfsToEspressoJob makeHdfsToEspressoJob(String name) {
    return new HdfsToEspressoJob(name);
  }

  /**
   * Factory method to build a GobblinJob
   *
   * @param name The job name
   * @return The job
   */
  GobblinJob makeGobblinJob(String name) {
    return new GobblinJob(name);
  }

  /**
   * Factory method to build a SqlJob
   *
   * @param name The job name
   * @return The job
   */
  SqlJob makeSqlJob(String name) {
    return new SqlJob(name);
  }

  /**
   * Factory method to build an HdfsWaitJob
   *
   * @param name The job name
   * @return The job
   */
  HdfsWaitJob makeHdfsWaitJob(String name) {
    return new HdfsWaitJob(name);
  }

  /**
   * Factory method to build a TensorFlowSparkJob
   *
   * @param name The job name
   * @return The job
   */
  TensorFlowJob makeTensorFlowSparkJob(String name) {
    return new TensorFlowSparkJob(name);
  }

  /**
   * @deprecated  Please use {@link #makeTonyJob} instead.
   *
   * @param name The job name
   * @return The job
   */
  @Deprecated
  TensorFlowJob makeTensorFlowTonyJob(String name) {
    return new TonyJob(name);
  }

  /**
   * Factory method to build a TonyJob
   *
   * @param name The job name
   * @return The job
   */
  TensorFlowJob makeTonyJob(String name) {
    return new TonyJob(name);
  }

  /**
   * Factory method to build a KubernetesJob
   *
   * @param name The job name
   * @return The job
   */
  KubernetesJob makeKubernetesJob(String name) {
    return new KubernetesJob(name);
  }

  /**
   * Factory method to build a Namespace.
   *
   * @param name The namespace name
   * @param project The Gradle project
   * @param parentScope The parent scope
   * @return The namespace
   */
  Namespace makeNamespace(String name, Project project, NamedScope parentScope) {
    return new Namespace(name, project, parentScope);
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
   * @param parentScope The parent scope
   * @return The PropertySet object
   */
  PropertySet makePropertySet(String name, NamedScope parentScope) {
    return new PropertySet(name, parentScope);
  }

  /**
   * Factory method to build a Workflow.
   *
   * @param name The workflow name
   * @param project The Gradle project
   * @param parentScope The parent scope
   * @return The workflow
   */
  Workflow makeWorkflow(String name, Project project, NamedScope parentScope) {
    return new Workflow(name, project, parentScope);
  }

  /**
   * Factory method to build a WormholePushJob.
   *
   * @param name The job name
   * @return The job
   */
  WormholePushJob makeWormholePushJob(String name) {
    return new WormholePushJob(name);
  }

  /**
   * Factory method to build a WormholePushJob2.
   *
   * @param name The job name
   * @return The job
   */
  WormholePushJob2 makeWormholePushJob2(String name) {
    return new WormholePushJob2(name);
  }

  /**
   * Factory method to build a KabootarJob.
   *
   * @param name The job name
   * @return The job
   */
  KabootarJob makeKabootarJob(String name) {
    return new KabootarJob(name);
  }

  /**
   * Factory method to build a CarbonJob.
   *
   * @param name The job name
   * @return The job
   */
  CarbonJob makeCarbonJob(String name) {
    return new CarbonJob(name);
  }

  /**
   * Factory method to build a Trigger.
   *
   * @param name The trigger name
   * @param project The Gradle project
   * @param parentScope The parent scope
   * @return The trigger
   */
  Trigger makeTrigger(String name, Project project) {
    return new Trigger(name, project);
  }

  /**
   * Factory method to build an Assertion.
   *
   * @param name The Assertion name
   * @param project The Gradle project
   * @param parentScope The parent scope
   * @return The AssertionExtension
   */
  AssertionWorkflowExtension makeAssertion(String name, Project project, NamedScope parentScope) {
    return new AssertionWorkflowExtension(name, project, parentScope);
  }
  /**
   * Factory method to build a Test.
   *
   * @param name The name of the test
   * @param project The Gradle project
   * @param parentScope The parent scope
   * @return The created Test
   */
  TestExtension makeTest(String name, Project project, NamedScope parentScope) {
    return new TestExtension(name, project, parentScope);
  }

}
