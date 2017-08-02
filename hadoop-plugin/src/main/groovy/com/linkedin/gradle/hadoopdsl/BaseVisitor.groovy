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
import com.linkedin.gradle.hadoopdsl.job.HdfsWaitJob;
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
import com.linkedin.gradle.hadoopdsl.job.TableauJob;
import com.linkedin.gradle.hadoopdsl.job.TeradataToHdfsJob;
import com.linkedin.gradle.hadoopdsl.job.VenicePushJob;
import com.linkedin.gradle.hadoopdsl.job.VoldemortBuildPushJob;

/**
 * Base implementation of Visitor to make it easy to implement visitor subclasses. By default, the
 * multi-method overloads will simply call the overload for the base type. This will make it easy
 * for subclasses that want to only override the overload for the base type.
 */
@SuppressWarnings("deprecation")
abstract class BaseVisitor implements Visitor {
  /**
   * Keep track of the top-level extension.
   */
  HadoopDslExtension extension;

  /**
   * Keeps track of the current parent scope for the DSL element being built.
   */
  NamedScope parentScope;

  /**
   * Helper method for DSL elements that subclass BaseNamedScopeContainer.
   *
   * @param container The DSL element subclassing BaseNamedScopeContainer
   */
  void visitScopeContainer(BaseNamedScopeContainer container) {
    // Save the last scope information
    NamedScope oldParentScope = this.parentScope;

    // Set the new parent scope
    this.parentScope = container.scope;

    // Visit each job
    container.jobs.each { Job job ->
      visitJob(job);
    }

    // Visit each workflow
    container.workflows.each { Workflow workflow ->
      visitWorkflow(workflow);
    }

    // Visit each property set object
    container.propertySets.each { PropertySet propertySet ->
      visitPropertySet(propertySet);
    }

    // Visit each properties object
    container.properties.each { Properties props ->
      visitProperties(props);
    }

    // Visit each child namespace
    container.namespaces.each { Namespace namespace ->
      visitNamespace(namespace);
    }

    // Restore the last parent scope
    this.parentScope = oldParentScope;
  }

  @Override
  void visitPlugin(HadoopDslPlugin plugin) {
    visitScopeContainer(plugin);
    visitExtension(plugin.extension);
  }

  @Override
  void visitExtension(HadoopDslExtension hadoop) {
    // Save the extension and visit its nested DSL elements
    this.extension = hadoop;
    visitScopeContainer(hadoop);
  }

  @Override
  void visitNamespace(Namespace namespace) {
    visitScopeContainer(namespace);
  }

  @Override
  void visitProperties(Properties props) {
  }

  @Override
  void visitPropertySet(PropertySet propertySet) {
  }

  @Override
  void visitWorkflow(Workflow workflow) {
    visitScopeContainer(workflow);
  }

  @Override
  void visitJob(Job job) {
  }

  @Override
  void visitJob(CommandJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(HadoopJavaJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(HadoopShellJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(HiveJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(JavaJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(JavaProcessJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(KafkaPushJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(NoOpJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(PigJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(PinotBuildAndPushJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(SparkJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(VoldemortBuildPushJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(HdfsToTeradataJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(TableauJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(TeradataToHdfsJob job) {
    visitJob((Job)job);
  }

 @Override
  void visitJob(HdfsToEspressoJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(GobblinJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(SqlJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(HdfsWaitJob job) {
    visitJob((Job)job);
  }

  @Override
  void visitJob(VenicePushJob job) {
    visitJob((Job)job);
  }
}
