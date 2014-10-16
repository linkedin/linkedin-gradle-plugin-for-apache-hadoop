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

/**
 * Defines a visitor pattern interface for the Hadoop DSL.
 *
 * Groovy has multi-methods (i.e. argument-type runtime specialization) so we can define method
 * overloads by argument type.
 */
interface Visitor {
  void visitExtension(HadoopDslExtension hadoop);
  void visitProperties(Properties props);
  void visitWorkflow(Workflow workflow);
  void visitJob(Job job);
  void visitJob(CommandJob job);
  void visitJob(HadoopJavaJob job);
  void visitJob(HiveJob job);
  void visitJob(JavaJob job);
  void visitJob(JavaProcessJob job);
  void visitJob(KafkaPushJob job);
  void visitJob(NoOpJob job);
  void visitJob(PigJob job);
  void visitJob(VoldemortBuildPushJob job);
}

/**
 * Base implementation of Visitor to make it easy to implement visitor subclasses. By default, the
 * multi-method overloads will simply call the overload for the base type. This will make it easy
 * for subclasses that want to only override the overload for the base type.
 */
class BaseVisitor implements Visitor {
  @Override
  void visitExtension(HadoopDslExtension hadoop) {
    // Visit each workflow
    hadoop.workflows.each() { Workflow workflow ->
      visitWorkflow(workflow);
    }

    // Visit each properties object
    hadoop.properties.each() { Properties props ->
      visitProperties(props);
    }
  }

  @Override
  void visitProperties(Properties props) {
  }

  @Override
  void visitWorkflow(Workflow workflow) {
    // Visit each job in the workflow
    workflow.jobs.each() { Job job ->
      visitJob(job);
    }

    // Visit each properties object in the workflow
    workflow.properties.each() { Properties props ->
      visitProperties(props);
    }
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
  void visitJob(VoldemortBuildPushJob job) {
    visitJob((Job)job);
  }
}