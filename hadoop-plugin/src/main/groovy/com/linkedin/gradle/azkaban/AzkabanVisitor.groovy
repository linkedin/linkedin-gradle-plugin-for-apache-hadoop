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

/**
 * Defines a visitor pattern interface for the Azkaban DSL.
 *
 * Groovy has multi-methods (i.e. argument-type runtime specialization) so we can define method
 * overloads by argument type.
 */
interface AzkabanVisitor {
  void visitAzkabanExtension(AzkabanExtension azkaban);
  void visitAzkabanProperties(AzkabanProperties props);
  void visitAzkabanWorkflow(AzkabanWorkflow workflow);
  void visitAzkabanJob(AzkabanJob job);
  void visitAzkabanJob(CommandJob job);
  void visitAzkabanJob(HadoopJavaJob job);
  void visitAzkabanJob(HiveJob job);
  void visitAzkabanJob(JavaJob job);
  void visitAzkabanJob(JavaProcessJob job);
  void visitAzkabanJob(KafkaPushJob job);
  void visitAzkabanJob(NoOpJob job);
  void visitAzkabanJob(PigJob job);
  void visitAzkabanJob(VoldemortBuildPushJob job);
}

/**
 * Base implementation of AzkabanVisitor to make it easy to implement visitor subclasses. By
 * default, the multi-method overloads will simply call the overload for the base type. This will
 * make it easy for subclasses that want to only override the overload for the base type.
 */
class BaseAzkabanVisitor implements AzkabanVisitor {
  @Override
  void visitAzkabanExtension(AzkabanExtension azkaban) {
    // Visit each workflow
    azkaban.workflows.each() { AzkabanWorkflow workflow ->
      visitAzkabanWorkflow(workflow);
    }

    // Visit each properties object
    azkaban.properties.each() { AzkabanProperties props ->
      visitAzkabanProperties(props);
    }
  }

  @Override
  void visitAzkabanProperties(AzkabanProperties props) {
  }

  @Override
  void visitAzkabanWorkflow(AzkabanWorkflow workflow) {
    // Visit each job in the workflow
    workflow.jobs.each() { AzkabanJob job ->
      visitAzkabanJob(job);
    }

    // Visit each properties object in the workflow
    workflow.properties.each() { AzkabanProperties props ->
      visitAzkabanProperties(props);
    }
  }

  @Override
  void visitAzkabanJob(AzkabanJob job) {
  }

  @Override
  void visitAzkabanJob(CommandJob job) {
    visitAzkabanJob((AzkabanJob)job);
  }

  @Override
  void visitAzkabanJob(HadoopJavaJob job) {
    visitAzkabanJob((AzkabanJob)job);
  }

  @Override
  void visitAzkabanJob(HiveJob job) {
    visitAzkabanJob((AzkabanJob)job);
  }

  @Override
  void visitAzkabanJob(JavaJob job) {
    visitAzkabanJob((AzkabanJob)job);
  }

  @Override
  void visitAzkabanJob(JavaProcessJob job) {
    visitAzkabanJob((AzkabanJob)job);
  }

  @Override
  void visitAzkabanJob(KafkaPushJob job) {
    visitAzkabanJob((AzkabanJob)job);
  }

  @Override
  void visitAzkabanJob(NoOpJob job) {
    visitAzkabanJob((AzkabanJob)job);
  }

  @Override
  void visitAzkabanJob(PigJob job) {
    visitAzkabanJob((AzkabanJob)job);
  }

  @Override
  void visitAzkabanJob(VoldemortBuildPushJob job) {
    visitAzkabanJob((AzkabanJob)job);
  }
}
