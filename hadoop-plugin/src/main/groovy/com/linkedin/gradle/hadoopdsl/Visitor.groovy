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
  void visitPlugin(HadoopDslPlugin plugin);
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