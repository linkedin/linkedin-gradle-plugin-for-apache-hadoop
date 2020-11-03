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
import com.linkedin.gradle.hadoopdsl.job.NoOpJob;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.hadoopdsl.job.PinotBuildAndPushJob;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;
import com.linkedin.gradle.hadoopdsl.job.SqlJob;
import com.linkedin.gradle.hadoopdsl.job.TableauJob;
import com.linkedin.gradle.hadoopdsl.job.TensorFlowJob;
import com.linkedin.gradle.hadoopdsl.job.TeradataToHdfsJob;
import com.linkedin.gradle.hadoopdsl.job.VenicePushJob;
import com.linkedin.gradle.hadoopdsl.job.VoldemortBuildPushJob;
import com.linkedin.gradle.hadoopdsl.job.WormholePushJob;
import com.linkedin.gradle.hadoopdsl.job.WormholePushJob2;

/**
 * Defines a visitor pattern interface for the Hadoop DSL.
 *
 * Groovy has multi-methods (i.e. argument-type runtime specialization) so we can define method
 * overloads by argument type.
 */
@SuppressWarnings("deprecation")
interface Visitor {
  void visitPlugin(HadoopDslPlugin plugin);
  void visitExtension(HadoopDslExtension hadoop);
  void visitNamespace(Namespace namespace);
  void visitProperties(Properties props);
  void visitPropertySet(PropertySet propertySet);
  void visitWorkflow(Workflow workflow);
  void visitJob(Job job);
  void visitJob(CommandJob job);
  void visitJob(GobblinJob job);
  void visitJob(HadoopJavaJob job);
  void visitJob(HadoopShellJob job);
  void visitJob(HdfsToEspressoJob job);
  void visitJob(HdfsToTeradataJob job);
  void visitJob(HdfsWaitJob job);
  void visitJob(HiveJob job);
  void visitJob(JavaJob job);
  void visitJob(JavaProcessJob job);
  void visitJob(KafkaPushJob job);
  void visitJob(NoOpJob job);
  void visitJob(PigJob job);
  void visitJob(PinotBuildAndPushJob job);
  void visitJob(SparkJob job);
  void visitJob(SqlJob job);
  void visitJob(TableauJob job);
  void visitJob(TensorFlowJob job);
  void visitJob(TeradataToHdfsJob job);
  void visitJob(VenicePushJob job);
  void visitJob(VoldemortBuildPushJob job);
  void visitJob(WormholePushJob job);
  void visitJob(KabootarJob job);
  void visitJob(WormholePushJob2 job);
  void visitJob(KubernetesJob job);
  void visitJob(CarbonJob job);
}
