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
 * Interface that enables us to declare which DSL classes can declare a new scope (i.e. that have a
 * NamedScope object as a member variable).
 */
@SuppressWarnings("deprecation")
interface NamedScopeContainer {
  /**
   * Returns the scope at this level.
   *
   * @return The scope at this level
   */
  NamedScope getScope();

  /**
   * DSL addJob method. Looks up the job with given name, clones it, configures the clone with the
   * given configuration closure and adds the clone to the workflow.
   *
   * @param name The job name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured job that was added to the workflow
   */
  @HadoopDslMethod
  Job addJob(String name, @DelegatesTo(Job) Closure configure);

  /**
   * DSL addJob method. Looks up the job with given name, clones it, renames the clone to the
   * specified name, configures the clone with the given configuration closure and adds the clone
   * to the workflow.
   *
   * @param name The job name to lookup
   * @param rename The new name to give the cloned job
   * @param configure The configuration closure
   * @return The cloned, renamed and configured job that was added to the workflow
   */
  @HadoopDslMethod
  Job addJob(String name, String rename, @DelegatesTo(Job) Closure configure);

  /**
   * DSL addNamespace method. Looks up the namespace with given name, clones it, configures the
   * clone with the given configuration closure and binds the clone in scope.
   *
   * @param name The namespace name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured namespace that was bound in scope
   */
  @HadoopDslMethod
  Namespace addNamespace(String name, @DelegatesTo(Namespace) Closure configure);

  /**
   * DSL addNamespace method. Looks up the namespace with given name, clones it, renames the clone
   * to the specified name, configures the clone with the given configuration closure and binds the
   * clone in scope.
   *
   * @param name The namespace name to lookup
   * @param rename The new name to give the cloned namespace
   * @param configure The configuration closure
   * @return The cloned, renamed and configured namespace that was bound in scope
   */
  @HadoopDslMethod
  Namespace addNamespace(String name, String rename, @DelegatesTo(Namespace) Closure configure);

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, configures the
   * clone with the given configuration closure and binds the clone in scope.
   *
   * @param name The properties name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured properties object that was bound in scope
   */
  @HadoopDslMethod
  Properties addPropertyFile(String name, @DelegatesTo(Properties) Closure configure);

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, renames the
   * clone to the specified name, configures the clone with the given configuration closure and
   * binds the clone in scope.
   *
   * @param name The properties name to lookup
   * @param rename The new name to give the cloned properties object
   * @param configure The configuration closure
   * @return The cloned, renamed and configured properties object that was bound in scope
   */
  @HadoopDslMethod
  Properties addPropertyFile(String name, String rename, @DelegatesTo(Properties) Closure configure);

  /**
   * DSL addPropertySet method. Looks up the PropertySet with given name, clones it, configures the
   * clone with the given configuration closure and binds the clone in scope.
   *
   * @param name The PropertySet name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured PropertySet that was bound in scope
   */
  @HadoopDslMethod
  PropertySet addPropertySet(String name, @DelegatesTo(PropertySet) Closure configure);

  /**
   * DSL addPropertySet method. Looks up the PropertySet with given name, clones it, renames the
   * clone to the specified name, configures the clone with the given configuration closure and
   * binds the clone in scope.
   *
   * @param name The PropertySet name to lookup
   * @param rename The new name to give the cloned PropertySet
   * @param configure The configuration closure
   * @return The cloned, renamed and configured PropertySet that was bound in scope
   */
  @HadoopDslMethod
  PropertySet addPropertySet(String name, String rename, @DelegatesTo(PropertySet) Closure configure);

  /**
   * DSL addWorkflow method. Looks up the workflow with given name, clones it, configures the clone
   * with the given configuration closure and binds the clone in scope.
   *
   * @param name The workflow name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured workflow that was bound in scope
   */
  @HadoopDslMethod
  Workflow addWorkflow(String name, @DelegatesTo(Workflow) Closure configure);

  /**
   * DSL addWorkflow method. Looks up the workflow with given name, clones it, renames the clone to
   * the specified name, configures the clone with the given configuration closure and binds the
   * clone in scope.
   *
   * @param name The workflow name to lookup
   * @param rename The new name to give the cloned workflow
   * @param configure The configuration closure
   * @return The cloned, renamed and configured workflow that was bound in scope
   */
  @HadoopDslMethod
  Workflow addWorkflow(String name, String rename, @DelegatesTo(Workflow) Closure configure);

  /**
   * DSL evalHadoopClosure method. Evaluates the specified hadoopClosure against the specified
   * definition set and target.
   *
   * @param args A map whose required key "closureName" specifies the named hadoopClosure to evaluate
   *             and optional key "defs" specifies the definition set name to use as the current definition set before evaluating the closure
   *             and whose optional key "targetName" specifies the name of the Hadoop DSL object to set as the closure delegate before evaluating the closure.
   *             If the definition set is not specified, the default definition set is used, and if the target name is not specified, this object is used as the specified delegate target.
   */
  @HadoopDslMethod
  void evalHadoopClosure(Map args);

  /**
   * DSL evalHadoopClosures method. Evaluates all the anonymous hadoopClosure closures against the
   * default definition set and using this object as the specified delegate target.
   */
  @HadoopDslMethod
  void evalHadoopClosures();

  /**
   * DSL evalHadoopClosures method. Evaluates all the anonymous hadoopClosure closures against the
   * specified definition set and using this object as the specified delegate target.
   *
   * @param definitionSetName The definition set name to use as the current definition set before evaluating the closures
   */
  @HadoopDslMethod
  void evalHadoopClosures(String definitionSetName);

  /**
   * DSL evalHadoopClosures method. Evaluates all the anonymous hadoopClosure closures against the
   * specified definition set and target.
   *
   * @param args A map whose optional key "defs" specifies the definition set name to use as the current definition set before evaluating the closures
   *             and whose optional key "targetName" specifies the name of the Hadoop DSL object to set as the closure delegate before evaluating the closure.
   *             If the definition set is not specified, the default definition set is used, and if the target name is not specified, this object is used as the specified delegate target.
   */
  @HadoopDslMethod
  void evalHadoopClosures(Map args);

  /**
   * DSL lookup method. Looks up an object in scope.
   *
   * @param name The name to lookup
   * @return The object that is bound in scope to the given name, or null if no such name is bound in scope
   */
  @HadoopDslMethod
  Object lookup(String name);

  /**
   * DSL lookup method. Looks up an object in scope and then applies the given configuration
   * closure.
   *
   * @param name The name to lookup
   * @param configure The configuration closure
   * @return The object that is bound in scope to the given name, or null if no such name is bound in scope
   */
  @HadoopDslMethod
  Object lookup(String name, Closure configure);

  /**
   * DSL lookupRef method. Looks up the scope binding reference for an object.
   *
   * @param name The name to lookup
   * @return The NamedScopeReference for the binding, or null if the given name is not bound in scope
   */
  @HadoopDslMethod
  NamedScopeReference lookupRef(String name);

  /**
   * DSL namespace method. Creates a Namespace in scope with the given name and configuration.
   * <p>
   * For ease of organizing the user's Gradle scripts, namespaces can be redeclared at the same
   * scope level. If the namespace already exists, it will simply be configured with the given
   * closure configuration.
   *
   * @param name The namespace name
   * @param configure The configuration closure
   * @return The namespace
   */
  @HadoopDslMethod
  Namespace namespace(String name, @DelegatesTo(Namespace) Closure configure);

  /**
   * DSL propertyFile method. Creates a Properties object in scope with the given name and
   * configuration.
   *
   * @param name The properties name
   * @param configure The configuration closure
   * @return The new properties object
   */
  @HadoopDslMethod
  Properties propertyFile(String name, @DelegatesTo(Properties) Closure configure);

  /**
   * DSL propertySet method. Creates a PropertySet object in scope with the given name and
   * configuration.
   *
   * @param name The PropertySet name
   * @param configure The configuration closure
   * @return The new PropertySet object
   */
  @HadoopDslMethod
  PropertySet propertySet(String name, @DelegatesTo(PropertySet) Closure configure);

  /**
   * DSL workflow method. Creates a Workflow in scope with the given name and configuration.
   *
   * @param name The workflow name
   * @param configure The configuration closure
   * @return The new workflow
   */
  @HadoopDslMethod
  Workflow workflow(String name, @DelegatesTo(Workflow) Closure configure);

  /**
   * DSL job method. Creates an Job in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  Job job(String name, @DelegatesTo(Job) Closure configure);

  /**
   * DSL commandJob method. Creates a CommandJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  CommandJob commandJob(String name, @DelegatesTo(CommandJob) Closure configure);

  /**
   * DSL hadoopJavaJob method. Creates a HadoopJavaJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  HadoopJavaJob hadoopJavaJob(String name, @DelegatesTo(HadoopJavaJob) Closure configure);

  /**
   * DSL hadoopShellJob method. Creates a HadoopShellJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure.
   * @return The new job
   */
  @HadoopDslMethod
  HadoopShellJob hadoopShellJob(String name, @DelegatesTo(HadoopShellJob) Closure configure);

  /**
   * DSL hiveJob method. Creates a HiveJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  HiveJob hiveJob(String name, @DelegatesTo(HiveJob) Closure configure);

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
  @HadoopDslMethod
  JavaJob javaJob(String name, @DelegatesTo(JavaJob) Closure configure);

  /**
   * DSL javaProcessJob method. Creates a JavaProcessJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  JavaProcessJob javaProcessJob(String name, @DelegatesTo(JavaProcessJob) Closure configure);

  /**
   * DSL kafkaPushJob method. Creates a KafkaPushJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  KafkaPushJob kafkaPushJob(String name, @DelegatesTo(KafkaPushJob) Closure configure);

  /**
   * DSL noOpJob method. Creates a NoOpJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  NoOpJob noOpJob(String name, @DelegatesTo(NoOpJob) Closure configure);

  /**
   * DSL pigJob method. Creates a PigJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  PigJob pigJob(String name, @DelegatesTo(PigJob) Closure configure);

  /**
   * DSL pinotBuildAndPushJob method. Creates a PinotBuildAndPushJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  PinotBuildAndPushJob pinotBuildAndPushJob(String name, @DelegatesTo(PinotBuildAndPushJob) Closure configure);

  /**
   * DSL sparkJob method. Creates a SparkJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  SparkJob sparkJob(String name, @DelegatesTo(SparkJob) Closure configure);

  /**
   * DSL voldemortBuildPushJob method. Creates a VoldemortBuildPushJob in scope with the given name
   * and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  VoldemortBuildPushJob voldemortBuildPushJob(String name, @DelegatesTo(VoldemortBuildPushJob) Closure configure);

  /**
   * DSL hdfsToTeradataJob method. Creates a HdfsToTeradataJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  HdfsToTeradataJob hdfsToTeradataJob(String name, @DelegatesTo(HdfsToTeradataJob) Closure configure);

  /**
   * DSL tableauJob method. Creates a TableauJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  TableauJob tableauJob(String name, @DelegatesTo(TableauJob) Closure configure);

  /**
   * DSL teradataToHdfsJob method. Creates a TeradataToHdfsJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  TeradataToHdfsJob teradataToHdfsJob(String name, @DelegatesTo(TeradataToHdfsJob) Closure configure);

  /**
   * DSL hdfsToEspresso method. Creates a HdfsToEspressoJob in scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  HdfsToEspressoJob hdfsToEspressoJob(String name, @DelegatesTo(HdfsToEspressoJob) Closure configure);

  /**
   * DSL gobblinJob method. Creates a GobblinJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  GobblinJob gobblinJob(String name, @DelegatesTo(GobblinJob) Closure configure);

  /**
   * DSL sqlJob method. Creates a SqlJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  SqlJob sqlJob(String name, @DelegatesTo(SqlJob) Closure configure);

  /**
   * DSL hdfsWaitJob method. Creates an HdfsWaitJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
   @HadoopDslMethod
   HdfsWaitJob hdfsWaitJob(String name, @DelegatesTo(HdfsWaitJob) Closure configure);

  /**
   * DSL venicePushJob method. Creates a VenicePushJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  VenicePushJob venicePushJob(String name, @DelegatesTo(VenicePushJob) Closure configure);
}
