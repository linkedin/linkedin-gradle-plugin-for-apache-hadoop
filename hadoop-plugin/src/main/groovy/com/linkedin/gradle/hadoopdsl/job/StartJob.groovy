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
package com.linkedin.gradle.hadoopdsl.job;

import com.linkedin.gradle.hadoopdsl.NamedScope;

/**
 * A StartJob is a special kind of NoOpJob that is placed at the root of a subflow that is used to
 * tie together the root jobs of the subflow and the target dependencies in the parent workflow.
 * <p>
 * Start jobs are not specified in the DSL. When you build a workflow with a dependency on a
 * subflow, a StartJob (with dependencies set to be the names of the subflow dependencies) is
 * created for you.
 */
class StartJob extends NoOpJob {
  // The targets in the parent workflow for this subflow. We use a separate member variable for
  // this (rather than using the inherited dependencyNames member variable) since many classes
  // (such as the static checker) assume the invariant that dependencyNames should only refer to
  // targets that are bound in the same scope (rather than the parent scope).
  Set<String> flowDependencyNames;

  /**
   * Constructor for a StartJob.
   *
   * @param jobName The job name
   */
  StartJob(String jobName) {
    super(jobName);
    flowDependencyNames = new LinkedHashSet<String>();
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jobProperties correctly.
   *
   * @param parentScope The parent scope in which to lookup the base properties
   * @return The job properties map that holds all the properties that will go into the built job file
   */
  @Override
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> allProperties = super.buildProperties(parentScope);

    // A StartJob connects the root jobs of a subflow to target dependencies in its parent
    // workflow. To accomplish this, refer to the dependencies using the parent workflow's scope
    // when we build the fully-qualified dependency names.
    if (flowDependencyNames.size() > 0) {
      allProperties["dependencies"] = flowDependencyNames.collect { String targetName -> return buildFileName(parentScope.nextLevel, targetName) }.join(",");
    }

    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  StartJob clone() {
    return clone(new StartJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  StartJob clone(StartJob cloneJob) {
    cloneJob.flowDependencyNames.addAll(flowDependencyNames);
    return ((StartJob)super.clone(cloneJob));
  }
}