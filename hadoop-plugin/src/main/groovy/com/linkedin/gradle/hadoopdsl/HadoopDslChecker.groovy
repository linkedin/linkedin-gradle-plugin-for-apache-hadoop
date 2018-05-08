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

import com.linkedin.gradle.hadoopdsl.checker.JobDependencyChecker;
import com.linkedin.gradle.hadoopdsl.checker.PropertySetChecker;
import com.linkedin.gradle.hadoopdsl.checker.RequiredFieldsChecker;
import com.linkedin.gradle.hadoopdsl.checker.RequiredParametersChecker;
import com.linkedin.gradle.hadoopdsl.checker.TriggerChecker;
import com.linkedin.gradle.hadoopdsl.checker.ValidFieldsChecker;
import com.linkedin.gradle.hadoopdsl.checker.ValidNameChecker;
import com.linkedin.gradle.hadoopdsl.checker.WorkflowJobChecker;

import org.gradle.api.Project;

/**
 * Top-level static checker that scans the user's DSL for any potential problems. The static
 * checker consists of a series of static checking rules, each of which is typically implemented
 * in its own class.
 * <p>
 * The static checker applies the following checker classes:
 * <ul>
 *   <li>ValidNameChecker: checks that names for all DSL objects are limited to approved characters</li>
 *   <li>RequiredFieldsChecker: checks that all the required fields in the DSL are set</li>
 *   <li>WorkflowJobChecker: checks various properties of workflows</li>
 *   <li>JobDependencyChecker: checks various properties of jobs, such as no cyclic dependencies and potential read-before-write race conditions</li>
 *   <li>PropertySetChecker: checks that property files and jobs that declare baseProperties refer to valid property sets</li>
 *   <li>TriggerChecker: checks that the triggers and their  dependencies are properly defined</li>
 * </ul>
 */
class HadoopDslChecker extends BaseStaticChecker {
  /**
   * Constructor for the Hadoop DSL static checker.
   *
   * @param project The Gradle project
   */
  HadoopDslChecker(Project project) {
    super(project);
  }

  /**
   * Builds the list of static checking rules that will be applied to DSL. This factory method can
   * be overridden by subclasses to customize or manipulate the static checks that get made.
   *
   * @return The list of static checks to apply to the DSL
   */
  List<StaticChecker> buildStaticChecks() {
    List<StaticChecker> checks = new ArrayList<StaticChecker>();
    checks.add(makeValidNameChecker());
    checks.add(makeRequiredFieldsChecker());
    checks.add(makeRequiredParametersChecker());
    checks.add(makeWorkflowJobChecker());
    checks.add(makeJobDependencyChecker());
    checks.add(makePropertySetChecker());
    checks.add(makeValidFieldsChecker());
    checks.add(makeTriggerChecker());
    return checks;
  }

  /**
   * Makes this static check on the DSL.
   *
   * @param plugin The Hadoop DSL plugin
   */
  @Override
  void check(HadoopDslPlugin plugin) {
    List<StaticChecker> checks = buildStaticChecks();
    checks.each { staticCheck ->
      if (!foundError) {
        staticCheck.check(plugin);
        foundError |= staticCheck.failedCheck();
      }
    }
  }

  /**
   * Factory method to build the JobDependencyChecker check. Allows subclasses to provide a custom
   * implementation of this check.
   *
   * @return The JobDependencyChecker check
   */
  JobDependencyChecker makeJobDependencyChecker() {
    return new JobDependencyChecker(project);
  }

  /**
   * Factory method to build the PropertySetChecker check. Allows subclasses to provide a custom
   * implementation of this check.
   *
   * @return The RequiredFieldsChecker check
   */
  PropertySetChecker makePropertySetChecker() {
    return new PropertySetChecker(project);
  }

  /**
   * Factory method to build the RequiredFieldsChecker check. Allows subclasses to provide a custom
   * implementation of this check.
   *
   * @return The RequiredFieldsChecker check
   */
  RequiredFieldsChecker makeRequiredFieldsChecker() {
    return new RequiredFieldsChecker(project);
  }

  /**
   * Factory method to build the RequiredParametersChecker check. Allows subclasses to provide a custom
   * implementation of this check.
   *
   * @return The RequiredParametersChecker check
   */
  RequiredParametersChecker makeRequiredParametersChecker() {
    return new RequiredParametersChecker(project);
  }

  /**
   * Factory method to build the ValidFieldsChecker check. Allows subclasses to provide a custom
   * implementation of this check.
   * @return The ValidFieldsChecker check
   */
  ValidFieldsChecker makeValidFieldsChecker() {
    return new ValidFieldsChecker(project);
  }

  /**
   * Factory method to build the ValidNameChecker check. Allows subclasses to provide a custom
   * implementation of this check.
   *
   * @return The ValidNameChecker check
   */
  ValidNameChecker makeValidNameChecker() {
    return new ValidNameChecker(project);
  }

  /**
   * Factory method to build the WorkflowJobChecker check. Allows subclasses to provide a custom
   * implementation of this check.
   *
   * @return The WorkflowJobChecker check
   */
  WorkflowJobChecker makeWorkflowJobChecker() {
    return new WorkflowJobChecker(project);
  }

  /**
   * Factory method to build the TriggerChecker check. Allows subclasses to provide a custom
   * implementation of this check.
   *
   * @return The TriggerChecker check
   */
  TriggerChecker makeTriggerChecker() {
    return new TriggerChecker(project);
  }
}
