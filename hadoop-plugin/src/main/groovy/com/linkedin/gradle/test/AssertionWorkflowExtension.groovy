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

package com.linkedin.gradle.test;


import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Workflow;
import org.gradle.api.Project;


/**
 * The AssertionWorkflowExtension allows users to add assertions in their workflow tests
 */
class AssertionWorkflowExtension extends Workflow {

  List<String> asserts;

  /**
   * Base constructor for a AssertionWorkflowExtension
   *
   * @param name The assertion name
   * @param project The Gradle project
   */
  AssertionWorkflowExtension(String name, Project project) {
    this(name, project, null);
  }

  /**
   * Constructor for a AssertionExtension given a parent scope.
   *
   * @param name The assertion name
   * @param project The Gradle project
   * @param parentScope The parent scope
   */
  AssertionWorkflowExtension(String name, Project project, NamedScope parentScope) {
    super(name,project, parentScope);
    asserts = new ArrayList<String>();
  }

  /**
   * This method helps to identify the workflow asserted by this assertion
   * @param workflowToAssert The name of the workflow to assert
   */
  @HadoopDslMethod
  void asserts(String workflowToAssert){
    asserts.add(workflowToAssert);
  }

  /**
   * This method helps to identify the list workflow asserted by this assertion
   * @param workflowToAssert The list of the workflow to assert
   */
  @HadoopDslMethod
  void asserts(List<String> workflowsToAssert) {
    asserts.addAll(workflowsToAssert);
  }

}
