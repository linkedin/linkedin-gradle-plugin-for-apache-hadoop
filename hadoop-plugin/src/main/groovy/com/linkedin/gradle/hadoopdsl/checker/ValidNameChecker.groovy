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
package com.linkedin.gradle.hadoopdsl.checker;

import com.linkedin.gradle.hadoopdsl.Job;
import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.BaseStaticChecker;

import java.util.regex.Pattern;

import org.gradle.api.Project;

/**
 * The ValidNameChecker makes the following checks:
 * <ul>
 *   <li>Names for all DSL objects are limited to approved characters</li>
 * </ul>
 */
class ValidNameChecker extends BaseStaticChecker {
  /**
   * Regex pattern for the name validation.
   */
  static Pattern pattern = Pattern.compile("^[a-zA-Z0-9-]*\$");

  /**
   * Constructor for the ValidNameChecker.
   *
   * @param project The Gradle project
   */
  ValidNameChecker(Project project) {
    super(project);
  }

  /**
   * Validates names of declared objects in the DSL. Valid names are non-empty and must consist of
   * alphumeric characters plus hyphens. Spaces or underscores are not allowed.
   *
   * @param name The name to validate
   * @return Whether or not the name is valid
   */
  boolean validateName(String name) {
    if (name == null || name.isEmpty()) {
      return false;
    }
    return name.matches(pattern);
  }

  @Override
  void visitProperties(Properties props) {
    if (!validateName(props.name)) {
      project.logger.lifecycle("ValidNameChecker ERROR: The properties object ${props.name} has an invalid name. Names of objects declared in the DSL must be non-empty and consist of alphanumeric characters plus hyphens. Spaces or underscores are not allowed.");
      foundError = true;
    }
  }

  @Override
  void visitWorkflow(Workflow workflow) {
    if (!validateName(workflow.name)) {
      project.logger.lifecycle("ValidNameChecker ERROR: The workflow ${workflow.name} has an invalid name. Names of objects declared in the DSL must be non-empty and consist of alphanumeric characters plus hyphens. Spaces or underscores are not allowed.");
      foundError = true;
    }

    // Be sure to recursively check the jobs and properties contained in the workflow.
    super.visitWorkflow(workflow);
  }

  @Override
  void visitJob(Job job) {
    if (!validateName(job.name)) {
      project.logger.lifecycle("ValidNameChecker ERROR: The job ${job.name} has an invalid name. Names of objects declared in the DSL must be non-empty and consist of alphanumeric characters plus hyphens. Spaces or underscores are not allowed.");
      foundError = true;
    }
  }
}