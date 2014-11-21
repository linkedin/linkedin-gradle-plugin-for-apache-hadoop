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

import com.linkedin.gradle.hadoopdsl.BaseStaticChecker;
import com.linkedin.gradle.hadoopdsl.Job;
import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.PropertySet;

import org.gradle.api.Project;

/**
 * The PropertySetChecker makes the following checks:
 * <ul>
 *   <li>ERROR if a Properties object refers to a name for its baseProperties that is not in scope or is not bound to a PropertySet</li>
 *   <li>ERROR if a Job refers to a name for its baseProperties that is not in scope or is not bound to a PropertySet</li>
 * </ul>
 */
class PropertySetChecker extends BaseStaticChecker {
  /**
   * Constructor for the PropertySetChecker.
   *
   * @param project The Gradle project
   */
  PropertySetChecker(Project project) {
    super(project);
  }

  @Override
  void visitJob(Job job) {
    String basePropertySetName = job.basePropertySetName;
    if (basePropertySetName == null)
      return;

    Object object = parentScope.lookup(basePropertySetName);
    if (object == null) {
      project.logger.lifecycle("PropertySetChecker ERROR: job ${job.name} declares baseProperties ${basePropertySetName} that is not bound in scope");
      foundError = true;
      return;
    }

    if (!(object instanceof PropertySet)) {
      project.logger.lifecycle("PropertySetChecker ERROR: job ${job.name} declares baseProperties ${basePropertySetName} that refers to an object that is not a PropertySet");
      foundError = true;
      return;
    }
  }

  @Override
  void visitProperties(Properties props) {
    String basePropertySetName = props.basePropertySetName;
    if (basePropertySetName == null)
      return;

    Object object = parentScope.lookup(basePropertySetName);
    if (object == null) {
      project.logger.lifecycle("PropertySetChecker ERROR: properties ${props.name} declares baseProperties ${basePropertySetName} that is not bound in scope");
      foundError = true;
      return;
    }

    if (!(object instanceof PropertySet)) {
      project.logger.lifecycle("PropertySetChecker ERROR: properties ${props.name} declares baseProperties ${basePropertySetName} that refers to an object that is not a PropertySet");
      foundError = true;
      return;
    }
  }
}