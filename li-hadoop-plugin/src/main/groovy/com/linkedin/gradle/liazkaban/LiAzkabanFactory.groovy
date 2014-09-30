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
package com.linkedin.gradle.liazkaban

import com.linkedin.gradle.azkaban.AzkabanFactory;
import com.linkedin.gradle.azkaban.AzkabanWorkflow;
import com.linkedin.gradle.azkaban.NamedScope;

import org.gradle.api.Project;

/**
 * LinkedIn-specific AzkabanFactory class that provides customized subclass instances.
 */
class LiAzkabanFactory extends AzkabanFactory {
  /**
   * Returns a LinkedIn-specific LiAzkabanWorkflow
   *
   * @param name The workflow name
   * @param project The Gradle project
   * @param nextLevel Reference to the parent scope
   * @return The workflow
   */
  @Override
  AzkabanWorkflow makeAzkabanWorkflow(String name, Project project, NamedScope nextLevel) {
    return new LiAzkabanWorkflow(name, project, nextLevel);
  }

  /**
   * Factory method to build a LinkedIn-specific PigLiJob.
   *
   * @param name The job name
   * @return The job
   */
  PigLiJob makePigLiJob(String name) {
    return new PigLiJob(name);
  }
}
