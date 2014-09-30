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
package com.linkedin.gradle.liazkaban;

import com.linkedin.gradle.azkaban.AzkabanWorkflow;
import com.linkedin.gradle.azkaban.NamedScope;

import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to AzkabanWorkflow that allow for additional LinkedIn-specific
 * job types to be created in the workflow.
 */
class LiAzkabanWorkflow extends AzkabanWorkflow {
  /**
   * Base constructor for a LiAzkabanWorkflow.
   *
   * @param name The workflow name
   * @param project The Gradle project
   */
  LiAzkabanWorkflow(String name, Project project) {
    super(name, project);
  }

  /**
   * Constructor for a LiAzkabanWorkflow given a parent scope.
   *
   * @param name The workflow name
   * @param project The Gradle project
   * @param nextLevel The parent scope
   */
  LiAzkabanWorkflow(String name, Project project, NamedScope nextLevel) {
    super(name, project, nextLevel);
  }

  /**
   * Clones the workflow.
   *
   * @return The cloned workflow
   */
  LiAzkabanWorkflow clone() {
    return clone(new LiAzkabanWorkflow(name, project, null));
  }

  /**
   * Helper method to set the properties on a cloned workflow.
   *
   * @param workflow The workflow being cloned
   * @return The cloned workflow
   */
  LiAzkabanWorkflow clone(LiAzkabanWorkflow workflow) {
    return super.clone(workflow);
  }

  /**
   * DSL pigLiJob method. Creates a PigLiJob in workflow scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  PigLiJob pigLiJob(String name, Closure configure) {
    return configureJob(((LiAzkabanFactory)azkabanFactory).makePigLiJob(name), configure);
  }
}
