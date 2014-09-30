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

import com.linkedin.gradle.azkaban.AzkabanFactory;
import com.linkedin.gradle.azkaban.AzkabanPlugin;

import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to the Azkaban Plugin.
 */
class LiAzkabanPlugin extends AzkabanPlugin {
  @Override
  void apply(Project project) {
    super.apply(project);
    project.extensions.add("pigLiJob", this.&pigLiJob);
  }

  /**
   * Returns the LinkedIn-specific AzkabanFactory. Can be overridden by subclasses that wish to
   * provide their own AzkabanFactory.
   *
   * @return The AzkabanFactory to use
   */
  @Override
  AzkabanFactory makeAzkabanFactory() {
    return new LiAzkabanFactory();
  }

  /**
   * DSL pigLiJob method. Creates a PigLiJob in global scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  PigLiJob pigLiJob(String name, Closure configure) {
    LiAzkabanFactory liAzkabanFactory = (LiAzkabanFactory)azkabanFactory;
    return configureJob(liAzkabanFactory.makePigLiJob(name), configure);
  }
}
