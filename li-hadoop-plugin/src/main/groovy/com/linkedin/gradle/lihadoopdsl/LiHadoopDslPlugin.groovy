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
package com.linkedin.gradle.lihadoopdsl;

import com.linkedin.gradle.hadoopdsl.HadoopDslFactory;
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin;

import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to the Hadoop DSL Plugin.
 */
class LiHadoopDslPlugin extends HadoopDslPlugin {
  /**
   * Applies the LinkedIn-specific Hadoop DSL Plugin.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    super.apply(project);
    project.extensions.add("pigLiJob", this.&pigLiJob);
  }

  /**
   * Returns the LinkedIn-specific HadoopDslFactory. Can be overridden by subclasses that wish to
   * provide their own HadoopDslFactory.
   *
   * @return The HadoopDslFactory to use
   */
  @Override
  HadoopDslFactory makeFactory() {
    return new LiHadoopDslFactory();
  }

  /**
   * DSL pigLiJob method. Creates a PigLiJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  PigLiJob pigLiJob(String name, Closure configure) {
    LiHadoopDslFactory liFactory = (LiHadoopDslFactory)factory;
    return configureJob(liFactory.makePigLiJob(name), configure);
  }
}