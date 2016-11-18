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
package com.linkedin.gradle.lihadoopdsl;

import com.linkedin.gradle.hadoopdsl.HadoopDslExtension;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.lihadoopdsl.lijob.LiPigBangBangJob;
import com.linkedin.gradle.lihadoopdsl.lijob.PigLiJob;
import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to Workflow that allow for additional LinkedIn-specific job
 * types to be created in the extension.
 */
class LiHadoopDslExtension extends HadoopDslExtension implements LiNamedScopeContainer {
  /**
   * Base constructor for the HadoopDslExtension
   *
   * @param project The Gradle project
   */
  LiHadoopDslExtension(Project project) {
    super(project)
  }

  /**
   * Constructor for the HadoopDslExtension that is aware of its parent scope (global scope).
   *
   * @param project The Gradle project
   * @param parentScope The parent scope
   */
  LiHadoopDslExtension(Project project, NamedScope parentScope) {
    super(project, parentScope);
  }

  /**
   * DSL LiPigBangBangJob method creates a LiPigBangBangJob in workflow scope with the given name
   * and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  LiPigBangBangJob liPigBangBangJob(String name, @DelegatesTo(LiPigBangBangJob) Closure configure) {
    return ((LiPigBangBangJob)configureJob(((LiHadoopDslFactory)factory).makeLiPigBangBangJob(name), configure));
  }

  /**
   * @deprecated PigLiJob now has no differences with PigJob.
   *
   * DSL pigLiJob method. Creates a PigLiJob in workflow scope with the given name and
   * configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @Deprecated
  PigLiJob pigLiJob(String name, @DelegatesTo(PigLiJob) Closure configure) {
    return ((PigLiJob)configureJob(((LiHadoopDslFactory)factory).makePigLiJob(name), configure));
  }
}
