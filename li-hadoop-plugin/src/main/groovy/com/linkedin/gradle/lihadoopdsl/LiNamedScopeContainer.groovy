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

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;
import com.linkedin.gradle.hadoopdsl.NamedScopeContainer;
import com.linkedin.gradle.lihadoopdsl.lijob.AutoTunePigLiJob;
import com.linkedin.gradle.lihadoopdsl.lijob.LiPigBangBangJob;
import com.linkedin.gradle.lihadoopdsl.lijob.PigLiJob;

/**
 * Interface that enables us to declare which DSL classes can declare a new scope (i.e. that have a
 * NamedScope object as a member variable).
 */
@SuppressWarnings("deprecation")
interface LiNamedScopeContainer extends NamedScopeContainer {
  /**
   * DSL liPigBangBangJob method creates a LiPigBangBangJob in workflow scope with the given name
   * and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  LiPigBangBangJob liPigBangBangJob(String name, @DelegatesTo(LiPigBangBangJob) Closure configure);

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
  @HadoopDslMethod
  PigLiJob pigLiJob(String name, @DelegatesTo(PigLiJob) Closure configure);

  /**
   * DSL autoTunePigLiJob method creates a AutoTunePigLiJob in workflow scope with the given name
   * and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  AutoTunePigLiJob autoTunePigLiJob(String name, @DelegatesTo(AutoTunePigLiJob) Closure configure);
}
