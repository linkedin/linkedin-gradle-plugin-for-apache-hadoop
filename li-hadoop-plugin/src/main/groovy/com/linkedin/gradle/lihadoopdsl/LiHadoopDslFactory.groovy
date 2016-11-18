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

import com.linkedin.gradle.hadoopdsl.HadoopDslFactory;
import com.linkedin.gradle.hadoopdsl.Namespace;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.lihadoopdsl.lijob.LiPigBangBangJob;
import com.linkedin.gradle.lihadoopdsl.lijob.PigLiJob;
import org.gradle.api.Project;

/**
 * LinkedIn-specific HadoopDslFactory class that provides customized subclass instances.
 */
@SuppressWarnings("deprecation")
class LiHadoopDslFactory extends HadoopDslFactory {
  /**
   * Returns a LinkedIn-specific LiHadoopDslExtension.
   *
   * @param project The Gradle project
   * @param scope Reference to the global scope
   * @return The HadoopDslExtension
   */
  @Override
  LiHadoopDslExtension makeExtension(Project project, NamedScope scope) {
    return new LiHadoopDslExtension(project, scope);
  }

  /**
   * Returns a LinkedIn-specific LiNamespace.
   *
   * @param name The namespace name
   * @param project The Gradle project
   * @param parentScope The parent scope
   * @return The namespace
   */
  @Override
  Namespace makeNamespace(String name, Project project, NamedScope parentScope) {
    return new LiNamespace(name, project, parentScope);
  }

  /**
   * Returns a LinkedIn-specific LiWorkflow.
   *
   * @param name The workflow name
   * @param project The Gradle project
   * @param parentScope Reference to the parent scope
   * @return The workflow
   */
  @Override
  Workflow makeWorkflow(String name, Project project, NamedScope parentScope) {
    return new LiWorkflow(name, project, parentScope);
  }

  /**
   * Factory method to build a Linkedin-specific LiPigBangBangJob.
   *
   * @param name The job name
   * @return The job
   */
  LiPigBangBangJob makeLiPigBangBangJob(String name) {
    return new LiPigBangBangJob(name);
  }

  /**
   * @deprecated PigLiJob now has no differences with PigJob.
   *
   * Factory method to build a LinkedIn-specific PigLiJob.
   *
   * @param name The job name
   * @return The job
   */
  @Deprecated
  PigLiJob makePigLiJob(String name) {
    return new PigLiJob(name);
  }
}
