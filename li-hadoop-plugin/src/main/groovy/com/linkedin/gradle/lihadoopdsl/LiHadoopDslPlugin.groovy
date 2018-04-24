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

import com.linkedin.gradle.hadoopdsl.HadoopDslCompiler;
import com.linkedin.gradle.hadoopdsl.HadoopDslFactory;
import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin;
import com.linkedin.gradle.liazkaban.LiAzkabanDslCompiler
import com.linkedin.gradle.liazkaban.LiAzkabanDslYamlCompiler;
import com.linkedin.gradle.lihadoopdsl.lijob.AutoTunePigLiJob;
import com.linkedin.gradle.lihadoopdsl.lijob.LiPigBangBangJob;
import com.linkedin.gradle.lihadoopdsl.lijob.PigLiJob;
import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to the Hadoop DSL Plugin.
 */
class LiHadoopDslPlugin extends HadoopDslPlugin implements LiNamedScopeContainer {

  static final String GENERATE_YAML_OUTPUT_FLAG_LOCATION = ".hadoop.generate_yaml_output";

  /**
   * Applies the LinkedIn-specific Hadoop DSL Plugin.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    // Enable users to skip the plugin
    if (project.hasProperty("disableHadoopDslPlugin")) {
      println("HadoopDslPlugin disabled");
      return;
    }

    super.apply(project);
    project.extensions.add("liPigBangBangJob", this.&liPigBangBangJob);
    project.extensions.add("pigLiJob", this.&pigLiJob);
    project.extensions.add("autoTunePigLiJob",this.&autoTunePigLiJob);
  }

  /**
   * Returns the LinkedIn-specific HadoopDslFactory. Can be overridden by subclasses that wish to
   * provide their own HadoopDslFactory.
   *
   * @return The HadoopDslFactory to use
   */
  @Override
  protected HadoopDslFactory makeFactory() {
    return new LiHadoopDslFactory();
  }

  /**
   * DSL liPigBangBangJob method creates a LiPigBangBangJob in scope with the given name
   * and configuration
   * @param name The Job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  LiPigBangBangJob liPigBangBangJob(String name, @DelegatesTo(LiPigBangBangJob) Closure configure) {
    LiHadoopDslFactory liFactory = (LiHadoopDslFactory)factory;
    return ((LiPigBangBangJob)configureJob(liFactory.makeLiPigBangBangJob(name), configure));
  }

  /**
   * @deprecated PigLiJob now has no differences with PigJob.
   *
   * DSL pigLiJob method. Creates a PigLiJob in scope with the given name and configuration.
   *
   * @param name The job name
   * @param configure The configuration closure
   * @return The new job
   */
  @Deprecated
  @HadoopDslMethod
  PigLiJob pigLiJob(String name, @DelegatesTo(PigLiJob) Closure configure) {
    LiHadoopDslFactory liFactory = (LiHadoopDslFactory)factory;
    return ((PigLiJob)configureJob(liFactory.makePigLiJob(name), configure));
  }

  /**
   * DSL autoTunePigLiJob method creates a AutoTunePigLiJob in scope with the given name
   * and configuration
   * @param name The Job name
   * @param configure The configuration closure
   * @return The new job
   */
  @HadoopDslMethod
  AutoTunePigLiJob autoTunePigLiJob(String name, @DelegatesTo(AutoTunePigLiJob) Closure configure){
    LiHadoopDslFactory liFactory = (LiHadoopDslFactory)factory;
    return ((AutoTunePigLiJob)configureJob(liFactory.makeAutoTunePigLiJob(name), configure));
  }

  /**
   * Based on whether or not the flag generate_yaml_output is set to true in the hadoop scope
   * (i.e. within the hadoop { } closure), select between LiAzkabanDslYamlCompiler and LiAzkabanDslCompiler.
   *
   * Default is LiAzkabanDslCompiler for now.
   *
   * Flag is called 'generate_yaml_output' because underscores are not allowed in the names of
   * other Hadoop objects, so it is highly unlikely for there to be unintentional collisions.
   *
   * @param project The Gradle project
   * @return The User-configured Compiler, default is LiAzkabanDslCompiler
   */
  HadoopDslCompiler selectCompilerType(Project project) {
    return scope.lookup(GENERATE_YAML_OUTPUT_FLAG_LOCATION) ?
            new LiAzkabanDslYamlCompiler(project) : new LiAzkabanDslCompiler(project);
  }
}