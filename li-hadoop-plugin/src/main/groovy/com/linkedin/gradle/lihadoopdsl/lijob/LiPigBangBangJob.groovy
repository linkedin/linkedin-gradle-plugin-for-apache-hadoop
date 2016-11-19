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
package com.linkedin.gradle.lihadoopdsl.lijob;

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.libangbang.BangBangCommand;
import groovy.json.internal.LazyMap;

/**
 * Extends the Pig Job class with the LiPigBangBang job type.
 * <p>
 * In the DSL, a LiPigBangBang can be specified with:
 * <pre>
 *   liPigBangBangJob('jobName'){
 *     uses 'myScript.pig'     // Required
 *     runsOn spec.linkedin-pig.linkedin-pig-h2
 *     overwrite false
 *     caches files: [
 *       'foo.jar' : '/user/bazz/foo.jar'
 *     ]
 *     cachesArchive files: [
 *       'foobar' : '/user/bazz/foobar.zip'
 *     ]
 *     reads files: [
 *       'foo' : '/data/databases/foo'
 *     ]
 *     writes files: [
 *       'bazz' : '/user/bazz/foobar'
 *     ]
 *     set parameters: [
 *       'param1' : 'val1'
 *       'param2' : 'val2'
 *     ]
 *     queue 'marathon
 *  }
 * </pre>
 */
class LiPigBangBangJob extends PigJob implements LiBangBangJob {
  Boolean overWrite = true;
  String pigDependency;

  /**
   * Constructor for a LiPigBangBangJob.
   *
   * @param jobName The job name
   */
  LiPigBangBangJob(String jobName) {
    super(jobName)
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  LiPigBangBangJob clone() {
    return clone(new LiPigBangBangJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  LiPigBangBangJob clone(LiPigBangBangJob cloneJob) {
    cloneJob.overWrite = overWrite
    cloneJob.pigDependency = pigDependency;
    return ((LiPigBangBangJob)super.clone(cloneJob));
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jobProperties correctly.
   *
   * @param parentScope The parent scope in which to lookup the base properties
   * @return The job properties map that holds all the properties that will go into the built job file
   */
  @Override
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> bangbangBuildProperties = super.buildProperties(parentScope);
    bangbangBuildProperties.put("type", "hadoopShell");
    bangbangBuildProperties.put("command", createBangBangCommand(parentScope));
    return bangbangBuildProperties;
  }

  /**
   * Helper function to build the bangbang command for this job.
   *
   * @param parentScope The parent scope in which this job is declared
   * @return The bangbang command for this job
   */
  String createBangBangCommand(NamedScope parentScope) {
    return new BangBangCommand.Builder().setGradleFile("${this.buildFileName(parentScope)}.gradle")
        .setTasks(['runShell']).setGradleArguments("--quiet").build().getCommandAsString();
  }

  /**
   * Returns the dependency of the compiler that was set using runsOn.
   *
   * @return The dependency of the compiler set using runsOn
   */
  @HadoopDslMethod
  @Override
  String getDependency() {
    return pigDependency;
  }

  /**
   * Whether or not the generated script should be overwritten.
   *
   * @return Whether or not the generated script should be overwritten
   */
  @HadoopDslMethod
  @Override
  boolean isOverwritten() {
    return overWrite;
  }

  /**
   * Specify whether to overwrite the generated Gradle file or not.
   *
   * @param overWrite Whether the generated file will be overwritten or not
   */
  @HadoopDslMethod
  @Override
  void overwrite(boolean overWrite) {
    this.overWrite = overWrite;
  }

  /**
   * Specify the Gradle coordinates of the compiler to user for running the script.
   *
   * @param dependency The Gradle coordinates of the compiler to use
   */
  @HadoopDslMethod
  @Override
  void runsOn(String dependency) {
    this.pigDependency = dependency;
  }

  /**
   * Specifies the dependencies to use for Pig from a map.
   *
   * @param dependencyMap The map to convert into the dependencies to use for Pig
   */
  @HadoopDslMethod
  void runsOn(LazyMap dependencyMap) {
    List<String> mapArgs = new ArrayList<String>();
    dependencyMap.each {
      key,value -> mapArgs.add("$key: '$value'");
    }
    runsOn(mapArgs.join(", "));
  }
}