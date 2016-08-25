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


import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.libangbang.BangBangCommand;
import groovy.json.internal.LazyMap;

/**
 * Extend the Pig Job class with the LiPigBangBang job type.
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

  String pigDependency;
  Boolean overWrite = true;

  LiPigBangBangJob(String jobName) {
    super(jobName)
  }

/**
 * Get the dependency string of the pig to run
 * @param dependency The dependency string of the pig to run
 */
  @Override
  void runsOn(String pigDependency) {
    this.pigDependency = pigDependency;
  }

  /**
   * Get the dependency map of the pig to run
   * @param pigDependency The dependency string of the pig to run
   */
  void runsOn(LazyMap pigDependency) {
    this.pigDependency = wrapValueInQuotes(pigDependency);
  }

  /**
   * Wraps the value in quotes
   * @param pigDependency The pig dependency map to convert
   * @return The Map String in quoted values
   */
  private String wrapValueInQuotes(LazyMap pigDependency) {
    List<String> mapArgs = new ArrayList<String>();
      pigDependency.each {
        key,value -> mapArgs.add("$key: '$value'");
    }
    return mapArgs.join(", ");
  }

  void overwrite(boolean overWrite) {
    this.overWrite = overWrite;
  }

  @Override
  String getDependency() {
    return pigDependency;
  }

  @Override
  boolean isOverwritten() {
    return overWrite;
  }

  @Override
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> bangbangBuildProperties = super.buildProperties(parentScope);
    bangbangBuildProperties.put("type", "hadoopShell");
    bangbangBuildProperties.put("command", createBangBangCommand(parentScope));
    return bangbangBuildProperties;
  }

  String createBangBangCommand(NamedScope parentScope) {
    return new BangBangCommand.Builder().setGradleFile("${this.buildFileName(parentScope)}.gradle").setTasks(['runShell']).
        setGradleArguments("--quiet").build().getCommandAsString();
  }
}
