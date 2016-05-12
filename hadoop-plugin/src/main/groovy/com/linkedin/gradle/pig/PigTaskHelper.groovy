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
package com.linkedin.gradle.pig;

import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.NamedScopeContainer;
import com.linkedin.gradle.hadoopdsl.job.PigJob;

import org.gradle.api.Project;

/**
 * Contains helper methods for use with the Pig Plugin.
 */
class PigTaskHelper {

  /**
   * Builds the necessary text to pass JVM parameters to Pig.
   *
   * @param jvmParameters The JVM parameters
   * @return The formatted JVM parameters text
   */
  static String buildJvmParameters(Map<String, String> jvmParameters) {
    return jvmParameters.collect { key, val -> return "-D${key}=${val}" }.join(" ");
  }

  /**
   * Builds the necessary text to pass script parameters to Pig.
   *
   * @param parameters The Pig parameters
   * @return The formatted Pig parametes text
   */
  static String buildPigParameters(Map<String, String> parameters) {
    return parameters.collect { key, val -> return "-param ${key}=${val}" }.join(" ");
  }

  /**
   * Uniquifies the task names that correspond to running Pig scripts on a host, since there may be
   * more than one Pig script with the same name recursively under ${project.projectDir}/src.
   *
   * @param fileName The Pig script file name
   * @param The set of task names that have been generated so far
   */
  static String buildUniqueTaskName(String fileName, Set<String> taskNames) {
    if (!taskNames.contains(fileName)) {
      return fileName;
    }

    StringBuilder sb = new StringBuilder(fileName);
    int length = sb.length();

    int index = 1;
    sb.append(index);

    while (taskNames.contains(sb.toString())) {
      sb.setLength(length);
      sb.append(++index);
    }

    return sb.toString();
  }

  /**
   * Finds the Pig jobs configured in the Hadoop DSL and returns them as a map of the job to their
   * containing scope.
   *
   * @param project The Gradle project
   * @return A map of each Pig job to its containing scope
   */
  static Map<PigJob, NamedScope> findConfiguredPigJobs(Project project) {
    Map<PigJob, NamedScope> jobScopeMap = new LinkedHashMap<PigJob, NamedScope>();

    if (project.extensions.hadoopDslPlugin) {
      HadoopDslPlugin hadoopDslPlugin = project.extensions.hadoopDslPlugin;
      findConfiguredPigJobs(hadoopDslPlugin.scope, jobScopeMap);
    }

    return jobScopeMap;
  }

  /**
   * Finds PigJobs configured in the DSL by recursively checking the scope containers.
   *
   * @param scope The scope to check
   * @param jobScopeMap A map of each Pig job to its containing scope
   */
  static void findConfiguredPigJobs(NamedScope scope, Map<PigJob, NamedScope> jobScopeMap) {
    scope.thisLevel.each { String name, Object val ->
      if (val instanceof PigJob) {
        PigJob pigJob = (PigJob)val;
        jobScopeMap.put(pigJob, scope);
      }
      else if (val instanceof NamedScopeContainer) {
        NamedScopeContainer container = (NamedScopeContainer)val;
        findConfiguredPigJobs(container.scope, jobScopeMap);
      }
    }
  }
}