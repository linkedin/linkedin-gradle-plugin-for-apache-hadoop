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
package com.linkedin.gradle.spark;

import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.NamedScopeContainer;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;

import org.gradle.api.Project;

class SparkTaskHelper {
  /**
   * Finds the Spark jobs configured in the Hadoop DSL and returns them as a map of the job to their
   * containing scope.
   *
   * @param project The Gradle project
   * @return A map of each Spark job to its containing scope
   */
  static Map<SparkJob, NamedScope> findConfiguredSparkJobs(Project project) {
    Map<SparkJob, NamedScope> jobScopeMap = new LinkedHashMap<SparkJob, NamedScope>();

    if (project.extensions.hadoopDslPlugin) {
      HadoopDslPlugin hadoopDslPlugin = project.extensions.hadoopDslPlugin;
      findConfiguredSparkJobs(hadoopDslPlugin.scope, jobScopeMap);
    }

    return jobScopeMap;
  }

  /**
   * Finds SparkJobs configured in the DSL by recursively checking the scope containers.
   *
   * @param scope The scope to check
   * @param jobScopeMap A map of each Spark job to its containing scope
   */
  static void findConfiguredSparkJobs(NamedScope scope, Map<SparkJob, NamedScope> jobScopeMap) {
    scope.thisLevel.each { String name, Object val ->
      if (val instanceof SparkJob) {
        SparkJob sparkJob = (SparkJob)val;
        jobScopeMap.put(sparkJob, scope);
      }
      else if (val instanceof NamedScopeContainer) {
        NamedScopeContainer container = (NamedScopeContainer)val;
        findConfiguredSparkJobs(container.scope, jobScopeMap);
      }
    }
  }
}
