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
package com.linkedin.gradle.lispark;

import com.linkedin.gradle.spark.SparkExtension;
import com.linkedin.gradle.spark.SparkPlugin;

import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to the Spark Plugin.
 */
class LiSparkPlugin extends SparkPlugin {
  /**
   * Returns the LinkedIn-specific Spark extension. Can be overridden by subclasses.
   *
   * @param project The Gradle project
   * @return The SparkExtension object to use for the SparkPlugin
   */
  @Override
  SparkExtension makeSparkExtension(Project project) {
    return new LiSparkExtension(project);
  }

  @Override
  String buildRemoteSparkCmd(String executionJar, String appClass, Map<String, Object> confs, Set<String> flags, List<String> appParams, Map<String, Object> properties) {
    // At LinkedIn, we prefer all jobs submitted to the gateway box to run on our Hadoop YARN
    // cluster, instead of running locally on the gateway box. This customization sets the Spark
    // master to YARN if no master option is provided by user, or prints a gentle warning if master
    // is specifically set to local.
    if (properties.containsKey("master")) {
      if (properties.get("master").toLowerCase() ==~ /local.*/) {
        project.logger.lifecycle("This job is configured to run in local mode. It will run on the gateway box instead of on the Hadoop YARN cluster. It may take up limited gateway resources. If this is not the desired behavior, check out go/HaodopDSL, go/HadoopPlugin, and go/SparkFAQ for help on how to set the correct master option.")
      }
    }
    else {
      project.logger.lifecycle("No master option is provided, configuring the job to run on YARN by default.")
      properties.put("master", "yarn");
    }

    return super.buildRemoteSparkCmd(executionJar, appClass, confs, flags, appParams, properties)
  }
}
