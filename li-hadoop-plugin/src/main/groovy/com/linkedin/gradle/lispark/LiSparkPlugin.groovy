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
import com.linkedin.gradle.spark.SparkPlugin
import com.linkedin.gradle.spark.SparkSubmitHelper;

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
}
