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
package com.linkedin.gradle.spark

import org.gradle.api.Project

class SparkExtension {

  Project project;

  // Properties that can be set by the user
  String sparkCacheDir = ".hadoopPlugin";
  String sparkCommand = "spark-submit";

  // Properties that must be set if the user is running Spark on a remote host
  String remoteHostName;
  String remoteCacheDir;
  String remoteSshOpts;

  /**
   * Constructor for the SparkExtension.
   *
   * @param project The Gradle project
   */
  SparkExtension(Project project) {
    this.project = project;
  }

  /**
   * Sets properties on the SparkExtension from the incoming properties object.
   *
   * @param properties The properties object
   */
  void readFromProperties(Properties properties) {
    sparkCacheDir = properties.containsKey("sparkCacheDir") ? properties.getProperty("sparkCacheDir") : sparkCacheDir;
    sparkCommand = properties.containsKey("sparkCommand") ? properties.getProperty("sparkCommand") : sparkCommand;
    remoteHostName = properties.containsKey("remoteHostName") ? properties.getProperty("remoteHostName") : remoteHostName;
    remoteCacheDir = properties.containsKey("remoteCacheDir") ? properties.getProperty("remoteCacheDir") : remoteCacheDir;
    remoteSshOpts = properties.containsKey("remoteSshOpts") ? properties.getProperty("remoteSshOpts") : remoteSshOpts;
  }

  /**
   * Validates the current set of properties set on the SparkExtension. Throws an exception if the
   * property vales cannot be validated.
   */
  void validateProperties() {

    if (!sparkCacheDir || !sparkCommand) {
      String msg = "You must set the properties sparkCacheDir and sparkCommand in your .sparkProperties file to generate Spark tasks";
      throw new Exception(msg);
    }

    if (remoteHostName) {
      if (!remoteCacheDir) {
        String msg = "If you set remoteHostName in your .sparkProperties file, you must also set remoteCacheDir to generate Spark tasks";
        throw new Exception(msg);
      }
    }
  }
}
