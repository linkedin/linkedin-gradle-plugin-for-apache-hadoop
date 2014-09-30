/*
 * Copyright 2014 LinkedIn Corp.
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

import org.gradle.api.Project;

/**
 * PigExtension exposes configuration properties for running Pig scripts from the command line.
 * <p>
 * NOTE: It is very difficult to use the Gradle DSL itself at configuration time to configure other
 * tasks. It is much more convenient to simply load the properties by an explicit property file.
 */
class PigExtension {
  Project project;

  // Properties that can be set by the user
  String dependencyConf;
  String pigCacheDir = ".hadoopPlugin";
  String pigCommand = "pig";
  String pigOptions;

  // The user can set this to false to not generate any Pig tasks
  boolean generateTasks = true;

  // Properties that must be set if the user is running Pig on a remote host
  String remoteHostName;
  String remoteCacheDir;
  String remoteSshOpts;

  /**
   * Constructor for the PigExtension.
   *
   * @param project The Gradle project
   */
  PigExtension(Project project) {
    this.project = project;
  }

  /**
   * Sets properties on the PigExtension from the incoming properties object.
   *
   * @param properties The properties object
   */
  void readFromProperties(Properties properties) {
    generateTasks = properties.containsKey("generateTasks") ? Boolean.parseBoolean(properties.getProperty("generateTasks")) : generateTasks;
    dependencyConf = properties.containsKey("dependencyConf") ? properties.getProperty("dependencyConf") : dependencyConf;
    pigCacheDir = properties.containsKey("pigCacheDir") ? properties.getProperty("pigCacheDir") : pigCacheDir;
    pigCommand = properties.containsKey("pigCommand") ? properties.getProperty("pigCommand") : pigCommand;
    pigOptions = properties.containsKey("pigOptions") ? properties.getProperty("pigOptions") : pigOptions;
    remoteHostName = properties.containsKey("remoteHostName") ? properties.getProperty("remoteHostName") : remoteHostName;
    remoteCacheDir = properties.containsKey("remoteCacheDir") ? properties.getProperty("remoteCacheDir") : remoteCacheDir;
    remoteSshOpts = properties.containsKey("remoteSshOpts") ? properties.getProperty("remoteSshOpts") : remoteSshOpts;
  }

  /**
   * Validates the current set of properties set on the PigExtension. Thrwos an exception if the
   * property vales cannot be validated.
   */
  void validateProperties() {
    if (!generateTasks) {
      throw new Exception("Method validateProperties called when generateTasks is false");
    }

    if (!pigCacheDir || !pigCommand) {
      String msg = "You must set the properties pigCacheDir and pigCommand in your .pigProperties file to generate Pig tasks";
      throw new Exception(msg);
    }

    if (dependencyConf != null && project.configurations.find { it.name == dependencyConf } == null) {
      String msg = "You set the property dependencyConf to ${dependencyConf} in your .pigProperties file, but no such configuration exists for the project";
      throw new Exception(msg);
    }

    if (remoteHostName) {
      if (!remoteCacheDir) {
        String msg = "If you set remoteHostName in your .pigProperties file, you must also set remoteCacheDir to generate Pig tasks";
        throw new Exception(msg);
      }
    }
  }
}
