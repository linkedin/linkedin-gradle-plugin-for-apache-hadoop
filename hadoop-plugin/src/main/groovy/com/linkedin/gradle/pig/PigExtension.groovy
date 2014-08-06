package com.linkedin.gradle.pig;

import org.gradle.api.Project;

/**
 * PigExtension exposes properties to that allow the user to configure how
 * to run Pig scripts from the command line.
 *
 * NOTE: At first, I was planning to use the standard Gradle DSL to allow
 * users to configure this extension, but it is difficult to use the DSL
 * at configuration time to configure other tasks, so I moved the properties
 * to an explicit property file.
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

  PigExtension(Project project) {
    this.project = project;
  }

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
