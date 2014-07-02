package com.linkedin.gradle.hadoop;

import org.gradle.api.Project;

/**
 * PigExtension exposes properties to that allow the user to configure how
 * to run Pig scripts from the command line.
 */
class PigExtension {
  Project project;

  // The user must set this to true to cause Pig tasks to be generated
  boolean generateTasks = false;

  // Properties that can be set by the user
  String dependencyConf = "runtime";
  String pigCacheDir = ".hadoopPlugin";
  String pigCommand = "pig";
  String pigOptions;

  // Properties that must be set if the user is running Pig on a remote host
  String remoteHostName;
  String remoteCacheDir;
  String remoteShellCmd;

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
    remoteShellCmd = properties.containsKey("remoteShellCmd") ? properties.getProperty("remoteShellCmd") : remoteShellCmd;
  }

  void validateProperties() {
    if (!generateTasks) {
      throw new Exception("Method validateProperties called when generateTasks is false");
    }

    if (!dependencyConf || !pigCacheDir || !pigCommand) {
      String msg = "You must set the properties dependencyConf, pigCacheDir, and pigCommand to generate Pig tasks";
      throw new Exception(msg);
    }

    if (remoteHostName) {
      if (!remoteCacheDir || !remoteShellCmd) {
        String msg = "If you set remoteHostName, you must also set remoteCacheDir and remoteShellCmd to generate Pig tasks";
        throw new Exception(msg);
      }
    }
  }
}
