package com.linkedin.gradle.hadoop;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * HadoopPlugin is the class that implements our Gradle Plugin.
 */
class HadoopPlugin implements Plugin<Project> {

  void apply(Project project) {
    project.getPlugins().apply(AzkabanPlugin.class);
    project.getPlugins().apply(PigPlugin.class);
  }
}