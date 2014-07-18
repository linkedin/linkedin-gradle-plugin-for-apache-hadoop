package com.linkedin.gradle.hadoop;

import com.linkedin.gradle.azkaban.AzkabanPlugin;
import com.linkedin.gradle.pig.PigPlugin;

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