package com.linkedin.gradle.lipig;

import com.linkedin.gradle.pig.PigExtension;

import org.gradle.api.Project;

class LiPigExtension extends PigExtension {

  LiPigExtension(Project project) {
    super(project);

    // LinkedIn-specific properties to run on the Magic gateway
    if (project.configurations.find { it.name == "azkabanRuntime" } != null) {
      this.dependencyConf = "azkabanRuntime";
    }
    else if (project.configurations.find { it.name == "runtime" } != null) {
      this.dependencyConf = "runtime";
    }

    this.pigCacheDir = "${System.getProperty('user.home')}/.hadoopPlugin";
    this.pigCommand = "/export/apps/pig/latest/bin/pig";
    this.remoteHostName = "eat1-magicgw01.grid.linkedin.com";
    this.remoteCacheDir = "/export/home/${System.getProperty('user.name')}/.hadoopPlugin";
    this.remoteSshOpts = "-q -K";
  }
}