package com.linkedin.gradle.liazkaban;

import com.linkedin.gradle.azkaban.AzkabanFactory;
import com.linkedin.gradle.azkaban.AzkabanPlugin;

import org.gradle.api.Project;

class LiAzkabanPlugin extends AzkabanPlugin {
  @Override
  void applyTo(Project project) {
    super.applyTo(project);
    project.extensions.add("pigLiJob", this.&pigLiJob);
  }

  @Override
  AzkabanFactory makeAzkabanFactory() {
    return new LiAzkabanFactory();
  }

  PigLiJob pigLiJob(String name, Closure configure) {
    return configureJob(new PigLiJob(name), configure);
  }
}
