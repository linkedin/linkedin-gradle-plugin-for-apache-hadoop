package com.linkedin.gradle.liazkaban;

import com.linkedin.gradle.azkaban.AzkabanWorkflow;
import com.linkedin.gradle.azkaban.NamedScope;

import org.gradle.api.Project;

class LiAzkabanWorkflow extends AzkabanWorkflow {

  LiAzkabanWorkflow(String name, Project project) {
    super(name, project);
  }

  LiAzkabanWorkflow(String name, Project project, NamedScope nextLevel) {
    super(name, project, nextLevel);
  }

  PigLiJob pigLiJob(String name, Closure configure) {
    return configureJob(new PigLiJob(name), configure);
  }
}
