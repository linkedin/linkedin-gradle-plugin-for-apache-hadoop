package com.linkedin.gradle.liazkaban

import com.linkedin.gradle.azkaban.AzkabanFactory;
import com.linkedin.gradle.azkaban.AzkabanWorkflow;
import com.linkedin.gradle.azkaban.NamedScope;

import org.gradle.api.Project;

class LiAzkabanFactory extends AzkabanFactory {
  @Override
  AzkabanWorkflow makeAzkabanWorkflow(String name, Project project, NamedScope nextLevel) {
    return new LiAzkabanWorkflow(name, project, nextLevel);
  }

  // Factory method to build PigLiJob that can be overridden by subclasses.
  PigLiJob makePigLiJob(String name) {
    return new PigLiJob(name);
  }
}
