package com.linkedin.gradle.lipig;

import com.linkedin.gradle.pig.PigExtension;
import com.linkedin.gradle.pig.PigPlugin;

import org.gradle.api.Project;

class LiPigPlugin extends PigPlugin {
  @Override
  PigExtension makePigExtension(Project project) {
    return new LiPigExtension(project);
  }
}
