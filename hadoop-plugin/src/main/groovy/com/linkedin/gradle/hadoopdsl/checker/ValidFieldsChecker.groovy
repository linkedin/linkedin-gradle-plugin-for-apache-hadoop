package com.linkedin.gradle.hadoopdsl.checker;

import com.linkedin.gradle.hadoopdsl.BaseStaticChecker;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;
import org.gradle.api.Project;

/**
 * The ValidFieldsChecker checks if the provided field values are valid.
 */
class ValidFieldsChecker extends BaseStaticChecker {

  /**
   * Constructor for the ValidFieldsChecker
   *
   * @param project The Gradle project
   */
  ValidFieldsChecker(Project project) {
    super(project)
  }

  @Override
  void visitJob(SparkJob job) {

    if (job.executorCores != null && job.executorCores <= 0) {
      project.logger.lifecycle("ValidFieldsChecker ERROR: Value of executorCores must be greater than 0");
      foundError = true;
    }

    if (job.numExecutors != null && job.numExecutors <= 0) {
      project.logger.lifecycle("ValidFieldsChecker ERROR: Value of numExecutors must be greater than 0");
      foundError = true;
    }

    if (job.driverMemory != null && !isValidMemory(job.driverMemory)) {
      project.logger.lifecycle("ValidFieldsChecker ERROR: driverMemory doesn't conform to a valid spark memory specifier. Memory should be specified in one of [b,k,kb,m,mb,g,gb,t,tb,p,pb]");
      foundError = true;
    }

    if (job.executorMemory != null && !isValidMemory(job.executorMemory)) {
      project.logger.lifecycle("ValidFieldsChecker ERROR: executorMemory doesn't conform to a valid spark memory specifier. Memory should be specified in one of [b,k,kb,m,mb,g,gb,t,tb,p,pb]");
      foundError = true;
    }
  }

  /**
   * Checks if the passed memory conforms to spark memory specifier.
   * @param memory The memory value to check
   * @return true if the passed memory is a valid memory otherwise returns false
   */
  boolean isValidMemory(String memory) {
    String memoryRegEx = /\d*(b|k|kb|m|mb|g|gb|t|tb|p|pb)/
    if (memory.matches(memoryRegEx)) {
      return true;
    }
    return false;
  }
}
