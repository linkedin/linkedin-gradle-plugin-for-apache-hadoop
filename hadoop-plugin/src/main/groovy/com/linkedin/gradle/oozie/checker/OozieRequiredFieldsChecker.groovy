/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.gradle.oozie.checker;

import com.linkedin.gradle.hadoopdsl.checker.RequiredFieldsChecker;
import com.linkedin.gradle.hadoopdsl.job.HadoopJavaJob;
import com.linkedin.gradle.hadoopdsl.job.HiveJob;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;
import org.gradle.api.Project;


/**
 * Oozie specific checker for the required fields.
 */
class OozieRequiredFieldsChecker extends RequiredFieldsChecker {

  /**
   * Constructor for the RequiredFieldsChecker.
   *
   * @param project The Gradle project
   */
  OozieRequiredFieldsChecker(Project project) {
    super(project);
  }

  // Print a warning if mapClass or reduceClass is not set. User can set it in jobConf properties.
  @Override
  void visitJob(HadoopJavaJob job) {
    if ((job.mapClass == null || job.mapClass.isEmpty()) && (job.reduceClass == null || job.reduceClass.isEmpty())) {
      project.logger.lifecycle("RequiredFieldsChecker WARNING: HadoopJavaJob ${job.name} hasn't set either of mapClass or reduceCLass. They should be set in jobConf properties now");
    }
  }


  @Override
  void visitJob(SparkJob job) {
    if (job.appClass == null || job.appClass.isEmpty() || job.executionTarget == null || job.executionTarget.isEmpty() || !job.jobProperties.containsKey("master") || !job.jobProperties.containsKey("deploy-mode")) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: SparkJob ${job.name} must set uses, executes and should set master and deploy-mode in the properties");
      foundError = true;
    }
  }

  // For oozie, jobXml is also required which contains the required settings to contact metastore for hive.
  @Override
  void visitJob(HiveJob job) {
    if (job.script == null || job.script.isEmpty() || !job.jobProperties.containsKey("jobXml")) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: HiveJob ${job.name} must set script and set jobXml in the properties");
      foundError = true;
    }
  }
}
