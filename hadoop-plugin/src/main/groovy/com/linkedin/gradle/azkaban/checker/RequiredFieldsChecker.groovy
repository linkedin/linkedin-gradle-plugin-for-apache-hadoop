/*
 * Copyright 2014 LinkedIn Corp.
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
package com.linkedin.gradle.azkaban.checker;

import com.linkedin.gradle.azkaban.AzkabanJob;
import com.linkedin.gradle.azkaban.AzkabanProperties;
import com.linkedin.gradle.azkaban.AzkabanWorkflow;
import com.linkedin.gradle.azkaban.BaseStaticChecker;
import com.linkedin.gradle.azkaban.CommandJob;
import com.linkedin.gradle.azkaban.HadoopJavaJob;
import com.linkedin.gradle.azkaban.HiveJob;
import com.linkedin.gradle.azkaban.JavaJob;
import com.linkedin.gradle.azkaban.JavaProcessJob;
import com.linkedin.gradle.azkaban.KafkaPushJob;
import com.linkedin.gradle.azkaban.NoOpJob;
import com.linkedin.gradle.azkaban.PigJob;
import com.linkedin.gradle.azkaban.VoldemortBuildPushJob;

import org.gradle.api.Project;

/**
 * Checks that all the required fields in the DSL are set.
 */
class RequiredFieldsChecker extends BaseStaticChecker {
  /**
   * Constructor for the RequiredFieldsChecker.
   *
   * @param project The Gradle project
   */
  RequiredFieldsChecker(Project project) {
    super(project);
  }

  @Override
  void visitAzkabanProperties(AzkabanProperties props) {
    if (props.properties.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker WARNING: Properties ${props.name} does not declare any properties. Nothing will be built for this properties object.");
    }
  }

  @Override
  void visitAzkabanJob(AzkabanJob job) {
    // AzkabanJob has no required fields
  }

  @Override
  void visitAzkabanJob(CommandJob job) {
    if (job.command == null || job.command.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: CommandJob ${job.name} must set command");
      foundError = true;
    }
  }

  @Override
  void visitAzkabanJob(HadoopJavaJob job) {
    if (job.jobClass == null || job.jobClass.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: HadoopJavaJob ${job.name} must set jobClass");
      foundError = true;
    }
  }

  @Override
  void visitAzkabanJob(HiveJob job) {
    boolean emptyQuery = job.query == null || job.query.isEmpty();
    boolean emptyQueryFile = job.queryFile == null || job.queryFile.isEmpty();
    if (emptyQuery && emptyQueryFile) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: HiveJob ${job.name} must set query or queryFile");
      foundError = true;
    }
    if (!emptyQuery && !emptyQueryFile) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: HiveJob ${job.name} sets both query and queryFile");
      foundError = true;
    }
  }

  @Override
  void visitAzkabanJob(JavaJob job) {
    if (job.jobClass == null || job.jobClass.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: JavaJob ${job.name} must set jobClass");
      foundError = true;
    }
  }

  @Override
  void visitAzkabanJob(JavaProcessJob job) {
    if (job.javaClass == null || job.javaClass.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: JavaProcessJob ${job.name} must set javaClass");
      foundError = true;
    }
  }

  @Override
  void visitAzkabanJob(KafkaPushJob job) {
    if (job.inputPath == null || job.inputPath.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: KafkaPushJob ${job.name} must set inputPath");
      foundError = true;
    }
    if (job.topic == null || job.topic.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: KafkaPushJob ${job.name} must set topic");
      foundError = true;
    }
  }

  @Override
  void visitAzkabanJob(NoOpJob job) {
    // NoOpJob has no required fields, but is only useful if it declares dependencies.
    if (job.dependencyNames.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker WARNING: NoOpJob ${job.name} does not declare any dependencies. It won't do anything.");
    }
  }

  @Override
  void visitAzkabanJob(PigJob job) {
    if (job.script == null || job.script.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: PigJob ${job.name} must set script");
      foundError = true;
    }
  }

  @Override
  void visitAzkabanJob(VoldemortBuildPushJob job) {
    boolean emptyStoreDesc = job.storeDesc == null || job.storeDesc.isEmpty();
    boolean emptyStoreName = job.storeName == null || job.storeName.isEmpty();
    boolean emptyStoreOwnr = job.storeOwners == null || job.storeOwners.isEmpty();
    boolean emptyInputPath = job.buildInputPath == null || job.buildInputPath.isEmpty();
    boolean emptyOutptPath = job.buildOutputPath == null || job.buildOutputPath.isEmpty();
    boolean emptyRepFactor = job.repFactor == null;

    if (emptyStoreDesc || emptyStoreName || emptyStoreOwnr || emptyInputPath || emptyOutptPath || emptyRepFactor) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: VoldemortBuildPushJob ${job.name} has the following required fields: storeDesc, storeName, storeOwners, buildInputPath, buildOutputPath, repFactor");
      foundError = true;
    }

    // Cannot be empty if isAvroData is true
    boolean emptyAvroKeyFd = job.avroKeyField == null || job.avroKeyField.isEmpty();
    boolean emptyAvroValFd = job.avroValueField == null || job.avroValueField.isEmpty();

    if (job.isAvroData == true && (emptyAvroKeyFd || emptyAvroValFd)) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: VoldemortBuildPushJob ${job.name} must set both avroKeyField and avroValueField when isAvroData is set to true");
      foundError = true;
    }

    // Print a warning if avroKeyField or avroValueField is set but isAvroData is false
    if (job.isAvroData == false && (!emptyAvroKeyFd || !emptyAvroValFd)) {
      project.logger.lifecycle("RequiredFieldsChecker WARNING: VoldemortBuildPushJob ${job.name} will not use avroKeyField and avroValueField since isAvroData is set to false");
    }
  }
}
