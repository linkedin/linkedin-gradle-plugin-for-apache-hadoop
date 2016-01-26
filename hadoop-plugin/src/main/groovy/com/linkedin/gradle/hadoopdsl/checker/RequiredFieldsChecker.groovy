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
package com.linkedin.gradle.hadoopdsl.checker;

import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.BaseStaticChecker;
import com.linkedin.gradle.hadoopdsl.job.CommandJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopJavaJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopShellJob;
import com.linkedin.gradle.hadoopdsl.job.HiveJob;
import com.linkedin.gradle.hadoopdsl.job.JavaJob;
import com.linkedin.gradle.hadoopdsl.job.JavaProcessJob;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.KafkaPushJob;
import com.linkedin.gradle.hadoopdsl.job.NoOpJob;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;
import com.linkedin.gradle.hadoopdsl.job.VoldemortBuildPushJob;

import org.gradle.api.Project;

/**
 * Checks that all the required fields in the DSL are set.
 */
@SuppressWarnings("deprecation")
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
  void visitProperties(Properties props) {
    if (props.basePropertySetName == null && props.confProperties.isEmpty() && props.jobProperties.isEmpty() && props.jvmProperties.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker WARNING: Properties ${props.name} does not set any confProperties, jobProperties, jvmProperties or basePropertySetName. Nothing will be built for this properties object.");
    }
  }

  @Override
  void visitJob(Job job) {
    // Job has no required fields
  }

  @Override
  void visitJob(CommandJob job) {
    boolean emptyCommand = job.command == null || job.command.isEmpty();
    boolean emptyCommands = job.commands == null || job.commands.isEmpty();
    if (emptyCommand && emptyCommands) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: CommandJob ${job.name} must set command or commands");
      foundError = true;
    }
    if (!emptyCommand && !emptyCommands) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: CommandJob ${job.name} sets both command and commands");
      foundError = true;
    }
  }

  @Override
  void visitJob(HadoopJavaJob job) {
    if (job.jobClass == null || job.jobClass.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: HadoopJavaJob ${job.name} must set jobClass");
      foundError = true;
    }
  }

  @Override
  void visitJob(HadoopShellJob job) {
    boolean emptyCommand = job.command == null || job.command.isEmpty();
    boolean emptyCommands = job.commands == null || job.commands.isEmpty();
    if (emptyCommand && emptyCommands) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: HadoopShellJob ${job.name} must set command or commands");
      foundError = true;
    }
    if (!emptyCommand && !emptyCommands) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: HadoopShellJob ${job.name} sets both command and commands");
      foundError = true;
    }
  }

  @Override
  void visitJob(HiveJob job) {
    if (job.script == null || job.script.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: HiveJob ${job.name} must set script");
      foundError = true;
    }
  }

  @Override
  void visitJob(JavaJob job) {
    if (job.jobClass == null || job.jobClass.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: JavaJob ${job.name} must set jobClass");
      foundError = true;
    }
  }

  @Override
  void visitJob(JavaProcessJob job) {
    if (job.javaClass == null || job.javaClass.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: JavaProcessJob ${job.name} must set javaClass");
      foundError = true;
    }
  }

  @Override
  void visitJob(KafkaPushJob job) {
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
  void visitJob(NoOpJob job) {
    // NoOpJob has no required fields, but is only useful if it declares dependencies.
    if (job.dependencyNames.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker WARNING: NoOpJob ${job.name} does not declare any dependencies. It won't do anything.");
    }
  }

  @Override
  void visitJob(PigJob job) {
    if (job.script == null || job.script.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: PigJob ${job.name} must set script");
      foundError = true;
    }
  }

  @Override
  void visitJob(SparkJob job) {
    if(job.appClass == null || job.appClass.isEmpty() || job.executionJar == null || job.executionJar.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: SparkJob ${job.name} must set uses and executes");
      foundError = true;
    }
  }

  @Override
  void visitJob(VoldemortBuildPushJob job) {
    boolean emptyStoreName = job.storeName == null || job.storeName.isEmpty();
    boolean emptyClustName = job.clusterName == null || job.clusterName.isEmpty();
    boolean emptyInputPath = job.buildInputPath == null || job.buildInputPath.isEmpty();
    boolean emptyOutptPath = job.buildOutputPath == null || job.buildOutputPath.isEmpty();
    boolean emptyStoreOwnr = job.storeOwners == null || job.storeOwners.isEmpty();
    boolean emptyStoreDesc = job.storeDesc == null || job.storeDesc.isEmpty();

    if (emptyStoreName || emptyClustName || emptyInputPath || emptyOutptPath || emptyStoreOwnr || emptyStoreDesc) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: VoldemortBuildPushJob ${job.name} has the following required fields: storeName, clusterName, buildInputPath, buildOutputPath, storeOwners, storeDesc");
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
