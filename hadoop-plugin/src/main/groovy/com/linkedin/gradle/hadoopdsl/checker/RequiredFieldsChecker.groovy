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
import com.linkedin.gradle.hadoopdsl.BaseStaticChecker;
import com.linkedin.gradle.hadoopdsl.job.CommandJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopJavaJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopShellJob;
import com.linkedin.gradle.hadoopdsl.job.HdfsToEspressoJob;
import com.linkedin.gradle.hadoopdsl.job.HdfsToTeradataJob;
import com.linkedin.gradle.hadoopdsl.job.HdfsWaitJob;
import com.linkedin.gradle.hadoopdsl.job.HiveJob;
import com.linkedin.gradle.hadoopdsl.job.JavaJob;
import com.linkedin.gradle.hadoopdsl.job.JavaProcessJob;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.KafkaPushJob;
import com.linkedin.gradle.hadoopdsl.job.NoOpJob;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.hadoopdsl.job.PinotBuildAndPushJob;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;
import com.linkedin.gradle.hadoopdsl.job.SqlJob;
import com.linkedin.gradle.hadoopdsl.job.TableauJob;
import com.linkedin.gradle.hadoopdsl.job.TensorFlowJob;
import com.linkedin.gradle.hadoopdsl.job.TeradataToHdfsJob;
import com.linkedin.gradle.hadoopdsl.job.VenicePushJob;
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
    if ((job.topic == null || job.topic.isEmpty()) && !job.multiTopic) {
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
  void visitJob(PinotBuildAndPushJob job) {
    if (job.tableName == null || job.inputPath == null || job.pushLocation == null) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: PinotBuildAndPushJob ${job.name} must set tableName, inputPath, and pushLocation");
      foundError = true;
    }
  }

  @Override
  void visitJob(SparkJob job) {
    // executes is a required field for both java and python applications. For java, this is the jar file. For python, this is the py file.
    // uses is a conditional field. It's required for java applications and should NOT be used for python applications.

    if (job.executionTarget == null || job.executionTarget.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: SparkJob ${job.name} must set executes");
      foundError = true;
      return;
    }

    if (job.executionTarget.toLowerCase().endsWith(".jar")) {
      if (job.appClass == null || job.appClass.isEmpty()) {
        project.logger.lifecycle("RequiredFieldsChecker ERROR: SparkJob ${job.name} must set uses for Java application")
        foundError = true
      }
    }

    if (job.executionTarget.toLowerCase().endsWith(".py")) {
      if (job.appClass != null) {
        project.logger.lifecycle("RequiredFieldsChecker ERROR: SparkJob ${job.name} must not set uses for Python application")
        foundError = true
      }
    }
  }

  @Override
  void visitJob(VenicePushJob job) {
    boolean emptyAvroKeyField = job.avroKeyField == null || job.avroKeyField.isEmpty();
    boolean emptyAvroValueField = job.avroValueField == null || job.avroValueField.isEmpty();
    boolean emptyClusterName = job.clusterName == null || job.clusterName.isEmpty();
    boolean emptyInputPath = job.inputPath == null || job.inputPath.isEmpty();
    boolean emptyVeniceStoreName = job.veniceStoreName == null || job.veniceStoreName.isEmpty();

    if (emptyAvroKeyField || emptyAvroValueField || emptyClusterName || emptyInputPath || emptyVeniceStoreName) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: VenicePushJob ${job.name} must set avroKeyField, avroValueField, clusterName, inputPath, veniceStoreName");
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

    if (job.isAvroData && (emptyAvroKeyFd || emptyAvroValFd)) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: VoldemortBuildPushJob ${job.name} must set both avroKeyField and avroValueField when isAvroData is set to true");
      foundError = true;
    }

    // Print a warning if avroKeyField or avroValueField is set but isAvroData is false
    if (!job.isAvroData && (!emptyAvroKeyFd || !emptyAvroValFd)) {
      project.logger.lifecycle("RequiredFieldsChecker WARNING: VoldemortBuildPushJob ${job.name} will not use avroKeyField and avroValueField since isAvroData is set to false");
    }
  }

  @Override
  void visitJob(HdfsToTeradataJob job) {
    foundError |= validateNotEmpty(job, "hostName", job.hostName);
    foundError |= validateNotEmpty(job, "userId", job.userId);
    foundError |= validateNotEmpty(job, "targetTable", job.targetTable);
    foundError |= validateTeradataCredential(job, job.credentialName, job.encryptedCredential, job.cryptoKeyFilePath)
    validateSource(job);
  }

  @Override
  void visitJob(TableauJob job) {
    if (job.workbookName == null || job.workbookName.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: TableauJob ${job.name} must set workbookName");
      foundError = true;
    }
  }

  @Override
  void visitJob(TeradataToHdfsJob job) {
    foundError |= validateNotEmpty(job, "hostName", job.hostName);
    foundError |= validateNotEmpty(job, "userId", job.userId);
    foundError |= validateNotEmpty(job, "targetHdfsPath", job.targetHdfsPath);
    foundError |= validateTeradataCredential(job, job.credentialName, job.encryptedCredential, job.cryptoKeyFilePath)

    boolean isSourceExist = false;
    isSourceExist ^= job.sourceTable == null || job.sourceTable.isEmpty();
    isSourceExist ^= job.sourceQuery == null || job.sourceQuery.isEmpty();

    if (!isSourceExist) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: ${job.getClass().getSimpleName()} ${job.name} needs either sourceTable or sourceQuery defined");
      foundError = true;
    }

    boolean isAvroSchemaExist = false;
    isAvroSchemaExist ^= job.avroSchemaPath == null || job.avroSchemaPath.isEmpty();
    isAvroSchemaExist ^= job.avroSchemaInline == null || job.avroSchemaInline.isEmpty();

    if (!isAvroSchemaExist) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: ${job.getClass().getSimpleName()} ${job.name} needs either avroSchemaPath or avroSchemaInline defined");
      foundError = true;
    }
  }

  @Override
  void visitJob(HdfsToEspressoJob job) {
    foundError |= validateTrue(job, "qps should be a positive number", job.qps > 0);
    foundError |= validateNotEmpty(job, "sourceHdfsPath", job.sourceHdfsPath);
    foundError |= validateNotEmpty(job, "espressoEndpoint", job.espressoEndpoint);
    foundError |= validateNotEmpty(job, "espressoDatabaseName", job.espressoDatabaseName);
    foundError |= validateNotEmpty(job, "espressoTableName", job.espressoTableName);
    foundError |= validateNotEmpty(job, "espressoContentType", job.espressoContentType);
    foundError |= validateNotEmpty(job, "espressoKey", job.espressoKey);
    foundError |= validateNotEmpty(job, "espressoOperation", job.espressoOperation);
    foundError |= validateNotEmpty(job, "errorHdfsPath", job.errorHdfsPath);
  }

  @Override
  void visitJob(SqlJob job) {
    foundError |= validateNotEmpty(job, "jdbcDriverClass", job.jdbcDriverClass);
    foundError |= validateNotEmpty(job, "jdbcUrl", job.jdbcUrl);
    foundError |= validateNotEmpty(job, "jdbcUserId", job.jdbcUserId);
    foundError |= validateNotEmpty(job, "jdbcEncryptedCredential", job.jdbcEncryptedCredential);
    foundError |= validateNotEmpty(job, "jdbcCryptoKeyPath", job.jdbcCryptoKeyPath);
  }

  @Override
  void visitJob(HdfsWaitJob job) {
    boolean emptyDirPath = job.dirPath == null || job.dirPath.isEmpty();
    boolean emptyFreshness = job.freshness == null || job.freshness.isEmpty();
    boolean emptyTimeout = job.timeout == null || job.timeout.isEmpty();
    boolean emptyForceJobToFail = job.forceJobToFail == null;

    if (emptyDirPath || emptyFreshness || emptyTimeout || emptyForceJobToFail) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: HdfsWaitJob ${job.name} must set dirPath, freshness, timeout, and forceJobToFail");
      foundError = true;
    }
  }

  @Override
  void visitJob(TensorFlowJob job) {
    foundError |= validateNotEmpty(job, "taskCommand", job.taskCommand);
    foundError |= validateNotEmpty(job, "archive", job.archive);
  }

  /**
   * Validates the source for an HdfsToTeradataJob job. Checks the required fields based on whether
   * the source is Hive or an Avro file.
   *
   * @param job The HdfsToTeradataJob job to validate
   */
  void validateSource(HdfsToTeradataJob job) {
    // Check if the source is Hive
    if (!isEmpty(job.sourceHiveDatabase) || !isEmpty(job.sourceHiveTable)) {
      foundError |= validateNotEmpty(job, "sourceHiveDatabase", job.sourceHiveDatabase);
      foundError |= validateNotEmpty(job, "sourceHiveTable", job.sourceHiveTable);
      foundError |= validateTrue(job, "Avro source should have not defined, when Hive source is defined.",
          isEmpty(job.sourceHdfsPath) && isEmpty(job.avroSchemaPath) && isEmpty(job.avroSchemaInline));
      return;
    }

    // Check required fields for when the source is an Avro file
    boolean isAvroSchemaExist = false;
    isAvroSchemaExist ^= job.avroSchemaPath == null || job.avroSchemaPath.isEmpty();
    isAvroSchemaExist ^= job.avroSchemaInline == null || job.avroSchemaInline.isEmpty();

    if (!isAvroSchemaExist) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: ${job.getClass().getSimpleName()} ${job.name} needs either avroSchemaPath or avroSchemaInline defined");
      foundError = true;
    }

    foundError = validateNotEmpty(job, "sourceHdfsPath", job.sourceHdfsPath);
  }

  boolean validateTeradataCredential(
      Job job,
      String credentialName,
      String encryptedCredential,
      String cryptoKeyFilePath) {

    boolean isCredentialExist = false;
    isCredentialExist ^= credentialName == null || credentialName.isEmpty();
    isCredentialExist ^= encryptedCredential == null || encryptedCredential.isEmpty();

    if (!isCredentialExist) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: ${job.getClass().getSimpleName()} ${job.name} needs either credentialName or encryptedCredential defined");
      return true;
    }

    if (encryptedCredential != null && !encryptedCredential.isEmpty()) {
      return validateNotEmpty(job, "cryptoKeyFilePath", cryptoKeyFilePath);
    }
    return false;
  }

  boolean validateNotEmpty(Job job, String name, String val) {
    if (val == null || val.isEmpty()) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: ${job.getClass().getSimpleName()} ${job.name} has the following required fields: ${name}");
      return true;
    }
    return false;
  }

  boolean validateTrue(Job job, String message, boolean condition) {
    if (!condition) {
      project.logger.lifecycle("RequiredFieldsChecker ERROR: ${job.getClass().getSimpleName()} ${job.name} has the following error: ${message}");
      return true;
    }
    return false;
  }

  boolean isEmpty(String s) {
    return s == null || s.isEmpty();
  }
}
