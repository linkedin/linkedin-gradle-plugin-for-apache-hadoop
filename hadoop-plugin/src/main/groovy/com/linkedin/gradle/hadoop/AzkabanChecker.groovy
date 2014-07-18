package com.linkedin.gradle.hadoop;

/**
 * This is a "lint-like" class that scans the user's DSL for any potential
 * problems.
 *
 * This tool is based on a presentation by twalker of his tool "Demeantor".
 */
class AzkabanChecker {

  boolean checkAzkabanExtension(AzkabanExtension azkaban) {
    boolean ok = true;

    azkaban.workflows.each() { workflow ->
      // We want to print error messages for every job, so don't short-circuit the call.
      ok = checkAzkabanWorkflow(workflow) && ok
    }

    return ok;
  }

  boolean checkAzkabanWorkflow(AzkabanWorkflow workflow) {
    boolean ok = true;

    // Then check each job. Groovy has multi-methods, so it will do overload
    // resolution based on the runtime type of the job.
    workflow.jobs.each() { job ->
      // We want to print error messages for every job, so don't short-circuit the call.
      ok = checkAzkabanJob(job) && ok
    }

    return ok;
  }

  boolean checkAzkabanJob(AzkabanJob job) {
    return true;
  }

  boolean checkAzkabanJob(CommandJob job) {
    if (job.command == null || job.command.isEmpty()) {
      System.err.println("AzkabanDslChecker ERROR: CommandJob ${job.name} must set command");
      return false;
    }
    return true;
  }

  boolean checkAzkabanJob(HiveJob job) {
    boolean emptyQuery = job.query == null || job.query.isEmpty();
    boolean emptyQueryFile = job.queryFile == null || job.queryFile.isEmpty();
    if (emptyQuery && emptyQueryFile) {
      System.err.println("AzkabanDslChecker ERROR: HiveJob ${job.name} must set query or queryFile");
      return false;
    }
    if (!emptyQuery && !emptyQueryFile) {
      System.err.println("AzkabanDslChecker ERROR: HiveJob ${job.name} sets both query and queryFile");
      return false;
    }
    return true;
  }

  boolean checkAzkabanJob(JavaJob job) {
    if (job.jobClass == null || job.jobClass.isEmpty()) {
      System.err.println("AzkabanDslChecker ERROR: JavaJob ${job.name} must set jobClass");
      return false;
    }
    return true;
  }

  boolean checkAzkabanJob(JavaProcessJob job) {
    if (job.javaClass == null || job.javaClass.isEmpty()) {
      System.err.println("AzkabanDslChecker ERROR: JavaProcessJob ${job.name} must set javaClass");
      return false;
    }
    return true;
  }

  boolean checkAzkabanJob(KafkaPushJob job) {
    if (job.inputPath == null || job.inputPath.isEmpty()) {
      System.err.println("AzkabanDslChecker ERROR: KafkaPushJob ${job.name} must set inputPath");
      return false;
    }
    if (job.topic == null || job.topic.isEmpty()) {
      System.err.println("AzkabanDslChecker ERROR: KafkaPushJob ${job.name} must set topic");
      return false;
    }
    return true;
  }

  boolean checkAzkabanJob(NoOpJob job) {
    return true;
  }

  boolean checkAzkabanJob(PigJob job) {
    if (job.script == null || job.script.isEmpty()) {
      System.err.println("AzkabanDslChecker ERROR: PigJob ${job.name} must set script");
      return false;
    }
    return true;
  }

  boolean checkAzkabanJob(VoldemortBuildPushJob job) {
    boolean emptyStoreDesc = job.storeDesc == null || job.storeDesc.isEmpty();
    boolean emptyStoreName = job.storeName == null || job.storeName.isEmpty();
    boolean emptyStoreOwnr = job.storeOwners == null || job.storeOwners.isEmpty();
    boolean emptyInputPath = job.buildInputPath == null || job.buildInputPath.isEmpty();
    boolean emptyOutptPath = job.buildOutputPath == null || job.buildOutputPath.isEmpty();
    boolean emptyRepFactor = job.repFactor == null;

    if (emptyStoreDesc || emptyStoreName || emptyStoreOwnr || emptyInputPath || emptyOutptPath || emptyRepFactor) {
      System.err.println("AzkabanDslChecker ERROR: VoldemortBuildPushJob ${job.name} has the following required fields: storeDesc, storeName, storeOwners, buildInputPath, buildOutputPath, repFactor");
      return false;
    }

    // Cannot be empty if isAvroData is true
    boolean emptyAvroKeyFd = job.avroKeyField == null || job.avroKeyField.isEmpty();
    boolean emptyAvroValFd = job.avroValueField == null || job.avroValueField.isEmpty();

    if (job.isAvroData == true && (emptyAvroKeyFd || emptyAvroValFd)) {
      System.err.println("AzkabanDslChecker ERROR: VoldemortBuildPushJob ${job.name} must set both avroKeyField and avroValueField when isAvroData is set to true");
      return false;
    }

    // Print a warning if avroKeyField or avroValueField is set but isAvroData is false
    if (job.isAvroData == false && (!emptyAvroKeyFd || !emptyAvroValFd)) {
      System.out.println("AzkabanDslChecker WARNING: VoldemortBuildPushJob ${job.name} will not use avroKeyField and avroValueField since isAvroData is set to false");
    }

    return true;
  }
}