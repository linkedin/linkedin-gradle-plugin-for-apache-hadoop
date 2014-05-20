/**
 * This is a "lint-like" class that scans the user's DSL for any potential
 * problems.
 *
 * This tool is based on a presentation by twalker of his tool "Demeantor".
 */
class AzkabanLintChecker {

  boolean checkAzkabanExtension(AzkabanExtension azkaban) {
    boolean ok = true;

    // Show a warning if there are no jobs.
    if (azkaban.workflows.isEmpty()) {
      System.err.println("WARNING HogwartsChecker: no workflows added to the Azkaban extension")
    }

    azkaban.workflows.each() { workflow ->
      // We want to print error messages for every job, so don't short-circuit
      // the call.
      ok = checkAzkabanWorkflow(workflow) && ok
    }

    return ok;
  }

  boolean checkAzkabanWorkflow(AzkabanWorkflow workflow) {
    boolean ok = true;

//    // Show a warning if there is no final job.
//    if (workflow.jobs.isEmpty()) {
//      System.err.println("WARNING HogwartsChecker: no jobs added to the Azkaban extension")
//    }
//
//    // Check that no jobs have the same name.
//    Set<String> names = new HashSet<String>();
//    azkaban.jobs.each() { job ->
//      String jobName = job.name.trim().toUpperCase();
//      if (names.contains(jobName)) {
//        System.err.println("FAILED HogwartsChecker: more than one job has the name ${job.name}");
//        ok = false;
//      }
//      else {
//        names.add(jobName);
//      }
//    }
//
//    // Then check each job. Groovy has multi-methods, so it will do overload
//    // resolution based on the runtime type of the job.
//    azkaban.jobs.each() { job ->
//      // We want to print error messages for every job, so don't short-circuit
//      // the call to checkAzkabanJob.
//      ok = checkAzkabanJob(job) && ok
//    }

    return ok;
  }

  boolean checkAzkabanJob(AzkabanJob job) {
    return true;
  }

  boolean checkAzkabanJob(CommandJob job) {
    return true;
  }

  boolean checkAzkabanJob(HiveJob job) {
    return true;
  }

  boolean checkAzkabanJob(JavaJob job) {
    return true;
  }

  boolean checkAzkabanJob(JavaProcessJob job) {
    return true;
  }

  boolean checkAzkabanJob(PigJob job) {
    return true;
  }

  boolean checkAzkabanJob(VoldemortBuildPushJob job) {
    return true;
  }
}