package com.linkedin.gradle.hadoopdsl.job

/**
 * HadoopShell is a Hadoop security enabled "command" jobtype. This jobtype
 * adheres to same format and other details as "command" jobtype.
 *
 * Job class for type=hadoopShell jobs.
 * <p>
 * In the DSL, a HadoopShellJob can be specified with:
 * <pre>
 *   def commands = ['echo "hello"', 'echo "This is how one runs a command job"', 'whoami']
 *
 *   hadoopShellJob('jobName') {
 *     uses 'echo "hello world"'  // Exactly one of uses or usesCommands is required
 *     usesCommands commands      // Exactly one of uses or usesCommands is required
 *   }
 * </pre>
 */
class HadoopShellJob extends CommandJob {
  /**
   * Constructor for a HadoopShellJob.
   *
   * @param jobName The job name
   */
  HadoopShellJob(String jobName) {
    super(jobName);
    setJobProperty("type", "hadoopShell");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  HadoopShellJob clone() {
    return clone(new HadoopShellJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  HadoopShellJob clone(HadoopShellJob cloneJob) {
    cloneJob.command = command;
    cloneJob.commands = commands;
    return super.clone(cloneJob);
  }
}
