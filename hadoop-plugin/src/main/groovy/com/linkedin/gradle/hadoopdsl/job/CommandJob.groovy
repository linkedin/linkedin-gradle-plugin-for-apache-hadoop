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
package com.linkedin.gradle.hadoopdsl.job;

/**
 * Job class for type=command jobs.
 * <p>
 * In the DSL, a CommandJob can be specified with:
 * <pre>
 *   def commands = ['echo "hello"', 'echo "This is how one runs a command job"', 'whoami']
 *
 *   commandJob('jobName') {
 *     uses 'echo "hello world"'  // Exactly one of uses or usesCommands is required
 *     usesCommands commands      // Exactly one of uses or usesCommands is required
 *   }
 * </pre>
 */
class CommandJob extends Job {
  String command;
  List<String> commands;

  /**
   * Constructor for a CommandJob.
   *
   * @param jobName The job name
   */
  CommandJob(String jobName) {
    super(jobName);
    setJobProperty("type", "command");
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and jobProperties correctly.
   *
   * @param allProperties The job properties map that holds all the job properties that will go into the built job file
   * @return The input job properties map, with jobProperties and jvmProperties added
   */
  @Override
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    super.buildProperties(allProperties);

    if (commands != null && commands.size() > 0) {
      allProperties["command"] = commands.get(0);

      for (int i = 1; i < commands.size(); i++) {
        allProperties["command.${i}"] = commands.get(i);
      }
    }

    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  CommandJob clone() {
    return clone(new CommandJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  @Override
  CommandJob clone(CommandJob cloneJob) {
    cloneJob.command = command;
    cloneJob.commands = commands;
    return super.clone(cloneJob);
  }

  /**
   * DSL method uses specifies the command for the job. This method causes the property
   * command=value to be added the job.
   * <p>
   * Only one of the methods uses or usesCommands can be specified with a CommandJob.
   *
   * @param command The command for the job
   */
  void uses(String command) {
    this.command = command;
    setJobProperty("command", command);
  }

  /**
   * DSL method usesCommands specifies the commands for the job. This method causes the properties
   * command.1=value1, command.2=value2, etc. to be added the job.
   * <p>
   * Only one of the methods uses or usesCommands can be specified with a CommandJob.
   *
   * @param command The command for the job
   */
  void usesCommands(List<String> commands) {
    this.commands = commands;
  }
}