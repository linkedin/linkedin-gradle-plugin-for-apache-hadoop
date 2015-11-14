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
package com.linkedin.gradle.gateway;

import java.io.File;

import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.Exec;

class GatewayCommand {
  Project project;
  String localCacheDir;
  String remoteCacheDir;
  String remoteHostName;
  String remoteSshOpts;

  /**
   * Constructor method for GatewayCommand.
   *
   * @param project The Gradle project
   * @param localCacheDir The local project cache directory
   * @param remoteCacheDir The remote project cache directory
   * @param remoteHostName The remote host name
   * @param remoteSshOpts Extra options to use while ssh'ing into the remote host
   */
  GatewayCommand(Project project, String localCacheDir, String remoteCacheDir, String remoteHostName, String remoteSshOpts) {
    this.localCacheDir = localCacheDir;
    this.project = project;
    this.remoteCacheDir = remoteCacheDir;
    this.remoteHostName = remoteHostName;
    this.remoteSshOpts = remoteSshOpts;
  }

  /**
   * Method to build the Bash command that invokes a command on the local host from the local cache
   * directory. Subclasses can override this method to build their own command.
   *
   * @param localCommand The command to be invoked on the local host in the local cache directory
   * @return Command to be invoked
   */
  String buildCommandLocal(String localCommand) {
    String localProjectDir = "${localCacheDir}/${project.name}";
    return "cd ${localProjectDir}; ${localCommand}";
  }

  /**
   * Method to build the SSH command that invokes a command on the remote host from the remote
   * cache directory. Subclasses can override this method to build their own command.
   *
   * @param remoteCommand The command to be invoked on the remote host in the remote cache directory
   * @return Command to be invoked
   */
  String buildCommandRemote(String remoteCommand) {
    String remoteProjectDir = "${remoteCacheDir}/${project.name}";
    return "ssh ${remoteSshOpts} -tt ${remoteHostName} 'cd ${remoteProjectDir}; ${remoteCommand}'";
  }

  /**
   * Method to build the command to create the cache directory on the remote host. Subclasses can
   * override this method to provide their own command.
   *
   * @return Command that creates the cache directory on the remote host
   */
  String buildMkdirCommandRemote() {
    return "ssh ${remoteSshOpts} ${remoteHostName} mkdir -p ${remoteCacheDir}";
  }

  /**
   * Method to build the command to rsync the project directory to the remote host's cache
   * directory. Subclasses can override this method to provide their own command.
   *
   * @return Command that rsyncs the project directory to the remote host's cache directory
   */
  String buildRsyncCommandRemote() {
    String localProjectDir = "${localCacheDir}/${project.name}";
    return "rsync -av ${localProjectDir} -e \"ssh ${remoteSshOpts}\" ${remoteHostName}:${remoteCacheDir}";
  }

  /**
   * Writes out the shell script that will run the given local command.
   *
   * @param scriptName The name of the corresponding Gradle task for this execution of the script
   * @param localCommand The command to be executed by the script
   */
  void writeExecScriptLocal(String scriptName, String localCommand) {
    String localProjectDir = "${localCacheDir}/${project.name}";

    new File("${localProjectDir}/run_${scriptName}.sh").withWriter { out ->
      out.writeLine("#!/usr/bin/env bash");
      out.writeLine("echo ====================");
      out.writeLine("echo Running the script ${localProjectDir}/run_${scriptName}.sh");
      out.writeLine("echo Executing ${localCommand} on the local host");
      out.writeLine(buildCommandLocal(localCommand));
    }
  }

  /**
   * Writes out the shell script that will run the given remote command.
   *
   * @param scriptName The name of the corresponding Gradle task for this execution of the script
   * @param remoteCommand The command to be executed by the script
   */
  void writeExecScriptRemote(String scriptName, String remoteCommand) {
    String localProjectDir = "${localCacheDir}/${project.name}";
    String remoteProjectDir = "${remoteCacheDir}/${project.name}";

    new File("${localProjectDir}/run_${scriptName}.sh").withWriter { out ->
      out.writeLine("#!/usr/bin/env bash");
      out.writeLine("echo ====================");
      out.writeLine("echo Running the script ${localProjectDir}/run_${scriptName}.sh");
      out.writeLine("echo Creating directory ${remoteCacheDir} on host ${remoteHostName}");
      out.writeLine(buildMkdirCommandRemote());
      out.writeLine("echo Syncing local directory ${localProjectDir} to ${remoteCacheDir} on host ${remoteHostName}");
      out.writeLine(buildRsyncCommandRemote());
      out.writeLine("echo Executing '${remoteCommand}' on host ${remoteHostName}");
      out.writeLine(buildCommandRemote(remoteCommand));
    }
  }
}