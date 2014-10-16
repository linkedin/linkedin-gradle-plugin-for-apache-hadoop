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
package com.linkedin.gradle.lipig;

import com.linkedin.gradle.pig.PigExtension;
import com.linkedin.gradle.pig.PigPlugin;
import com.linkedin.gradle.pig.PigTaskHelper;

import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to the Pig Plugin.
 */
class LiPigPlugin extends PigPlugin {
  /**
   * Returns the LinkedIn-specific Pig extension. Can be overridden by subclasses.
   *
   * @param project The Gradle project
   * @return The PigExtension object to use for the PigPlugin
   */
  @Override
  PigExtension makePigExtension(Project project) {
    return new LiPigExtension(project);
  }

  /**
   * Builds the LinkedIn-specifc command that invokes Pig on the remote host. Subclasses can
   * override this method to provide their own command.
   *
   * @param relFilePath The relative path to the Pig script file
   * @param parameters The Pig parameters
   * @param jvmProperties The JVM properties
   * @return Command that invokes Pig on the remote host
   */
  @Override
  String buildRemotePigCmd(String relFilePath, Map<String, String> parameters, Map<String, String> jvmProperties) {
    String pigCommand = pigExtension.pigCommand;
    String pigOptions = pigExtension.pigOptions ?: "";
    String pigParams = parameters == null ? "" : PigTaskHelper.buildPigParameters(parameters);
    String jvmParams = jvmProperties == null ? "" : PigTaskHelper.buildJvmParameters(jvmProperties);
    String remoteHostName = pigExtension.remoteHostName;
    String remoteSshOpts = pigExtension.remoteSshOpts;
    String remoteCacheDir = pigExtension.remoteCacheDir;
    String remoteProjDir = "${remoteCacheDir}/${project.name}";
    return "ssh ${remoteSshOpts} -tt ${remoteHostName} 'export PIG_UDFS=${remoteProjDir}; cd ${remoteProjDir}; ${pigCommand} ${jvmParams} ${pigOptions} -f ${relFilePath} ${pigParams}'";
  }
}