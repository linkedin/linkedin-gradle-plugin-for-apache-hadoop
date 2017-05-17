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
package com.linkedin.gradle.lipig;

import com.linkedin.gradle.lihadoop.LiHadoopProperties;
import com.linkedin.gradle.pig.PigExtension;

import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to the PigExtension class. In particular, this class sets
 * default property values necessary to run the Pig Plugin on our development Hadoop cluster.
 */
class LiPigExtension extends PigExtension {

  private static final String HLD_GATEWAY_HOME = LiHadoopProperties.get(LiHadoopProperties.HLD_GATEWAY_HOME)
  private static final String HLD_GATEWAY_NODE = LiHadoopProperties.get(LiHadoopProperties.HLD_GATEWAY_NODE)
  private static final String REMOTE_PIG_COMMAND = LiHadoopProperties.get(LiHadoopProperties.REMOTE_PIG_COMMAND)

  /**
   * Constructor for the LiPigExtension.
   *
   * @param project The Gradle project
   */
  LiPigExtension(Project project) {
    super(project);

    // LinkedIn-specific properties to run on the gateway nodes
    if (project.configurations.find { it.name == "azkabanRuntime" } != null) {
      this.dependencyConf = "azkabanRuntime";
    }
    else if (project.configurations.find { it.name == "runtime" } != null) {
      this.dependencyConf = "runtime";
    }

    this.pigCacheDir = "${System.getProperty('user.home')}/.hadoopPlugin";
    this.pigCommand = REMOTE_PIG_COMMAND;
    this.remoteHostName = HLD_GATEWAY_NODE;
    this.remoteCacheDir = "${HLD_GATEWAY_HOME}/${System.getProperty('user.name')}/.hadoopPlugin";
    this.remoteSshOpts = "-q -K";
  }
}
