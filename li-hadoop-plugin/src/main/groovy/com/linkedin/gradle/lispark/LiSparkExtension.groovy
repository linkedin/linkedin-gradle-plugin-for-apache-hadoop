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
package com.linkedin.gradle.lispark;

import com.linkedin.gradle.lihadoop.LiHadoopProperties;
import com.linkedin.gradle.spark.SparkExtension;

import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to the SparkExtension class. In particular, this class sets
 * default property values necessary to run the Spark Plugin on our development Hadoop cluster.
 */
class LiSparkExtension extends SparkExtension {

  private static final String HLD_GATEWAY_HOME = LiHadoopProperties.get(LiHadoopProperties.HLD_GATEWAY_HOME)
  private static final String HLD_GATEWAY_NODE = LiHadoopProperties.get(LiHadoopProperties.HLD_GATEWAY_NODE)
  private static final String REMOTE_SPARK_COMMAND = LiHadoopProperties.get(LiHadoopProperties.REMOTE_SPARK_COMMAND)

  /**
   * Constructor for the LiSparkExtension.
   *
   * @param project The Gradle project
   */
  LiSparkExtension(Project project) {
    super(project);
    this.sparkCacheDir = "${System.getProperty('user.home')}/.hadoopPlugin";
    this.sparkCommand = REMOTE_SPARK_COMMAND;
    this.remoteHostName = HLD_GATEWAY_NODE;
    this.remoteCacheDir = "${HLD_GATEWAY_HOME}/${System.getProperty('user.name')}/.hadoopPlugin";
    this.remoteSshOpts = "-q -K";
  }
}
