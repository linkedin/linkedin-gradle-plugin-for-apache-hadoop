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
package com.linkedin.gradle.spark;

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

class TestSparkPlugin {
  String executionJar;
  String appClass;
  Map<String, Object> confs;
  Set<String> flags;
  List<String> appParams;
  Map<String, Object> properties;

  @Before
  void setup() {
    executionJar = "execution-jar";
    appClass = "com.linkedin.foo"
    flags = ["version", "verbose"];
    appParams = ["param1", "param2"];
    properties = new TreeMap<String, Object>();
    confs = new TreeMap<String, Object>();

    confs.put("key1", "value1")
    confs.put("key2", "value2")
    confs.put("key3", "value3")

    properties.put("master", "yarn-cluster");
    properties.put("jars", "jar1,jar2,jar3")
  }

  @Test
  void testBuildLocalSparkCommand() {
    SparkPlugin plugin = new SparkTestPlugin();
    Project project = ProjectBuilder.builder().build();
    project.tasks.create("buildHadoopZips") {
      dependsOn "startHadoopZips"
      description = "Builds all of the Hadoop zip archives";
      group = "Hadoop Plugin";
    }
    plugin.apply(project);
    String actual = plugin.buildLocalSparkCmd(executionJar, appClass, confs, flags, appParams, properties);
    String expected = "cd .hadoopPlugin/test; spark-submit --jars jar1,jar2,jar3 --master yarn-cluster  --conf key1=value1 --conf key2=value2 --conf key3=value3 --version --verbose --class com.linkedin.foo execution-jar param1 param2";
    Assert.assertEquals(expected, actual);
  }

  @Test
  void testBuildRemoteSparkCommand() {
    SparkPlugin plugin = new SparkTestPlugin();
    Project project = ProjectBuilder.builder().build();
    project.tasks.create("buildHadoopZips") {
      dependsOn "startHadoopZips"
      description = "Builds all of the Hadoop zip archives";
      group = "Hadoop Plugin";
    }
    plugin.apply(project);
    String actual = plugin.buildRemoteSparkCmd(executionJar, appClass, confs, flags, appParams, properties);
    String expected = "ssh remotesshoption -tt remoteHost 'cd remotecachedir/test; spark-submit --jars jar1,jar2,jar3 --master yarn-cluster  --conf key1=value1 --conf key2=value2 --conf key3=value3 --version --verbose --class com.linkedin.foo execution-jar param1 param2'"
    Assert.assertEquals(expected, actual);
  }

  @Test
  void buildRemoteRsyncCommand() {
    SparkPlugin plugin = new SparkTestPlugin();
    Project project = ProjectBuilder.builder().build();
    project.tasks.create("buildHadoopZips") {
      dependsOn "startHadoopZips"
      description = "Builds all of the Hadoop zip archives";
      group = "Hadoop Plugin";
    }
    plugin.apply(project);
    String actual = plugin.buildRemoteRsyncCmd();
    String expected = "rsync -av .hadoopPlugin/test -e \"ssh remotesshoption\" remoteHost:remotecachedir";
    Assert.assertEquals(expected, actual);
  }
}

/**
 * A test class which extends the SparkPlugin
 */
class SparkTestPlugin extends SparkPlugin {
  /**
   * Override the makeSparkExtension method to return custom sparkExtension.
   * @param project Project
   * @return The created SparkExtension
   */
  @Override
  SparkExtension makeSparkExtension(Project project) {
    SparkExtension sparkExtension = new SparkExtension();
    sparkExtension.remoteHostName = "remoteHost"
    sparkExtension.remoteSshOpts = "remotesshoption";
    sparkExtension.remoteCacheDir = "remotecachedir";
    return sparkExtension;
  }
}