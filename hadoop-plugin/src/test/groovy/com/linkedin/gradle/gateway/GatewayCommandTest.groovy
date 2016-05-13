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

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the GatewayCommand class.
 */
class GatewayCommandTest {
  Project project;
  GatewayCommand gateway;

  @Before
  void buildGateway() {
    project = ProjectBuilder.builder().withName("foobar").build();
    String localCacheDir = "~/.hadoopPlugin";
    String remoteCacheDir = "~/.hadoopPlugin";
    String remoteHostName = "theGatewayNode.linkedin.com";
    String remoteSshOpts = "";
    gateway = new GatewayCommand(project, localCacheDir, remoteCacheDir, remoteHostName, remoteSshOpts);
  }

  @Test
  void testGatewayCommandMethods() {
    assert("cd ~/.hadoopPlugin/${project.name}; testCommand" == gateway.buildCommandLocal("testCommand"));
    assert("ssh  -tt theGatewayNode.linkedin.com 'cd ~/.hadoopPlugin/${project.name}; testCommand'" == gateway.buildCommandRemote("testCommand"));
    assert("ssh  theGatewayNode.linkedin.com mkdir -p ~/.hadoopPlugin" == gateway.buildMkdirCommandRemote());
    assert("rsync -av ~/.hadoopPlugin/${project.name} -e \"ssh \" theGatewayNode.linkedin.com:~/.hadoopPlugin" == gateway.buildRsyncCommandRemote());
  }
}
