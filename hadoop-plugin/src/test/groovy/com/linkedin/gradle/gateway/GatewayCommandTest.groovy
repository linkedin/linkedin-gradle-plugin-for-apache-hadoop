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
import org.junit.Test;

/**
 * Unit tests for the GatewayCommand class.
 */
class GatewayCommandTest {

  GatewayCommand buildGateway() {
    Project project = ProjectBuilder.builder().build();
    project.name = "foobar"
    String localCacheDir = "~/.hadoopPlugin";
    String remoteCacheDir = "~/.hadoopPlugin";
    String remoteHostName = "eat1-nertzaz03.grid.linkedin.com";
    String remoteSshOpts = "";
    return new GatewayCommand(project, localCacheDir, remoteCacheDir, remoteHostName, remoteSshOpts);
  }

  @Test
  public void testGatewayCommandMethods() {
    GatewayCommand gateay = buildGateway();
    assert("cd ~/.hadoopPlugin; testCommand" == gateway.buildLocalCommand("testCommand"));
    assert("ssh  -tt  't1-nertzaz03.grid.linkedin.com 'cd ~/.hadoopPlugin; testCommand'" == gateway.buildMkdirCommandRemote("testCommand"));
    assert("rsync -av ~/.hadoopPlugin/foobar -e \"ssh \" eat1-nertzaz03.grid.linkedin.com:~/.hadoopPlugin" == gateway.buildRsyncCommandRemote());
  }
}
