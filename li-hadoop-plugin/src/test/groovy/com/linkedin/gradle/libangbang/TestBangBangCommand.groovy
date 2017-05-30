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
package com.linkedin.gradle.libangbang;

import org.junit.Assert;
import org.junit.Test;

class TestBangBangCommand {
  @Test
  void testBangBangCommandString() {
    Map<String,String> bangbangConfs = new HashMap<String, String>();
    bangbangConfs.put("gradle.build.dir","dir1");
    bangbangConfs.put("gradle.installation.dir","dir2");
    String actualCommand = new BangBangCommand.Builder().setGradleFile("abc.gradle").setTasks(['runShell']).setConfs(bangbangConfs).setGradleArguments("--quiet").build().getCommandAsString();
    String expectedCommand = "bangbang --gradle-tasks runShell --conf gradle.installation.dir=dir2 --conf gradle.build.dir=dir1 --gradle-arguments \"--quiet\" --gradle-file abc.gradle"
    Assert.assertEquals(actualCommand,expectedCommand);
  }
}
