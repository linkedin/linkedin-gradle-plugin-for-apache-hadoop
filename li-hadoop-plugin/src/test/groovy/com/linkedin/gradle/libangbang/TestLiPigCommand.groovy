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
import org.junit.Before;
import org.junit.Test;

class TestLiPigCommand {

  Map<String, String> properties;

  @Before
  void setup() {
    properties = new HashMap<String,String>();
    properties.put("env.PIG_HOME","/export/apps/pig");
    properties.put("env.JAVA_HOME","/home/jdk");
    properties.put("param.first","value1");
    properties.put("param.second","value2");
    properties.put("hadoop-inject.key1","inject_value1");
    properties.put("hadoop-inject.key2","inject_value2");
    properties.put("pig.script","/src/main/pig")
  }

  @Test
  void testBuildProperties() {
    LiPigCommand liPigCommand = new LiPigCommand(properties);
    liPigCommand.buildProperties();
    String actualArguments = liPigCommand.getArguments().sort().join(" ");
    String expectedArguments = ["'-Dkey1=inject_value1'","'-Dkey2=inject_value2'","'-param'","'first=value1'","'-param'","'second=value2'","'-f'","'/src/main/pig'"].sort().join(" ");
    Assert.assertEquals(expectedArguments,actualArguments);
  }
}