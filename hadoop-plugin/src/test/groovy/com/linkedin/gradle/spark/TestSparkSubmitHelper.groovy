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

import com.linkedin.gradle.spark.SparkSubmitHelper;

import org.junit.Assert;
import org.junit.Test;

class TestSparkSubmitHelper {
  @Test
  void testSparkOptions() {
    Map<String, Object> properties = new TreeMap<String, Object>();
    properties.put("master", "yarn-cluster");
    properties.put("queue", "marathon");
    properties.put("jars", "jar1");
    String actual = SparkSubmitHelper.buildSparkOptions(properties);
    String expected = "--jars jar1 --master yarn-cluster --queue marathon "
    Assert.assertEquals(expected, actual);

    // test for random values
    Map<String, Object> randomProperties = new HashMap<String, Object>();
    randomProperties.put("blah1", "blah1");
    randomProperties.put("blah2", "blah2");
    actual = SparkSubmitHelper.buildSparkOptions(randomProperties);
    expected = ""
    Assert.assertEquals(expected, actual);
  }

  @Test
  void testSparkClass() {
    Assert.assertEquals("--class className", SparkSubmitHelper.buildSparkClass("className"));
  }

  @Test
  void testSparkConfs() {
    Map<String, Object> confs = new HashMap<String, Object>();
    confs.put("key1", "value1");
    confs.put("key2", "value2");
    confs.put("key3", "value3");
    String actual = SparkSubmitHelper.buildSparkConfs(confs);
    String expected = "--conf key1=value1 --conf key2=value2 --conf key3=value3"
    Assert.assertEquals(expected, actual);

    // test for empty HashMap
    Assert.assertTrue(SparkSubmitHelper.buildSparkConfs(new HashMap<String, Object>()).length() == 0);
  }

  @Test
  void testSparkFlags() {
    Set s = ["verbose", "version", "help"];
    String expected = SparkSubmitHelper.buildSparkFlags(s);
    String actual = "--verbose --version --help";
    Assert.assertEquals(expected, actual);

    // test for empty set
    Set empty = []
    Assert.assertEquals("", SparkSubmitHelper.buildSparkFlags(empty));
  }

  @Test
  void testSparkAppParams() {
    List appParams = ["param1", "param2", "param3"];
    String actual = SparkSubmitHelper.buildSparkAppParams(appParams);
    String expected = "param1 param2 param3";
    Assert.assertEquals(expected, actual);

    // test for empty list
    Assert.assertEquals("", SparkSubmitHelper.buildSparkAppParams([]));
  }
}