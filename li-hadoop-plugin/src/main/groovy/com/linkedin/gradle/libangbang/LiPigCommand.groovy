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


class LiPigCommand extends LiHadoopShellCommand {

//  private static final String PIG_ADDITIONAL_JARS = "pig.additional.jars";
//  private static final String UDF_IMPORT = "udf.import.list";
  private static final String HADOOP_INJECT = "hadoop-inject.";
  private static final String PARAM = "param";
  private static final String ENV = "env.";
  private static final String PIG_COMMAND = "pig";
  private static final String FILE = "pig.script";

  private List<String> argumentList;
  private Map<String, String> environmentMap;
  private static final String PIG_JAVA_OPTS = "PIG_JAVA_OPTS";
  private List<String> javaOptsList;

  public LiPigCommand(Map<String, String> properties) {
    this.argumentList = new ArrayList<String>();
    this.environmentMap = new HashMap<String, String>();
    this.javaOptsList = new ArrayList<String>();
    buildProperties(properties);
  }

  /**
   * Builds the arguments from properties
   * @param properties The properties from which arguments should be build
   */
  void buildProperties(Map<String, String> properties) {
    properties.each { key, value ->
      if (key.equals(FILE)) {
        this.argumentList.add("-f");
        this.argumentList.add(value);
      } else if (key.startsWith(PARAM)) {
        this.argumentList.addAll(getParameters(key, value));
      } else if (key.startsWith(HADOOP_INJECT)) {
        addToJavaOptsList(key, value);
      } else if (key.startsWith(ENV)) {
        addToEnvironment(key, value);
      }
    }
  }

  /**
   * Adds the given key, value pair to java options.
   * @param key The key of the jvm option
   * @param value The value of the jvm option
   */
  void addToJavaOptsList(String key, String value) {
    String property = (key.substring(key.indexOf(".") + 1));
    javaOptsList.add(getJvmString(property, value));
  }

  /**
   * Adds the given key and value to the environment
   * @param key The key of the environment
   * @param value The value of the environment
   */
  void addToEnvironment(String key, String value) {
    String environmentKey = key.substring(key.indexOf(".") + 1);
    if (environmentMap.containsKey(environmentKey)) {
      environmentMap.put(environmentKey, [environmentMap.get(environmentKey), environmentMap].join(" "));
      return;
    }
    environmentMap.put(environmentKey , value);
  }

  /**
   * Takes a key,value pair and returns the jvm string of the form -Dkey=value
   * @param key The key of the option
   * @param value The value of the option
   * @return String of the form -Dkey=value
   */
  String getJvmString(String key, String value) {
    return String.format("-D%s=%s", key, value);
  }

  List<String> getParameters(String key, String value) {
    List<String> parameterList = new ArrayList<String>();
    parameterList.add("-$PARAM");
    parameterList.add(String.format("%s=%s", key.substring(key.indexOf(".") + 1), value));
    return parameterList;
  }


  @Override
  List<String> getArguments() {
    List<String> arguments = new ArrayList<String>();
    arguments.addAll(javaOptsList);
    arguments.addAll(argumentList);
    return arguments.collect { "\'${it}\'" };
  }

  @Override
  String getExecutable() {
    return PIG_COMMAND;
  }

  @Override
  Map<String, String> getEnvironment() {
    Map<String,String> quotedEnvironmentMap = new HashMap<String, String>();
    environmentMap.each {
      key,value -> quotedEnvironmentMap.put("\'${key}\'","\'${value}\'");
    }
    return quotedEnvironmentMap;
  }
}
