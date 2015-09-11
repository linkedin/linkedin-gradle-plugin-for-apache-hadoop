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
package com.linkedin.gradle.spark

class SparkSubmitHelper {

  static final Set<String> allSparkOptions = [
    "master",
    "deploy-mode",
    "jars",
    "py-files",
    "properties-file",
    "driver-memory",
    "driver-java-options",
    "driver-library-path",
    "driver-class-path",
    "executor-memory",
    "driver-cores",
    "total-executor-cores",
    "executor-cores",
    "queue",
    "num-executors",
    "archives",
    "principal",
    "keytab"
  ];

  /**
   * Build spark options
   * @param properties The job properties object for the spark job type
   * @return string of the form "--$key $value" which can be passed to spark-submit
   */
  static String buildSparkOptions(Map<String,Object> properties) {
    if(!validate(properties)) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    properties.each() { key,value ->
      if(allSparkOptions.contains(key)) {
        builder.append("--$key $value ");
      }
    }
    return builder.toString();
  }

  /**
   * Build application class
   * @param appClass The main execution class of the app
   * @return string of the form "--class $appClass" which can be directly passed to spark-submit
   */
  static String buildSparkClass(String appClass) {
    return "--class $appClass";
  }

  /**
   * Build spark confs
   * @param sparkConfs The spark configurations to be passed to spark-submit
   * @return string of the form "--conf key=value" which can be directly passed to spark-submit
   */
  static String buildSparkConfs(Map<String,Object> sparkConfs) {
    if(!validate(sparkConfs)) {
      return "";
    }
    return sparkConfs.collect() { key,value -> "--conf $key=$value" }.join(" ");
  }

  /**
   * Build spark flags
   * @param flags The spark flags to be passed to spark-submit
   * @return string of the form "--$flag" which can be directly passed to spark-submit
   */
  static String buildSparkFlags(Set<String> flags) {
    if(!validate(flags)) {
      return "";
    }
    return flags.collect() { flag -> return "--$flag"}.join(" ");
  }

  /**
   * Build spark application parameters
   * @param params The application parameters
   * @return space separated list of parameters
   */
  static String buildSparkAppParams(List<String> params) {
    if(!validate(params)) {
      return "";
    }
    return params.join(" ");
  }

  /**
   * Validates Set
   * @param s The Set to validate
   * @return true if Set is valid otherwise false
   */
  static boolean validate(Set<Object> s) {
    return !(s.isEmpty() || s == null);
  }

  /**
   * Validates Map
   * @param s The Map to validate
   * @return true if Map is valid otherwise false
   */
  static boolean validate(Map<Object,Object> s) {
    return !(s.isEmpty() || s == null);
  }

  /**
   * Validates List
   * @param s The List to validate
   * @return true if List is valid otherwise false
   */
  static boolean validate(List<Object> s) {
    return !(s.isEmpty() || s == null);
  }
}
