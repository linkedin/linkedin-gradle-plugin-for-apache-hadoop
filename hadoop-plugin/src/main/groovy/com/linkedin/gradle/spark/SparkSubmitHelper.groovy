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
   * Build Spark options.
   *
   * @param properties The job properties object for the spark job type
   * @return String of the form "--$key $value" which can be passed to spark-submit
   */
  static String buildSparkOptions(Map<String, Object> properties) {
    if (!validate(properties)) {
      return "";
    }

    StringBuilder builder = new StringBuilder();
    properties.each { key, value ->
      if (allSparkOptions.contains(key)) {
        builder.append("--$key $value ");
      }
    }

    return builder.toString();
  }

  /**
   * Build Spark application class option.
   *
   * @param appClass The main execution class of the app
   * @return String of the form "--class $appClass" which can be directly passed to spark-submit
   */
  static String buildSparkClass(String appClass) {
    return appClass == null ? "" : "--class $appClass";
  }

  /**
   * Build Spark conf options.
   *
   * @param sparkConfs The Spark configurations to be passed to spark-submit
   * @return String of the form "--conf key=value" which can be directly passed to spark-submit
   */
  static String buildSparkConfs(Map<String, Object> sparkConfs) {
    if (!validate(sparkConfs)) {
      return "";
    }
    return sparkConfs.collect { key, value -> "--conf $key=$value" }.join(" ");
  }

  /**
   * Build Spark flags.
   *
   * @param flags The Spark flags to be passed to spark-submit
   * @return String of the form "--$flag" which can be directly passed to spark-submit
   */
  static String buildSparkFlags(Set<String> flags) {
    if (!validate(flags)) {
      return "";
    }
    return flags.collect { flag -> return "--$flag"}.join(" ");
  }

  /**
   * Build Spark application parameters.
   *
   * @param params The application parameters
   * @return Space-separated list of parameters
   */
  static String buildSparkAppParams(List<String> params) {
    if (!validate(params)) {
      return "";
    }
    return params.join(" ");
  }

  /**
   * Validates a Set.
   *
   * @param s The Set to validate
   * @return Whether or not the Set is non-null and non-empty
   */
  static boolean validate(Set<Object> s) {
    return !(s.isEmpty() || s == null);
  }

  /**
   * Validates a Map.
   *
   * @param s The Map to validate
   * @return Whether or not the Map is non-null and non-empty
   */
  static boolean validate(Map<Object, Object> s) {
    return !(s.isEmpty() || s == null);
  }

  /**
   * Validates a List.
   *
   * @param s The List to validate
   * @return Whether or not the List is non-null and non-empty
   */
  static boolean validate(List<Object> s) {
    return !(s.isEmpty() || s == null);
  }
}
