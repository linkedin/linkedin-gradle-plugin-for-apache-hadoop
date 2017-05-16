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
package com.linkedin.gradle.lihadoop

/**
 * Helper class for loading LinkedIn-specific properties from an external resource file.
 * <p>
 * For open-source builds, the file will be missing. For internal builds, we will copy in an
 * external resource file with the correct values.
 */
class LiHadoopProperties {

  // The keys that should exist in this file
  public static final String ARTIFACTORY_REPO = "artifactoryRepo"
  public static final String AZKABAN_URL = "azkabanUrl"
  public static final String MESSAGE_URL = "messageUrl"
  public static final String HLD_GATEWAY_HOME = "hldGatewayHome"
  public static final String HLD_GATEWAY_NODE = "hldGatewayNode"
  public static final String HLD_NAME_NODE_HDFS = "hldNameNodeHdfs"
  public static final String HLD_NAME_NODE_WEBHDFS = "hldNameNodeWebHdfs"
  public static final String HLD_GRID_NAME = "hldGridName"
  public static final String PKE_GRID_NAME = "pkeGridName"
  public static final String SPD_GRID_NAME = "spdGridName"
  public static final String TRK_GRID_NAME = "trkGridName"
  public static final String WAR_GRID_NAME = "warGridName"
  public static final String DR_ELEPHANT_HLD_URL = "drElephantHldUrl"
  public static final String DR_ELEPHANT_PKE_URL = "drElephantPkeUrl"
  public static final String DR_ELEPHANT_SPD_URL = "drElephantSpdUrl"
  public static final String DR_ELEPHANT_TRK_URL = "drElephantTrkUrl"
  public static final String DR_ELEPHANT_WAR_URL = "drElephantWarUrl"
  public static final String OOZIE_URI = "oozieUri"
  public static final String REMOTE_PIG_COMMAND = "remotePigCommand"
  public static final String REMOTE_SPARK_COMMAND = "remoteSparkCommand"

  // For open-source builds the resource file will be missing, which should not cause an error
  private static boolean missing = true

  // Java properties object that will hold the key-value pairs from the embedded resource file
  private static Properties props = null

  /**
   * Helper function to load the embedded resource property file. This method is synchronized, so
   * that the property file will be loaded only once even in the context of multiple threads.
   *
   * @throws Exception If the embedded resource property file cannot be read
   */
  static synchronized void checkLoadResourceProperties() throws Exception {
    if (props == null) {
      props = new Properties()

      // Reference to another class besides this one, since we are in the middle of initializing it
      InputStream inputStream = LiHadoopPlugin.class.getResourceAsStream("/linkedin.properties")
      if (inputStream != null) {
        props.load(inputStream)
        missing = false
      }
    }
  }

  /**
   * Return the property value for the given property key.
   * <p>
   * Since the underlying Properties class returns null if the key is not found, we will explicitly
   * check if the properties object contains the key and throw an exception if it is not found
   * (unless this is an open-source build, in which case we return the empty string).
   *
   * @param key The property key
   * @return The property value for the given key
   * @throws IllegalArgumentException If the given property key does not exist
   */
  static String get(String key) throws IllegalArgumentException {
    // Check if we need to load the embedded property file
    checkLoadResourceProperties()

    // If this is an open-source build, the resource file will be missing. This should not cause an
    // error, so just return the empty string.
    if (missing) {
      return ""
    }

    if (!props.containsKey(key)) {
      throw new IllegalArgumentException("The key " + key + " does not exist in the linkedin.properties file")
    } else {
      return props.getProperty(key)
    }
  }
}
