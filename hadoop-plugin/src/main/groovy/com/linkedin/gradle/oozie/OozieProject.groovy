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
package com.linkedin.gradle.oozie;

class OozieProject {
  /**
   * The URI of the cluster e.g. webhdfs://theNameNode.linkedin.com:50070.
   */
  String clusterURI = "";

  /**
   * The URI of the oozie instance to interact with.
   */
  String oozieURI = "";

  /**
   * The name of the zip task e.g. oozieHadoopZip.
   */
  String oozieZipTask = "";

  /**
   * The project name. A directory will be created at uploadPath with this name.
   */
  String projectName = "";

  /**
   * The path on HDFS where the project directory must be created e.g. /user/annag.
   */
  String uploadPath = "";
}
