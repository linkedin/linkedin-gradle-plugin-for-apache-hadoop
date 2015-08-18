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

public final class OozieConstants {
  /**
   * The URI of the cluster e.g. webhdfs://eat1-nertznn01.grid.linkedin.com:50070.
   */
  public static final String OOZIE_CLUSTER_URI = "clusterURI";

  /**
   * The Name of the project. A directory will be created at PATH_TO_UPLOAD with this name.
   */
  public static final String OOZIE_PROJECT_NAME = "projectName";

  /**
   * The Path where project directory must be created e.g.
   * webhdfs://eat1-nertznn01.grid.linkedin.com:50070/user/annag.
   */
  public static final String PATH_TO_UPLOAD = "uploadPath";

  /**
   * The path of local directory which must be uploaded. This shouldn't be here. We should get this
   * value from some other task such as OozieBuildProject or OozieBuildFlows.
   */
  public static final String DIR_TO_UPLOAD = "dirToUpload";
}