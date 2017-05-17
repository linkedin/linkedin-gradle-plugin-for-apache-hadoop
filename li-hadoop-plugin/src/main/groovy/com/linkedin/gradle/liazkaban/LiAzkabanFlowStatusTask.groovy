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
package com.linkedin.gradle.liazkaban

import com.linkedin.gradle.azkaban.AzkabanFlowStatusTask
import com.linkedin.gradle.lihadoop.LiHadoopProperties

class LiAzkabanFlowStatusTask extends AzkabanFlowStatusTask {

  private static final String DR_ELEPHANT_HLD_URL = LiHadoopProperties.get(LiHadoopProperties.DR_ELEPHANT_HLD_URL)
  private static final String DR_ELEPHANT_PKE_URL = LiHadoopProperties.get(LiHadoopProperties.DR_ELEPHANT_PKE_URL)
  private static final String DR_ELEPHANT_SPD_URL = LiHadoopProperties.get(LiHadoopProperties.DR_ELEPHANT_SPD_URL)
  private static final String DR_ELEPHANT_TRK_URL = LiHadoopProperties.get(LiHadoopProperties.DR_ELEPHANT_TRK_URL)
  private static final String DR_ELEPHANT_WAR_URL = LiHadoopProperties.get(LiHadoopProperties.DR_ELEPHANT_WAR_URL)
  private static final String HLD_GRID_NAME = LiHadoopProperties.get(LiHadoopProperties.HLD_GRID_NAME)
  private static final String PKE_GRID_NAME = LiHadoopProperties.get(LiHadoopProperties.PKE_GRID_NAME)
  private static final String SPD_GRID_NAME = LiHadoopProperties.get(LiHadoopProperties.SPD_GRID_NAME)
  private static final String TRK_GRID_NAME = LiHadoopProperties.get(LiHadoopProperties.TRK_GRID_NAME)
  private static final String WAR_GRID_NAME = LiHadoopProperties.get(LiHadoopProperties.WAR_GRID_NAME)

  /**
   * Builds the URL to Dr. Elephant for a given flow.
   *
   * @param execUrl The flow execution URL
   * @return The URL to Dr. Elephant for the given flow
   */
  @Override
  String getDrElephantURL(String execUrl) {
    String drUrl = null

    if (execUrl.contains(HLD_GRID_NAME)) {
      drUrl = DR_ELEPHANT_HLD_URL
    } else if (execUrl.contains(PKE_GRID_NAME)) {
      drUrl = DR_ELEPHANT_PKE_URL
    } else if (execUrl.contains(SPD_GRID_NAME)) {
      drUrl = DR_ELEPHANT_SPD_URL
    } else if (execUrl.contains(TRK_GRID_NAME)) {
      drUrl = DR_ELEPHANT_TRK_URL
    } else if (execUrl.contains(WAR_GRID_NAME)) {
      drUrl = DR_ELEPHANT_WAR_URL
    }

    return (drUrl == null) ? null : drUrl + URLEncoder.encode(execUrl, "UTF-8").toString()
  }
}
