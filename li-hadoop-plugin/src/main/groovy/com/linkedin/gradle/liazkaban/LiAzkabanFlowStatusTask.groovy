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
package com.linkedin.gradle.liazkaban;

import com.linkedin.gradle.azkaban.AzkabanFlowStatusTask;

class LiAzkabanFlowStatusTask extends AzkabanFlowStatusTask {

  /**
   * Configuring to print Dr. Elephant URL
   *
   * @param execUrl Execution URL of the flow.
   * @return DrElephantUrl
   */
  @Override
  public String getDrElephantURL(String execUrl) {
    final String DR_ELEPHANT_HOLDEM_URL = "http://ltx1-holdemdre01.grid.linkedin.com:8080";
    final String DR_ELEPHANT_WAR_URL = "http://ltx1-waraz01.grid.linkedin.com:8080";

    if (execUrl.contains("holdem")) {
      return "${DR_ELEPHANT_HOLDEM_URL}/search?flow-exec-id=" + URLEncoder.encode(execUrl, "UTF-8").toString();
    } else if (execUrl.contains("war")) {
      return "${DR_ELEPHANT_WAR_URL}/search?flow-exec-id=" + URLEncoder.encode(execUrl, "UTF-8").toString();
    }
    return null;
  }
}
