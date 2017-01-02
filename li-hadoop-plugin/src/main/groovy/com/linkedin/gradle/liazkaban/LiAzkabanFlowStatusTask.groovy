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
   * @param execId Execution Id of the flow
   * @return DrElephantUrl
   */
  @Override
  public String getDrElephantURL(String execId) {

    if (azkProject.azkabanUrl.contains("holdem")) {
      return "http://go/drholdem_plugin%20${execId}";
    } else if (azkProject.azkabanUrl.contains("war")) {
      return "http://go/drwar_plugin%20${execId}";
    }
    return null;
  }
}
