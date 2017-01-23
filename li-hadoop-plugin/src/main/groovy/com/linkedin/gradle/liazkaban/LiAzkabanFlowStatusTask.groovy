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
  String getDrElephantURL(String execUrl) {
    final String DR_ELEPHANT_HOLDEM_URL = "http://ltx1-holdemdre01.grid.linkedin.com:8080/new#/workflow?referrer=hadoopplugin&workflowid=";
    final String DR_ELEPHANT_WAR_URL = "http://lva1-wardre01.grid.linkedin.com:8080/new#/workflow?referrer=hadoopplugin&workflowid=";
    final String DR_ELEPHANT_TAROCK_URL = "http://lva1-tarockaz01.grid.linkedin.com:8080/search?referrer=hadoopplugin&flow-exec-id=";
    final String DR_ELEPHANT_SPADES_URL = "http://lva1-spadesaz01.grid.linkedin.com:8080/search?referrer=hadoopplugin&flow-exec-id=";
    final String DR_ELEPHANT_POKEMON_URL = "http://lva1-pokemonaz01.grid.linkedin.com:8080/new#/workflow?referrer=hadoopplugin&workflowid=";

    if (execUrl.contains("holdem")) {
      return DR_ELEPHANT_HOLDEM_URL + URLEncoder.encode(execUrl, "UTF-8").toString();
    } else if (execUrl.contains("war")) {
      return DR_ELEPHANT_WAR_URL + URLEncoder.encode(execUrl, "UTF-8").toString();
    } else if (execUrl.contains("tarock")) {
      return DR_ELEPHANT_TAROCK_URL + URLEncoder.encode(execUrl, "UTF-8").toString();
    } else if (execUrl.contains("spades")) {
      return DR_ELEPHANT_SPADES_URL + URLEncoder.encode(execUrl, "UTF-8").toString();
    } else if (execUrl.contains("pokemon")) {
      return DR_ELEPHANT_POKEMON_URL + URLEncoder.encode(execUrl, "UTF-8").toString();
    }
    return null;
  }
}
