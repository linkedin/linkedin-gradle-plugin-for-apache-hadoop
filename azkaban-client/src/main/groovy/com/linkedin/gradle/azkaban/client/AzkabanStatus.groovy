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
package com.linkedin.gradle.azkaban.client;

class AzkabanStatus {

  private int _cancelled;
  private int _disabled;
  private int _failed;
  private int _ready;
  private int _running;
  private int _succeeded;

  AzkabanStatus() {
    _cancelled = 0;
    _disabled = 0;
    _failed = 0;
    _ready = 0;
    _running = 0;
    _succeeded = 0;
  }

  /**
   * Get cancelled jobs count
   * @return _cancelled Cancelled jobs count
   */
  int getCancelled() {
    return _cancelled;
  }

  /**
   * Get disabled jobs count
   * @return _disabled Disabled jobs count
   */
  int getDisabled() {
    return _disabled;
  }

  /**
   * Get failed jobs count
   * @return _failed Failed jobs count
   */
  int getFailed() {
    return _failed;
  }

  /**
   * Get jobs count which are ready
   * @return _ready Ready jobs count
   */
  int getReady() {
    return _ready;
  }

  /**
   * Get running jobs count
   * @return _running Running jobs count
   */
  int getRunning() {
    return _running;
  }

  /**
   * Get succeeded jobs count
   * @return _succeeded Succeeded jobs count
   */
  int getSucceeded() {
    return _succeeded;
  }

  /**
   * Get Total jobs count
   * @return _total Total jobs count
   */
  int getTotal() {
    return _cancelled + _disabled + _failed + _ready + _running + _succeeded;
  }

  /**
   * Inputs the count of job with that status.
   *
   * @param status
   */
  void increment(String status) {
    switch (status) {
      case "CANCELLED":
        _cancelled++;
        break;

      case "DISABLED":
        _disabled++;
        break;

      case "FAILED":
        _failed++;
        break;

      case "READY":
        _failed++;
        break;

      case "RUNNING":
        _running++;
        break;

      case "SUCCEEDED":
        _succeeded++
        break;

      default:
        break;
    }
  }

  /**
   * Returns Status string of the flow execution
   *
   * @return Status String
   */
  List<String> getStatusValues() {
    List<String> values = new ArrayList<String>();
    values.add(Integer.toString(getRunning()));
    values.add(Integer.toString(getSucceeded()));
    values.add(Integer.toString(getFailed()));
    values.add(Integer.toString(getReady()));
    values.add(Integer.toString(getCancelled()));
    values.add(Integer.toString(getDisabled()));
    values.add(Integer.toString(getTotal()));
    return values;
  }

  static List<String> getStatusLabels() {
    List<String> labels = new ArrayList<String>();
    labels.add("Running");
    labels.add("Succeeded");
    labels.add("Failed");
    labels.add("Ready");
    labels.add("Cancelled");
    labels.add("Disabled");
    labels.add("Total");
    return labels;
  }
}
