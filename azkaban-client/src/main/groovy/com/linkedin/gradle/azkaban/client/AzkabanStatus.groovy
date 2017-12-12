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
package com.linkedin.gradle.azkaban.client

class AzkabanStatus {

  private int cancelled
  private int disabled
  private int failed
  private int ready
  private int running
  private int succeeded

  AzkabanStatus() {
    cancelled = 0
    disabled = 0
    failed = 0
    ready = 0
    running = 0
    succeeded = 0
  }

  /**
   * Get cancelled jobs count
   * @return cancelled Cancelled jobs count
   */
  int getCancelled() {
    return cancelled
  }

  /**
   * Get disabled jobs count
   * @return disabled Disabled jobs count
   */
  int getDisabled() {
    return disabled
  }

  /**
   * Get failed jobs count
   * @return failed Failed jobs count
   */
  int getFailed() {
    return failed
  }

  /**
   * Get jobs count which are ready
   * @return ready Ready jobs count
   */
  int getReady() {
    return ready
  }

  /**
   * Get running jobs count
   * @return running Running jobs count
   */
  int getRunning() {
    return running
  }

  /**
   * Get succeeded jobs count
   * @return succeeded Succeeded jobs count
   */
  int getSucceeded() {
    return succeeded
  }

  /**
   * Get Total jobs count
   * @return total Total jobs count
   */
  int getTotal() {
    return cancelled + disabled + failed + ready + running + succeeded
  }

  /**
   * Inputs the count of job with that status.
   *
   * @param status
   */
  void increment(String status) {
    switch (status) {
      case "CANCELLED":
        cancelled++
        break

      case "DISABLED":
        disabled++
        break

      case "FAILED":
        failed++
        break

      case "READY":
        ready++
        break

      case "RUNNING":
        running++
        break

      case "SUCCEEDED":
        succeeded++
        break

      default:
        break
    }
  }

  /**
   * Returns Status string of the flow execution
   *
   * @return Status String
   */
  List<String> getStatusValues() {
    List<String> values = []
    values.add(Integer.toString(getRunning()))
    values.add(Integer.toString(getSucceeded()))
    values.add(Integer.toString(getFailed()))
    values.add(Integer.toString(getReady()))
    values.add(Integer.toString(getCancelled()))
    values.add(Integer.toString(getDisabled()))
    values.add(Integer.toString(getTotal()))
    return values
  }

  static List<String> getStatusLabels() {
    List<String> labels = []
    labels.add("Running")
    labels.add("Succeeded")
    labels.add("Failed")
    labels.add("Ready")
    labels.add("Cancelled")
    labels.add("Disabled")
    labels.add("Total")
    return labels
  }
}
