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
package com.linkedin.gradle.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;

/**
 * Util class for Azkaban Client.
 */
class AzkabanClientUtil {
  /**
   * Converts epoch to Date format.
   *
   * @param epoch timestamp
   * @return Date format in string. Returns "- "if epoch is -1
   */
  static String epochToDate(String epoch) {
    if (epoch.equals("-1")) {
      return "-"
    } else {
      Date date = new Date(Long.parseLong(epoch));
      DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
      return dateFormat.format(date);
    }
  }

  /**
   * Returns time elapsed between start and end epoch times. Incase startEpoch is -1, returns "-".
   * In case endEpoch is -1, returns time elapsed between start and current epoch.
   *
   * @param startEpoch Starting epoch timestamp
   * @param endEpoch Ending epoch timestamp
   * @return elapsedTime Time elapsed between start and stop Epoch time
   */
  static String getElapsedTime(String startEpoch, String endEpoch) throws IllegalArgumentException, ArithmeticException {
    if (startEpoch.equals("-1")) {
      return "-";
    } else if (endEpoch.equals("-1")) {
      endEpoch = Long.toString(Instant.now().toEpochMilli());
    }

    Long start = Long.parseLong(startEpoch);
    Long end = Long.parseLong(endEpoch);

    if (start < 0 || end < 0 || start > end) {
      throw new IllegalArgumentException();
    }

    int elapsed = (end - start)/1000L;
    int elapsedSecs = elapsed % 60;
    int elapsedMins = ((int)(elapsed / 60)) % 60;
    int elapsedHours = elapsed / 3600;

    StringBuilder elapsedFormatted = new StringBuilder();

    if (elapsedHours) {
      elapsedFormatted.append("${elapsedHours} Hr ");
    }
    if (elapsedHours || elapsedMins) {
      elapsedFormatted.append("${elapsedMins} Min ");
    }
    elapsedFormatted.append("${elapsedSecs} Sec");

    return elapsedFormatted.toString();
  }
}
