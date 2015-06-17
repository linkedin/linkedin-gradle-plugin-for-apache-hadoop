/*
 * Copyright 2014 LinkedIn Corp.
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

/**
 * Utility for formatting Html String by removing tags and making the content
 * more readable. This is currently used for formatting Azkaban returned
 * HttpResponse to improve the readability of Azkaban generated messages.
 *
 */
class HtmlUtil {

  public static String toText(String html) {
    String newline = System.getProperty("line.separator");
    return html == null ? "" : html
        .replaceAll("<br/>", newline)
        .replaceAll("<ul>", newline + newline)
        .replaceAll("</ul>", newline)
        .replaceAll("<li>", "* ")
        .replaceAll("</li>", newline);
  }
}
