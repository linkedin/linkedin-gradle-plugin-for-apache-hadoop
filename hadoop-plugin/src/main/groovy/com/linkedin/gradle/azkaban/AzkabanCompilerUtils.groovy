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
package com.linkedin.gradle.azkaban

/**
 * Helpful utils for the Azkaban Compilers.
 */
class AzkabanCompilerUtils {
  /**
   * Helper method to sort a list of properties from a Job or a Properties object into a
   * standardized, sorted order that will make reading job and property files easy.
   *
   * @param propertyNames The property names for a Job or Properties object
   * @return The property names in a standardized, sorted order
   */
  static List<String> sortPropertiesToBuild(Set<String> propertyNames) {
    // First, sort the properties alphabetically.
    List<String> propertyList = new ArrayList<String>(propertyNames);
    Collections.sort(propertyList);

    List<String> sortedKeys = new ArrayList<String>(propertyList.size());

    // List the job type and dependencies first if they exist.
    if (propertyList.remove("type")) {
      sortedKeys.add("type");
    }

    if (propertyList.remove("dependencies")) {
      sortedKeys.add("dependencies");
    }

    // Then add the rest of the keys to the final list of sorted keys.
    sortedKeys.addAll(propertyList);
    return sortedKeys;
  }
}
