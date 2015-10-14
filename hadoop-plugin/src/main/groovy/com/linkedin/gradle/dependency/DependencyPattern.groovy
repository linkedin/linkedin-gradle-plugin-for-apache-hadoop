/*
 * Copyright 2015 LinkedIn Corp.
 *
 * Licensed under the Apache License, VersionPattern 2.0 (the "License"); you may not
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
package com.linkedin.gradle.dependency;

public enum SEVERITY {
  INFO,
  WARN,
  ERROR
}

class DependencyPattern {

  String groupPattern;
  String namePattern;
  String versionPattern;
  SEVERITY severity;
  String message;

  public DependencyPattern(String groupPattern, String namePattern, String versionPattern, SEVERITY severity) {
    this(groupPattern, namePattern, versionPattern, severity, "");
  }

  public DependencyPattern(String groupPattern, String namePattern, String versionPattern, SEVERITY severity, String message) {
    this.groupPattern = groupPattern;
    this.namePattern = namePattern;
    this.versionPattern = versionPattern;
    this.severity = severity;
    this.message = message;
  }

  @Override
  public boolean equals(Object dependencyPattern) {
    return dependencyPattern.groupPattern.equals(groupPattern) && dependencyPattern.namePattern.equals(namePattern) && dependencyPattern.versionPattern.equals(versionPattern) && dependencyPattern.severity.toString().equals(severity.toString()) && dependencyPattern.message.equals(message);
  }

  @Override
  public String toString() {
    return "groupPattern : ${groupPattern}, namePattern ${namePattern}, versionPattern : ${versionPattern}, severity : ${severity}, message : ${message}";
  }

}
