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
package com.linkedin.gradle.scm;

import java.text.SimpleDateFormat;

import org.gradle.api.Project;

/**
 * This class generates basic metadata about the user and the project.
 */
class UserMetadata {
  String dateText;
  String hostName;
  String javaHome;
  String javaVersion;
  String platformName;
  String projectDir;
  String projectName;
  String userName;

  /**
   * Sets all the properties on this object using the static methods in this class and returns a
   * reference back to this object.
   *
   * @return Reference to this object
   */
  UserMetadata setMetadataProperties(Project project) {
    this.dateText = getDateText();
    this.hostName = getHostName();
    this.javaHome = getJavaHome();
    this.javaVersion = getJavaVersion();
    this.platformName = getPlatformName();
    this.projectDir = project.projectDir;
    this.projectName = project.name;
    this.userName = getUserName();
    return this;
  }

  /**
   * Gets the date in a nice readable format.
   *
   * @return The date
   */
  static String getDateText() {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());
  }

  /**
   * Gets the hostname of the machine on which the project is being built.
   * <p>
   * This makes an exec call to "hostname". See
   * http://stackoverflow.com/questions/7348711/recommended-way-to-get-hostname-in-java
   * for an explanation.
   *
   * @return The hostname
   */
  String getHostName() {
    def command = "hostname";
    return ScmMetadataContainer.runCommand(command);
  }

  /**
   * Gets the value of the JAVA_HOME property.
   *
   * @return The value of JAVA_HOME
   */
  String getJavaHome() {
    return System.getProperty("java.home");
  }

  /**
   * Gets the Java version.
   *
   * @return The Java version
   */
  String getJavaVersion() {
    return System.getProperty("java.version");
  }

  /**
   * Gets the operating system name.
   *
   * @return The operating system name
   */
  String getPlatformName() {
    return System.getProperty("os.name");
  }

  /**
   * Gets the user ID of the user building the project.
   *
   * @return The user ID
   */
  String getUserName() {
    return System.getProperty("user.name");
  }
}