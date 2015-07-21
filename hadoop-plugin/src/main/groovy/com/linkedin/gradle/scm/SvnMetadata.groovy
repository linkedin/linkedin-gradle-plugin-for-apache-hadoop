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

import org.gradle.api.Project;

/**
 * This class generates metadata about the user's Subversion project.
 */
class SvnMetadata {
  Boolean isSvnRepo;
  String svnRemote;
  String commitMessage;
  List<String> svnInfo;
  List<String> svnStatus;

  /**
   * Sets all the properties on this object using the static methods in this class and returns a
   * reference back to this object.
   *
   * @return Reference to this object
   */
  SvnMetadata setMetadataProperties(Project project) {
    String svnRepo = getSvnRepo();
    if (!svnRepo.isEmpty()) {
      this.commitMessage = getCommitMessage();
      this.svnInfo = getSvnInfo();
      this.svnRemote = getSvnRemote();
      this.svnStatus = getSvnStatus();
      this.isSvnRepo = true;
    }
    else {
      this.isSvnRepo = false;
    }
    return this;
  }

  /**
   * Gets the last commit message.
   *
   * @return The last commit message
   */
  static String getCommitMessage() {
    def command = "svn log -l 1"
    return ScmMetadataContainer.runCommand(command);
  }

  /**
   * Gets the output of 'svn info' split into lines.
   *
   * @return The output of 'svn info' split into lines
   */
  static List<String> getSvnInfo() {
    def command = "svn info";
    String standardOut = ScmMetadataContainer.runCommand(command);
    return standardOut.readLines();
  }

  /**
   * Gets the location of the remote svn repository root, or returns the empty string if the user's
   * project directory is not part of an svn repository.
   *
   * @return The location of the remote svn repository root
   */
  static String getSvnRemote() {
    String filter = "Repository Root: "
    def command = "svn info";
    String standardOut = ScmMetadataContainer.runCommand(command, filter);
    return standardOut.replace(filter, "");
  }

  /**
   * Gets the location of the local svn repository, or returns the empty string if the user's
   * project directory is not part of an svn repository.
   *
   * @return The location of the local svn repository
   */
  static String getSvnRepo() {
    String filter = "Working Copy Root Path: ";
    def command = "svn info";
    String standardOut = ScmMetadataContainer.runCommand(command, filter);
    return standardOut.replace(filter, "");
  }

  /**
   * Gets the output of 'svn status' split into lines.
   *
   * @return The output of 'svn status' split into lines
   */
  static List<String> getSvnStatus() {
    def command = "svn status";
    String standardOut = ScmMetadataContainer.runCommand(command);
    return standardOut.readLines();
  }
}