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
package com.linkedin.gradle.scm;

/**
 * Describes the source control management type for the project.
 */
enum ScmType {
  GIT,
  SVN,
  NONE
}

/**
 * Helper class for the SCM metadata so we can easily generate JSON for it.
 */
class ScmMetadataContainer {
  GitMetadata gitMetadata;
  UserMetadata userMetadata;
  SvnMetadata svnMetadata;
  ScmType scmType;

  /**
   * Constructor for ScmMetadata.
   *
   * @param gitMetadata The Git metadata
   * @param svnMetadata The Subversion metadata
   * @param userMetadata The user metadata
   */
  ScmMetadataContainer(GitMetadata gitMetadata, SvnMetadata svnMetadata, UserMetadata userMetadata) {
    this.userMetadata = userMetadata;

    if (gitMetadata.isGitRepo) {
      this.scmType = ScmType.GIT;
      this.gitMetadata = gitMetadata;
    }

    // In the strange case that a project is both a Git and SVN repository, add them both, but
    // leave the primary type as a Git repository if the type is already set.
    if (svnMetadata.isSvnRepo) {
      this.scmType = (this.scmType != null) ? this.scmType : ScmType.SVN;
      this.svnMetadata = svnMetadata;
    }

    // Set scmType to NONE if we did not find either a Git or SVN repository.
    this.scmType = (this.scmType != null) ? this.scmType : ScmType.NONE;
  }

  /**
   * Helper routine to run an executable command and return its standard out.
   *
   * @param command The executable command to run
   * @return The standard output text of the command
   */
  static String runCommand(String command) {
    try {
      def process = command.execute();
      process.waitFor();
      String standardOut = process.in.text;
      return standardOut?.trim() ?: "";
    }
    catch (Exception ex) {
      return "Exception: " + ex.getMessage();
    }
  }

  /**
   * Helper routine to run an executable command, examines each line of its standard out, and
   * returns that line if its prefixed with the given line prefix.
   *
   * @param command The executable command to run
   * @param linePrefix The line prefix
   * @return The standard output text of the command
   */
  static String runCommand(String command, String linePrefix) {
    try {
      def process = command.execute();
      process.waitFor();
      String standardOut = "";
      process.in.eachLine { line ->
        line = line.trim();
        if (line.startsWith(linePrefix)) {
          standardOut = line;
        }
      }
      return standardOut;
    }
    catch (Exception ex) {
      return "Exception: " + ex.getMessage();
    }
  }
}