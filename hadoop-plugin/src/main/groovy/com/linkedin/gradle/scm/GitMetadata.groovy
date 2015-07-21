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
 * This class generates metadata about the user's Git project.
 */
class GitMetadata {
  Boolean isGitRepo;
  String commitHash;
  String commitMessage;
  String gitBranch;
  String gitRepo;
  List<String> gitRemote;
  List<String> gitStatus;

  /**
   * Sets all the properties on this object using the static methods in this class and returns a
   * reference back to this object.
   *
   * @return Reference to this object
   */
  GitMetadata setMetadataProperties(Project project) {
    String gitRepo = getGitRepo();
    if (!gitRepo.isEmpty()) {
      this.commitHash = getCommitHash();
      this.commitMessage = getCommitMessage();
      this.gitBranch = getGitBranch();
      this.gitRemote = getGitRemote();
      this.gitRepo = gitRepo;
      this.gitStatus = getGitStatus();
      this.isGitRepo = true;
    }
    else {
      this.isGitRepo = false;
    }
    return this;
  }

  /**
   * Gets the last commit hash.
   *
   * @return The last commit hash
   */
  static String getCommitHash() {
    def command = "git rev-parse HEAD";
    return ScmMetadataContainer.runCommand(command);
  }

  /**
   * Gets the last commit message.
   *
   * @return The last commit message
   */
  static String getCommitMessage() {
    def command = "git log -1 --oneline";
    return ScmMetadataContainer.runCommand(command);
  }

  /**
   * Gets the output of 'git branch'.
   *
   * @return The output of 'git branch'
   */
  static String getGitBranch() {
    def command = "git branch";
    return ScmMetadataContainer.runCommand(command);
  }

  /**
   * Gets the output of 'git remote -v' split into lines.
   *
   * @return The output of 'git remote -v' split into lines
   */
  static List<String> getGitRemote() {
    def command = "git remote -v";
    String standardOut = ScmMetadataContainer.runCommand(command);
    return standardOut.readLines();
  }

  /**
   * Gets the location of the local git repository, or returns the empty string if the user's
   * project directory is not part of a git repository.
   *
   * @return The location of the local git repository
   */
  static String getGitRepo() {
    def command = "git rev-parse --git-dir";
    return ScmMetadataContainer.runCommand(command);
  }

  /**
   * Gets the output of 'git status --porcelain' split into lines.
   *
   * @return The output of 'git status --porcelain' split into lines
   */
  static List<String> getGitStatus() {
    def command = "git status --porcelain";
    String standardOut = ScmMetadataContainer.runCommand(command);
    return standardOut.readLines();
  }
}