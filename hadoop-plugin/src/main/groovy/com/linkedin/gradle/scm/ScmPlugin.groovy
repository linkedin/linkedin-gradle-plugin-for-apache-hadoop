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

import groovy.json.JsonBuilder;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * ScmPlugin implements features that generate source control management (scm) metadata, in
 * particular for git and Subversion.
 */
class ScmPlugin implements Plugin<Project> {
  /**
   * Applies the ScmPlugin.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    project.tasks.create("buildScmMetadata") {
      description = "Writes SCM metadata about the project";
      group = "Hadoop Plugin";

      doLast {
        ScmMetadataContainer scm = buildScmMetadata(project);
        String scmJson = new JsonBuilder(scm).toPrettyString();
        new File("${project.projectDir}/.buildMetadata").write(scmJson);
      }
    }

    project.tasks.create("printScmMetadata") {
      description = "Prints SCM metadata about the project to the screen";
      group = "Hadoop Plugin";

      doLast {
        ScmMetadataContainer scm = buildScmMetadata(project);
        println(new JsonBuilder(scm).toPrettyString());
      }
    }
  }

  /**
   * Builds and populates the SCM metadata using the various factory methods in this class.
   * Subclasses can override this method if they want to customize how the SCM metadata is built.
   *
   * @param project The Gradle project
   * @return The ScmMetadata populated and ready to be serialized to JSON
   */
  ScmMetadataContainer buildScmMetadata(Project project) {
    GitMetadata git = createGitMetadata();
    git.setMetadataProperties(project);

    UserMetadata user = createUserMetadata();
    user.setMetadataProperties(project);

    SvnMetadata svn = createSvnMetadata();
    svn.setMetadataProperties(project);

    return createScmMetadataContainer(git, svn, user);
  }

  /**
   * Factory method to create a new GitMetadata instance. Subclasses can override this method to
   * provide a custom GitMetadata instance.
   *
   * @return A new GitMetadata instance
   */
  GitMetadata createGitMetadata() {
    return new GitMetadata();
  }

  /**
   * Factory method to create a new UserMetadata instance. Subclasses can override this method to
   * provide a custom UserMetadata instance.
   *
   * @return A new UserMetadata instance
   */
  UserMetadata createUserMetadata() {
    return new UserMetadata();
  }

  /**
   * Factory method to create a new ScmMetadataContainer instance. Subclasses can override this
   * method to provide a custom ScmMetadataContainer object.
   *
   * @param gitMetadata The Git metadata
   * @param svnMetadata The Subversion metadata
   * @param userMetadata The user metadata
   * @return A new ScmMetadataContainer instance
   */
  ScmMetadataContainer createScmMetadataContainer(GitMetadata gitMetadata, SvnMetadata svnMetadata, UserMetadata userMetadata) {
    return new ScmMetadataContainer(gitMetadata, svnMetadata, userMetadata);
  }

  /**
   * Factory method to create a new SvnMetadata instance. Subclasses can override this method to
   * provide a custom SvnMetadata instance.
   *
   * @return A new SvnMetadata instance
   */
  SvnMetadata createSvnMetadata() {
    return new SvnMetadata();
  }
}