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

import groovy.json.JsonBuilder;
import groovy.json.JsonSlurper;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.bundling.Zip;

/**
 * ScmPlugin implements features that generate source control management (scm) metadata, in
 * particular for Git and Subversion.
 */
class ScmPlugin implements Plugin<Project> {
  /**
   * Applies the ScmPlugin.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    // Enable users to skip the plugin
    if (project.hasProperty("disableScmPlugin")) {
      println("ScmPlugin disabled");
      return;
    }

    project.tasks.create("buildScmMetadata") {
      description = "Writes SCM metadata about the project to the project's build directory";
      group = "Hadoop Plugin";

      // Add an extension with the path to the metadata file so it can be accessed by other tasks
      ext.metadataPath = getMetadataFilePath(project);

      doLast {
        ScmMetadataContainer scm = buildScmMetadata(project);
        String scmJson = new JsonBuilder(scm).toPrettyString();
        File file = new File(ext.metadataPath);
        file.getParentFile().mkdirs();
        file.write(scmJson);
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

    project.tasks.create("writeScmPluginJson") {
      description = "Writes a default .scmPlugin.json file in the root project directory"
      group = "Hadoop Plugin";

      doLast {
        String pluginJsonPath = getPluginJsonPath(project);
        if (!new File(pluginJsonPath).exists()) {
          String pluginJson = new JsonBuilder(new ScmPluginData()).toPrettyString();
          new File(pluginJsonPath).write(pluginJson);
        }
      }
    }

    // We'll create the buildSourceZip task on the root project, so that there is only one sources
    // zip created that can be shared by all projects. Thus, only create the buildSourceZip task on
    // the root project if it hasn't been created already (you will get an exception if you try to
    // create it more than once).
    if (project.getRootProject().tasks.findByName("buildSourceZip") == null) {
      createSourceTask(project);
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

  /**
   * Factory method to create a task that builds a sources zip for the root project.
   *
   * @param project The Gradle project
   * @return Task that creates a sources zip
   */
  Task createSourceTask(Project project) {
    return project.getRootProject().tasks.create(name: "buildSourceZip", type: Zip) {
      classifier = "sources"
      description = "Builds a sources zip starting from the root project, excluding all build directories";
      group = "Hadoop Plugin";

      String projectRoot = "${project.getRootProject().projectDir}/";
      List<String> excludeList = buildExcludeList(project);

      FileTree fileTree = project.getRootProject().fileTree([
        dir: projectRoot,
        excludes: excludeList
      ]);

      from fileTree;
    }
  }

  /**
   * Builds a list of relative paths to exclude from the sources zip for the project.
   *
   * @param project The Gradle project
   * @return The list of relative paths to exclude from the sources zip
   */
  List<String> buildExcludeList(Project project) {
    List<String> excludeList = new ScmPluginData().sourceExclude;

    def pluginJson = readScmPluginJson(project);
    if (pluginJson != null) {
      pluginJson.sourceExclude.each { exclude ->
        if (!excludeList.contains(exclude)) {
          excludeList.add(exclude);
        }
      }
    }

    return excludeList;
  }

  /**
   * Helper method to determine the location of the build metadata file. This helper method will
   * make it easy for subclasses to get (or customize) the file location.
   *
   * @param project The Gradle project
   * @return The path to the build metadata file
   */
  String getMetadataFilePath(Project project) {
    return "${project.buildDir}/buildMetadata.json";
  }

  /**
   * Helper method to determine the location of the plugin json file. This helper method will make
   * it easy for subclasses to get (or customize) the file location.
   *
   * @param project The Gradle project
   * @return The path to the plugin json file
   */
  String getPluginJsonPath(Project project) {
    return "${project.getRootProject().projectDir}/.scmPlugin.json";
  }

  /**
   * Helper method to read the plugin json file as a JSON object. For backwards compatibility
   * reasons, we should read it as a JSON object instead of coercing it into a domain object.
   *
   * @param project The Gradle project
   * @return A JSON object or null if the file does not exist
   */
  def readScmPluginJson(Project project) {
    String pluginJsonPath = getPluginJsonPath(project);
    if (!new File(pluginJsonPath).exists()) {
      return null;
    }

    def reader = null;
    try {
      reader = new BufferedReader(new FileReader(pluginJsonPath));
      def slurper = new JsonSlurper();
      def pluginJson = slurper.parse(reader);
      return pluginJson;
    }
    catch (Exception ex) {
      throw new Exception("\nError parsing ${pluginJsonPath}.\n" + ex.toString());
    }
    finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
