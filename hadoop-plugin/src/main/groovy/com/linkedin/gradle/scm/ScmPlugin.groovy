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
import groovy.json.JsonSlurper;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.bundling.Zip

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
    // Enable users to skip the plugin
    if (project.hasProperty("disableScmPlugin")) {
      println("ScmPlugin disabled");
      return;
    }


    project.tasks.create("writeScmPluginData"){
      description = "writes SCM Plugin data"
      group = "Hadoop Plugin"

      def scmPluginDataFilePath = getScmPluginDataFilePath(project)
      if(!new File(scmPluginDataFilePath).exists()){
        String scmDataJson = new JsonBuilder(new ScmPluginData()).toPrettyString()
        new File(scmPluginDataFilePath).write(scmDataJson)
      }
    }


    project.tasks.create("buildScmMetadata") {
      description = "Writes SCM metadata about the project";
      group = "Hadoop Plugin";

      doLast {
        ScmMetadataContainer scm = buildScmMetadata(project);
        String scmJson = new JsonBuilder(scm).toPrettyString();
        new File(getMetadataFilePath(project)).write(scmJson);
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
    List<String> excludeList = new ArrayList<String>();
    def slurper = new JsonSlurper();
    def scmPluginDataFilePath = getScmPluginDataFilePath(project);
    try {
      def reader = new BufferedReader(new FileReader(scmPluginDataFilePath));
      def parsedData = slurper.parse(reader).sourceExclude;
      parsedData.each {  exclude ->
        excludeList.add(exclude)
      }
    } catch(IOException e){
      println(e.printStackTrace())
    }
    return excludeList;
  }

  /**
   * @param project
   * @return path of .scmPlugin.json
   */
  String getScmPluginDataFilePath(Project project){
    return  "${project.getRootProject().projectDir}/.scmPlugin.json"
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
   * Helper method to determine the location of the sources zip file. This helper method will make
   * it easy for subclasses to get (or customize) the file location.
   *
   * @param project The Gradle project
   * @return The path to the sources zip file
   */
  String getSourceZipFilePath(Project project) {
    return "${project.rootProject.buildDir}/distributions/${project.rootProject.name}-${project.rootProject.version}-sources.zip"
  }
}