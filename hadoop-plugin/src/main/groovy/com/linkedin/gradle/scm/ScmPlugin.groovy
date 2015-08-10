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

import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileTree;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.bundling.Zip;

/**
 * ScmPlugin implements features that generate source control management (scm) metadata, in
 * particular for Git and Subversion.
 */
class ScmPlugin implements Plugin<Project> {
  Configuration hadoopZipConf;
  HadoopZipExtension hadoopZipExtension;

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

      doLast {
        ScmMetadataContainer scm = buildScmMetadata(project);
        String scmJson = new JsonBuilder(scm).toPrettyString();
        File file = new File(getMetadataFilePath(project));
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

    // Create the hadoopRuntime configuration and the HadoopZipExtension for the project.
    hadoopZipConf = createZipConfiguration(project);
    hadoopZipExtension = createZipExtension(project);

    // Create top-level tasks to build the Hadoop zips. The individual Hadoop zip tasks will
    // depend on startHadoopZips and be dependencies of buildHadoopZips. This enables the user's
    // build task to depend on buildHadoopZips and then have startHadoopZips depend on other tasks.
    project.tasks.create("startHadoopZips") {
      description = "Container task on which all the Hadoop zip tasks depend";
      group = "Hadoop Plugin";
    }

    project.tasks.create("buildHadoopZips") {
      dependsOn "startHadoopZips"
      description = "Builds all of the Hadoop zip archives";
      group = "Hadoop Plugin";
    }

    // Create task in afterEvaluate so that the 'main' in hadoopZip extension is resolved first,
    // otherwise the getContents() method of HadoopZipExtension returns null.
    project.afterEvaluate {
      for (String cluster : hadoopZipExtension.getZipMap().keySet()) {
        createZipTask(project, cluster);
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
   * Prepare the "hadoopRuntime" Hadoop configuration for the project.
   *
   * @param project The Gradle project
   * @return hadoopZipConf The "hadoopRuntime" Hadoop configuration
   */
  Configuration createZipConfiguration(Project project) {
    Configuration hadoopZipConf = project.getConfigurations().create("hadoopRuntime");

    // For Java projects, the Hadoop zip configuration should contain the runtime jars by default.
    project.getPlugins().withType(JavaPlugin) {
      hadoopZipConf.extendsFrom(project.getConfigurations().getByName("runtime"));
    }

    return hadoopZipConf;
  }

  /**
   * Helper method to create the Hadoop zip extension. Having this method allows for the unit tests
   * to override it.
   *
   * @param project The Gradle project
   * @return The Hadoop zip extension
   */
  HadoopZipExtension createZipExtension(Project project) {
    HadoopZipExtension extension = new HadoopZipExtension(project);
    project.extensions.add("hadoopZip", extension);
    return extension;
  }

  /**
   * Method to create the Hadoop Zip task for the given named zip.
   *
   * @param project The Gradle project
   * @param zipName The zip name
   * @return The zip task
   */
  Task createZipTask(Project project, String zipName) {
    Task zipTask = project.tasks.create(name: "${zipName}HadoopZip", type: Zip) { task ->
      classifier = zipName.equals("main") ? "" : zipName;
      description = "Creates a Hadoop zip archive for ${zipName}";
      group = "Hadoop Plugin";

      // This task is a dependency of buildHadoopZips and depends on the startHadoopZips
      project.tasks["buildHadoopZips"].dependsOn task;
      dependsOn "startHadoopZips";

      // This task depends on buildSourceZip and buildScmMetada tasks
      task.dependsOn(project.getRootProject().tasks["buildSourceZip"]);
      task.dependsOn(project.tasks["buildScmMetadata"]);

      // Include files specified by the user through hadoopZip extension. If there is a base
      // CopySpec, add it as a child of the cluster specific CopySpec.
      if (hadoopZipExtension.getBaseCopySpec() != null) {
        task.with(hadoopZipExtension.getZipCopySpec(zipName).with(hadoopZipExtension.getBaseCopySpec()));
      }
      else {
        task.with(hadoopZipExtension.getZipCopySpec(zipName));
      }

      // Add the buildMetadata.json file
      from(getMetadataFilePath(project)) { }

      // Add the source zip
      from(getSourceZipFilePath(project)) { }

      // For Java projects, include the project jar into the libPath directory in the zip by default
      project.getPlugins().withType(JavaPlugin) {
        includeLibs(project, task, project.tasks.getByName("jar"));
      }

      // Include hadoopRuntime dependencies into the libPath directory in the zip
      includeLibs(project, task, hadoopZipConf);

      // Add the task to project artifacts
      project.artifacts.add("archives", task);

      // When everything is done, print out a message
      doLast {
        project.logger.lifecycle("Prepared Hadoop zip archive at: ${archivePath}");
      }
    }
    return zipTask;
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
   * Helper method to determine the location of the sources zip file. This helper method will make
   * it easy for subclasses to get (or customize) the file location.
   *
   * @param project The Gradle project
   * @return The path to the sources zip file
   */
  String getSourceZipFilePath(Project project) {
    return "${project.rootProject.buildDir}/distributions/${project.rootProject.name}-${project.rootProject.version}-sources.zip"
  }

  /**
   * Includes libs in the directory specified by azkaban.ZipLibDir property if present.
   *
   * @param project
   * @param spec
   * @param target
   */
  void includeLibs(Project project, CopySpec spec, Object target) {
    spec.from(target) {
      into hadoopZipExtension.libPath;
    }
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