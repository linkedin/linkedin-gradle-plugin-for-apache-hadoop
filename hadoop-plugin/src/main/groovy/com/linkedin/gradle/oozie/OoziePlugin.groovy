package com.linkedin.gradle.oozie;

import groovy.json.JsonBuilder;
import groovy.json.JsonSlurper;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;

/** OoziePlugin implements features for Plugin **/
class OoziePlugin implements Plugin<Project> {

  /**
   *  Applies the OoziePlugin.
   *  @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    createOozieUploadTask(project);
    createWriteOoziePluginJsonTask(project);
  }

  /**
   * Creates oozieUpload task
   * @param project
   * @return the created task
   */
  Task createOozieUploadTask(Project project) {
    return project.tasks.create(name: "oozieUpload", type: OozieUploadTask) { task ->
      description = "Uploads the oozie project folder to the hdfs.";
      group = "Hadoop Plugin";

      doFirst{
        oozieProject = readOozieProject(project);
      }
    }
  }

  /**
   * Creates writeOoziePluginJson task
   * @param project
   * @return the created task
   */
  Task createWriteOoziePluginJsonTask(Project project) {
    return project.tasks.create("writeOoziePluginJson") {
      description = "Creates a .ooziePlugin.json file in the project directory with default properties."
      group = "Hadoop plugin";

      doLast {
        def ooziePluginFilePath = "${project.getProjectDir()}/.ooziePlugin.json";
        if(!new File(ooziePluginFilePath).exists()) {
          String oozieData = new JsonBuilder(new OozieProject()).toPrettyString();
          logger.debug("Writing oozie data $oozieData");
          new File(ooziePluginFilePath).write(oozieData);
        }
      }
    }
  }

/**
 * Helper method to read the plugin json file as a JSON object.
 *
 * @param project The Gradle project
 * @return A JSON object or null if the file does not exist
 */
  def readOoziePluginJson(Project project) {
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

  /**
   * Helper method to determine the location of the plugin json file. This helper method will make
   * it easy for subclasses to get (or customize) the file location.
   *
   * @param project The Gradle project
   * @return The path to the plugin json file
   */
  String getPluginJsonPath(Project project) {
    return "${project.getProjectDir()}/.ooziePlugin.json";
  }

  /**
   * Load the Oozie project properties defined in the .ooziePlugin.json file.
   *
   * @return An OozieProject object with the properties set
   */
  OozieProject readOozieProject(Project project) {
    def pluginJson = readOoziePluginJson(project);

    if (pluginJson == null) {
      throw new GradleException("\n\nPlease run \"gradle writeOoziePluginJson\" to create a default .ooziePlugin.json file in your project directory which you can then edit.\n")
    }
    OozieProject oozieProject = new OozieProject();
    oozieProject.clusterURI = pluginJson[OozieConstants.OOZIE_CLUSTER_URI]
    oozieProject.uploadPath = pluginJson[OozieConstants.PATH_TO_UPLOAD]
    oozieProject.projectName = pluginJson[OozieConstants.OOZIE_PROJECT_NAME]
    oozieProject.dirToUpload = pluginJson[OozieConstants.DIR_TO_UPLOAD]
    return oozieProject;
  }
}