package com.linkedin.gradle.lioozie;

import com.linkedin.gradle.oozie.OoziePlugin;
import org.gradle.api.Project;
import org.gradle.api.Task;

/**
 * Linkedin specific customizations to the OoziePlugin
 */
class LiOoziePlugin extends OoziePlugin {

  /**
   * Override the createOozieUploadTask method to make it of type LiOozieUploadTask instead of OozieUploadTask.
   * @param project
   * @return the created task
   */
  @Override
  Task createOozieUploadTask(Project project) {
    return project.tasks.create(name: "oozieUpload", type: LiOozieUploadTask) { task ->
      description = "Uploads oozie project folder to the hdfs.";
      group = "Hadoop Plugin";

      doFirst{
        oozieProject = super.readOozieProject(project);
      }
    }
  }
}
