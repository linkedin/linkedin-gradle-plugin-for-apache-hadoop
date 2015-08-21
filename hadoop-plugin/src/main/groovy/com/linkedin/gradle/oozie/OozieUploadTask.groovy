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
package com.linkedin.gradle.oozie;

import com.linkedin.gradle.hdfs.HdfsFileSystem;

import org.apache.hadoop.fs.Path;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;

/**
 * The OozieUploadTask will upload the project to HDFS to a versioned directory.
 */
class OozieUploadTask extends DefaultTask {

  // Reference to the HdfsFileSystem.
  protected HdfsFileSystem fs;

  // Reference to the Oozie project.
  protected OozieProject oozieProject;

  @TaskAction
  void upload() {
    // Create and initialize the HdfsFileSystem
    fs = makeHdfsFileSystem();
    fs.initialize(getURI());

    File zipFile = project.getProject().tasks[oozieProject.oozieZipTask].archivePath;
    Path projectPath = new Path("${oozieProject.uploadPath}/${oozieProject.projectName}/${project.version}".toString());
    logger.lifecycle("Uploading contents of ${zipFile.toString()} to HDFS path ${projectPath.toString()}");

    // Delete the directory if it exists
    if (fs.exists(projectPath)) {
      fs.delete(projectPath);
    }

    // Copy the zip contents to projectPath
    fs.copyFromLocalZip(zipFile, projectPath);
    logger.lifecycle("Zip file contents uploaded successfully to HDFS path ${projectPath.toString()}");
  }

  /**
   * Helper method to get the cluster URI.
   *
   * @return The cluster URI
   */
  URI getURI() {
    return new URI(oozieProject.clusterURI);
  }

  /**
   * Factory method to get a new HdfsFileSystem. Subclasses can override this method.
   *
   * @return A new instance of HdfsFileSystem
   */
  HdfsFileSystem makeHdfsFileSystem() {
    return new HdfsFileSystem(project);
  }
}