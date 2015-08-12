package com.linkedin.gradle.oozie;

import com.linkedin.gradle.hdfs.HdfsFileSystem;
import org.apache.hadoop.fs.Path;
import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.TaskAction;

/**
 * OozieUploadTask will upload the project to the hdfs.
 */
class OozieUploadTask extends DefaultTask {

  protected OozieProject oozieProject;
  protected HdfsFileSystem fs;

  @TaskAction
  void upload() {

    // create and initialize HdfsFileSystem
    fs = getHdfsFileSystem();
    fs.initialize(getURI());

    Path directoryPath = new Path(oozieProject.dirToUpload);
    Path projectPath = new Path(oozieProject.uploadPath + oozieProject.projectName + "/v${getProject().version}");

    try {
      logger.info("Project path: ${projectPath.toString()}");
      logger.info("Directory path: ${directoryPath.toString()}");
      if(fs.exists(projectPath)) {
        throw new IOException("$projectPath already exists")
      }
      fs.mkdir(projectPath);
      fs.copyFromLocalFile(directoryPath, projectPath);
    }
    catch (IOException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * @return clusterURI
   */
  URI getURI() {
    return new URI(oozieProject.clusterURI);
  }

  /**
   * @return new instance of HdfsFileSystem
   */
  HdfsFileSystem getHdfsFileSystem(){
    return new HdfsFileSystem();
  }
}
