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
package com.linkedin.gradle.hdfs;

import java.security.AccessControlException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.gradle.api.Project;

/**
 * HDFS file system abstraction for the Hadoop Plugin that uses WebHDFS.
 */
class HdfsFileSystem {

  // File System URI: {SCHEME}://namenode:port/path/to/file
  protected URI clusterURI;

  // Hadoop configuration to use
  protected Configuration conf;

  // FileSystem object to interact with HDFS
  protected FileSystem fs;

  // Krb5.conf file
  protected File krb5Conf;

  // Reference to the Gradle project so we can access the logger
  protected Project project;

  /**
   * Create a HdfsFileSystem instance with simple authentication.
   *
   * @param project The Gradle project
   */
  HdfsFileSystem(Project project) {
    project.logger.info("Initialized HdfsFileSystem with authentication: simple");
    this.conf = new Configuration();
    conf.addDefaultResource("hadoop_core-site.xml");
    this.krb5Conf = null;
    this.project = project;
    project.logger.debug("Initialized conf with " + conf.toString());
  }

  /**
   * Create a HdfsFileSystem instance with Kerberos authentication.
   *
   * @param project The Gradle project
   * @param krb5Conf The krb5 conf file
   */
  HdfsFileSystem(Project project, File krb5Conf) {
    project.logger.info("Initialized HdfsFileSystem with authentication: Kerberos")
    this.conf = new Configuration();
    conf.addDefaultResource("hadoop_core-site.xml");
    this.krb5Conf = krb5Conf;
    this.project = project;
    project.logger.debug("Initialized conf with " + conf.toString());
  }

  /**
   * Initialize a new HdfsFileSystem instance after it is constructed.
   *
   * @param clusterURI URI whose authority section names the host, port, etc. for this HdfsFileSystem
   */
  void initialize(URI clusterURI) {
    validateURI(clusterURI);
    this.clusterURI = clusterURI;

    // If Kerberos is enabled then set Kerberos authentication.
    if (krb5Conf != null) {
      useKerberosAuthentication();
    }

    // Update the Hadoop configuration
    updateConfiguration();

    // Create filesystem with the conf
    fs = WebHdfsFileSystem.get(conf);
  }

  /**
   * Checks if the user has kinited.
   *
   * @return Whether the user has kinited or not
   */
  private boolean checkForKinit() throws AccessControlException {
    String[] command = ["klist", "-s"];

    def processBuilder = new ProcessBuilder();
    processBuilder.command(command);

    def process = processBuilder.start();
    process.waitFor();
    return (process.exitValue() == 0);
  }

  /**
   * Helper method to update the Hadoop configuration.
   */
  private void updateConfiguration() {
    // Set default filesystem as clusterURI
    conf.set("fs.defaultFS", clusterURI.toString());

    // Set UserGroupInformation with the updated conf.
    UserGroupInformation.setConfiguration(conf);
  }

  /**
   * Updates the configuration to use Kerberos authentication. The user must have already kinited
   * to use Kerberos authentication.
   *
   * @throws AccessControlException If the user has not kinited
   */
  private void useKerberosAuthentication() {
    if (!checkForKinit()) {
      project.logger.error("The user has not kinited. Please kinit first.");
      throw new AccessControlException("The user has not kinited");
    }
    conf.set("hadoop.security.authentication", "kerberos");
    System.setProperty("java.security.krb5.conf", krb5Conf.getAbsolutePath());
  }

  /**
   * Checks that the given URI is valid for use with HdfsFileSystem.
   * <p>
   * In particular, it checks that the URI represents a WebHDFS URI.
   *
   * @param clusterURI The cluster URI
   * @throws IOException If the URI is invalid for use with HdfsFileSystem
   */
  private void validateURI(URI clusterURI) {
    if (!"webhdfs".equals(clusterURI.getScheme())) {
      throw new IOException("Invalid scheme. Expected webhdfs, found ${clusterURI.getScheme()}");
    }
  }

  /**
   * The src file is on the local disk. Add it to FS at the given dst name and the source is kept
   * intact afterwards.
   *
   * @param src path
   * @param dst path
   */
  void copyFromLocalFile(Path src, Path dst) throws IOException {
    copyFromLocalFile(false, src, dst);
  }

  /**
   * The src file is on the local disk. Add it to FS at the given dst name.
   * delSrc indicates if the source should be removed
   *
   * @param delSrc whether to delete the src
   * @param src path
   * @param dst path
   */
  void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    copyFromLocalFile(delSrc, true, src, dst);
  }

  /**
   * The src file is on the local disk. Add it to FS at the given dst name.
   * delSrc indicates if the source should be removed
   *
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   */
  void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    project.logger.info("Copying from ${src.toString()} to HDFS path ${dst.toString()}");
    fs.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  /**
   * The src files are on the local disk. Add it to FS at the given dst name.
   * delSrc indicates if the source should be removed
   *
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param srcs array of paths which are source
   * @param dst path
   */
  void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    project.logger.info("Copying to HDFS path ${dst.toString()}");
    fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  /**
   * The zipFile is the zip file on the local system. This file is extracted to the destDirectory.
   * If destDirectory doesn't exist, then it is created.
   *
   * @param zipFile The zip file on local system
   * @param destDirectory The destination directory on hdfs where zipFile should be extracted
   * @throws IOException If zipFile cannot be extracted to destDirectory
   */
  void copyFromLocalZip(File zipFile, Path destDirectory) throws IOException {
    project.logger.info("Copying contents of zip file ${zipFile.toString()} to HDFS path ${destDirectory.toString()}");

    // Create the destination directory if it doesn't exist
    if (!exists(destDirectory)) {
      mkdir(destDirectory);
    }

    FileInputStream fileIn = null;
    ZipInputStream zipIn = null;

    try {
      fileIn = new FileInputStream(zipFile);
      zipIn = new ZipInputStream(fileIn);

      // Iterate over entries in the zip file
      ZipEntry entry = zipIn.getNextEntry();

      while (entry != null) {
        String filePath = destDirectory.toString() + File.separator + entry.getName();
        if (!entry.isDirectory()) {
          extractFile(zipIn, filePath);  // If the entry is a file, extract it
        }
        else {
          mkdir(new Path(filePath));     // If the entry is a directory, create it
        }
        // Close this zip entry and get the next entry
        zipIn.closeEntry();
        entry = zipIn.getNextEntry();
      }
    }
    finally {
      if (zipIn != null) {
        zipIn.close();
      }
      if (fileIn != null) {
        fileIn.close();
      }
    }
  }

  /**
   * Delete a file.
   *
   * @param p the path to delete
   * @return true if successful else false
   */
  boolean delete(Path p) {
    project.logger.info("Deleting HDFS path ${p.toString()}")
    return delete(p, true);
  }

  /**
   * Delete a file.
   *
   * @param p the path to delete
   * @param recursive true if delete recursively
   * @return true if successful else false
   */
  boolean delete(Path p, Boolean recursive) {
    project.logger.info("Deleting HDFS path ${p.toString()}")
    return fs.delete(p, recursive);
  }

  /**
   * Check if exists.
   *
   * @param p path to check
   * @return true if p exists else returns false
   */
  boolean exists(Path p) {
    return fs.exists(p);
  }

  /**
   * Gets the user's home directory
   *
   * @return The user's home directory path
   */
  String getHomeDirectory() {
    return fs.getHomeDirectory();
  }

  /**
   * Gets the current working directory
   *
   * @return The current working directory path
   */
  String getWorkingDirectory() {
    return fs.getWorkingDirectory();
  }

  /**
   * Makes a directory on HDFS with the default permissions.
   *
   * @return Whether the directory was created or not
   */
  String mkdir(Path p) throws IOException {
    project.logger.info("Creating HDFS path ${p.toString()}")
    return fs.mkdirs(p);
  }

  /**
   * Utility function to copy a zip input stream to a file.
   *
   * @param zipIn Input stream from a zip file
   * @param filePath File path where zipIn should be written
   * @throws IOException If zipIn cannot be written to filePath
   */
  private extractFile(ZipInputStream zipIn, String filePath) throws IOException {
    // Get the name of the file to print for logging
    String fileName = filePath.substring(filePath.lastIndexOf("/") + 1);
    project.logger.lifecycle("Copying ${fileName} from zip to HDFS path ${filePath}");

    // Get output stream from the filesystem
    OutputStream out = null;

    try {
      out = fs.create(new Path(filePath));
      // Copy the file to the hdfs
      IOUtils.copyBytes(zipIn, out, conf, false);
    }
    finally {
      // Close the output stream.
      if (out != null) {
        out.close();
      }
    }
  }
}