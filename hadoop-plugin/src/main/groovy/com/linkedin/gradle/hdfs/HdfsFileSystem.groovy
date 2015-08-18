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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.AccessControlException;

/**
 * A filesystem for HDFS over the web
 */
class HdfsFileSystem {

  // FileSystem object to interact with hdfs
  protected FileSystem fs;
  // Configuration to use
  protected Configuration conf;
  // File System URI: {SCHEME}://namenode:port/path/to/file
  protected URI clusterURI;
  // Krb5.conf file
  protected File krb5Conf;

  Logger logger = LoggerFactory.getLogger(HdfsFileSystem.class);

  /**
   * Create a HdfsFileSystem instance.
   */
  public HdfsFileSystem() {
    krb5Conf = null;
    logger.info("Initialized HdfsFileSystem with authentication: simple")
    conf = new Configuration();
    logger.debug("initialized conf with " + conf.toString());
  }

  /**
   * Create a HdfsFileSystem instance with Kerberos authentication.
   *
   * @param krb5Conf The krb5 conf file
   */
  public HdfsFileSystem(File krb5Conf) {
    this.krb5Conf = krb5Conf;
    logger.info("Initialized HdfsFileSystem with authentication: kerberos")
    conf = new Configuration();
    logger.debug("initialized conf with " + conf.toString());
  }

  /**
   * Called after a new FileSystem instance is constructed.
   *
   * @param clusterURI a uri whose authority section names the host, port, etc. for this FileSystem
   * @param conf the configuration
   */
  public initialize(URI clusterURI) {

    logger.debug("Cluster URI ${clusterURI}, conf: ${conf.toString()}");

    // set clusterURI
    this.clusterURI = clusterURI;

    // validate the URI
    validateURI(clusterURI);

    // if kerberos is enabled then set kerberos authentication.
    if(krb5Conf!=null) {
      setKerberosAuthentication();
    }

    // set configuration values
    setConfiguration();

    // create filesystem with the conf
    fs = WebHdfsFileSystem.get(conf);
  }

  /**
   * Checks if webhdfs is used.
   *
   * @param clusterURI The cluster URI
   * @throws IOException
   */
  private void validateURI(URI clusterURI) throws IOException {
    if(clusterURI.getScheme()!="webhdfs") {
      throw new IOException("Invalid scheme. Expected webhdfs, found ${clusterURI.getScheme()}");
    }
  }

  /**
   * Updates the configuration to use Kerberos authentication.
   */
  private void setKerberosAuthentication() {
    conf.set("hadoop.security.authentication", "kerberos");
    System.setProperty("java.security.krb5.conf", krb5Conf.getAbsolutePath());
    checkForKinit();
  }

  /**
   * Checks if the user has kinited.
   *
   * @throws AccessControlException
   */
  void checkForKinit() throws AccessControlException {
    def processBuilder = new ProcessBuilder();
    String[] command = ["klist", "-s"];
    processBuilder.command(command);
    def process = processBuilder.start();
    process.waitFor();
    if (process.exitValue() != 0) {
      logger.error(" The user has not kinited...please kinit.")
      throw new AccessControlException("The user has not kinited")
    }
  }

  private setConfiguration() {
    // set default filesystem as clusterURI
    conf.set("fs.defaultFS", clusterURI.toString());
    // set UserGroupInformation with the updated conf.
    UserGroupInformation.setConfiguration(conf);
  }

  /**
   * Makes a directory on HDFS.
   *
   * @return true or false
   */
  public String mkdir(Path p) throws IOException {
    logger.info("mkdir called on path ${p.toString()}")
    return fs.mkdirs(p);
  }

  /**
   * The home directory
   * @return home directory
   */
  public String getHomeDirectory() {
    return fs.getHomeDirectory();
  }

  /**
   * The working directory
   * @return working directory
   */
  public String getWorkingDirectory() {
    return fs.getWorkingDirectory();
  }

  /**
   * The src file is on the local disk. Add it to FS at the given dst name and the source is kept
   * intact afterwards.
   * 
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(Path src, Path dst) throws IOException {
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
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst) throws IOException {
    copyFromLocalFile(delSrc, true, src, dst);
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
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst) throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
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
  public void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst) throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  /**
   * @param p path to check
   * @return true if p exists else returns false
   */
  public boolean exists(Path p) {
    return fs.exists(p);
  }

  /**
   * @param p the path to delete
   * @param recursive true if delete recursively
   * @return true if successful else false
   */
  public boolean delete(Path p, Boolean recursive) {
    logger.info("deleting ${p.toString()}")
    return fs.delete(p,recursive);
  }

  /**
   * @param p the path to delete
   * @return true if successful else false
   */
  public boolean delete(Path p) {
    return delete(p,true);
  }
}
