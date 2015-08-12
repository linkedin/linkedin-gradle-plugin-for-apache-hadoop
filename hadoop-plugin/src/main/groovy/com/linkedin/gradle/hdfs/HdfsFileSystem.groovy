package com.linkedin.gradle.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.AccessControlException;

/** A filesystem for hdfs over the web */
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
   * Create a HdfsFileSystem instance
   */
  public HdfsFileSystem() {
    krb5Conf = null;
    logger.info("Initialized HdfsFileSystem with authentication: simple")
    conf = new Configuration();
    logger.debug("initialized conf with " + conf.toString());
  }

  /**
   * Create a HdfsFileSystem instance with kerberos authentication
   * @param krb5Conf the krb5 conf file
   */
  public HdfsFileSystem(File krb5Conf) {
    this.krb5Conf = krb5Conf;
    logger.info("Initialized HdfsFileSystem with authentication: kerberos")
    conf = new Configuration();
    logger.debug("initialized conf with " + conf.toString());
  }

  /** Called after a new FileSystem instance is constructed.
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
 * Check if the webhdfs is used.
 * @param clusterURI the cluster URI
 * @throws IOException
 */
  private void validateURI(URI clusterURI) throws IOException {
    if(clusterURI.getScheme()!="webhdfs") {
      throw new IOException("Invalid scheme. Expected webhdfs, found ${clusterURI.getScheme()}");
    }
  }

  /**
   * Update Configuration to use kerberos authentication.
   */
  private void setKerberosAuthentication() {
    conf.set("hadoop.security.authentication", "kerberos");
    System.setProperty("java.security.krb5.conf", krb5Conf.getAbsolutePath());
    checkForKinit();
  }

  /**
   * Check if user has kinited
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
   * Make directory
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
   * The src file is on the local disk.  Add it to FS at
   * the given dst name and the source is kept intact afterwards
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(Path src, Path dst)
    throws IOException {
    copyFromLocalFile(false, src, dst);
  }
  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(boolean delSrc, Path src, Path dst)
    throws IOException {
    copyFromLocalFile(delSrc, true, src, dst);
  }

  /**
   * The src files are on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param srcs array of paths which are source
   * @param dst path
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path[] srcs, Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, srcs, dst);
  }

  /**
   * The src file is on the local disk.  Add it to FS at
   * the given dst name.
   * delSrc indicates if the source should be removed
   * @param delSrc whether to delete the src
   * @param overwrite whether to overwrite an existing file
   * @param src path
   * @param dst path
   */
  public void copyFromLocalFile(boolean delSrc, boolean overwrite,
                                Path src, Path dst)
    throws IOException {
    fs.copyFromLocalFile(delSrc, overwrite, src, dst);
  }

  /**
   * @param p path to check
   * @return true if p exists else returns false
   */
  public Boolean exists(Path p) {
    fs.exists(p);
  }
}
