package com.linkedin.gradle.lioozie;

import com.linkedin.gradle.hdfs.HdfsFileSystem;
import com.linkedin.gradle.liutil.LiKerberosUtil;
import com.linkedin.gradle.oozie.OozieUploadTask;

/**
 * Linkedin specific customizations to the OozieUploadTask
 */
class LiOozieUploadTask extends OozieUploadTask {

  /**
   * Override the getHdfsFileSystem method to use kerberos authenticated HdfsFileSystem.
   * @return new instance of kerberos authenticated HdfsFileSystem.
   */
  @Override
  HdfsFileSystem getHdfsFileSystem(){
    return new HdfsFileSystem(LiKerberosUtil.getKrb5File());
  }
}
