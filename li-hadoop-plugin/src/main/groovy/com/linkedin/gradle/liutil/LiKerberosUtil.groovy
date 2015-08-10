package com.linkedin.gradle.liutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class contains the utility methods required for kerberos authentication **/
class LiKerberosUtil {

  static Logger logger = LoggerFactory.getLogger(LiKerberosUtil.class);
  private static final String OOZIE_KRB5_CONF = "oozie-krb5.conf";

  /**
   * We need to copy the oozie-krb5.conf file to a temporary location since we cannot point property java.security.krb5.conf to the file inside jar.
   */
  static void writeConfToTemp() {
    logger.debug("writing oozie-krb5.conf file to ${System.getProperty("java.io.tmpdir")}");
    Thread.currentThread().getContextClassLoader().getResource(OOZIE_KRB5_CONF).withInputStream { inputStream ->
      getKrb5FileLocation().withOutputStream { outputStream ->
        outputStream << inputStream
      }
    }
  }

  /**
   * @return oozie-krb5.conf file
   */
  static File getKrb5FileLocation() {
    logger.debug("looking for oozie-krb5.conf file in " + System.getProperty("java.io.tmpdir"));
    File krb5File = new File(System.getProperty("java.io.tmpdir"),OOZIE_KRB5_CONF);
    if (!krb5File.exists()) {
      logger.debug("oozie-krb5.conf was not found in ${System.getProperty("java.io.tmpdir")}");
      writeConfToTemp();
    }
    return new File(System.getProperty("java.io.tmpdir"),OOZIE_KRB5_CONF);
  }
}
