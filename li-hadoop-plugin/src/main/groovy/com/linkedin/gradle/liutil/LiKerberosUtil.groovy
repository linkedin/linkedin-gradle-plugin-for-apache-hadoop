package com.linkedin.gradle.liutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class contains the utility methods required for kerberos authentication **/
class LiKerberosUtil {

  static Logger logger = LoggerFactory.getLogger(LiKerberosUtil.class);
  private static final String OOZIE_KRB5_CONF = "oozie-krb5.conf";
  private static final String JAVA_TMP_DIR = System.getProperty("java.io.tmpdir");

  /**
   * We need to copy the oozie-krb5.conf file to a temporary location since we cannot point property java.security.krb5.conf to the file inside jar.
   */
  static void writeConfToTemp() {
    logger.debug("writing oozie-krb5.conf file to ${JAVA_TMP_DIR}");
    Thread.currentThread().getContextClassLoader().getResource(OOZIE_KRB5_CONF).withInputStream { inputStream ->
      getKrb5FileLocation().withOutputStream { outputStream ->
        outputStream << inputStream
      }
    }
  }

  /**
   * @return oozie-krb5.conf file
   */
  static File getKrb5File() {
    logger.debug("looking for oozie-krb5.conf file in ${JAVA_TMP_DIR}");
    if (!getKrb5FileLocation().exists()) {
      logger.debug("oozie-krb5.conf was not found in ${JAVA_TMP_DIR}");
      writeConfToTemp();
    }
    return getKrb5FileLocation();
  }

  /**
   * @return location of oozie-krb5.conf file.
   */
  static File getKrb5FileLocation() {
    return new File(JAVA_TMP_DIR,OOZIE_KRB5_CONF);
  }
}
