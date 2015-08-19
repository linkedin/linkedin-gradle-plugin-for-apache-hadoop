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
package com.linkedin.gradle.liutil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains the utility methods required for Kerberos authentication.
 **/
class LiKerberosUtil {

  static final String OOZIE_KRB5_CONF = "oozie-krb5.conf";
  static final Logger logger = LoggerFactory.getLogger(LiKerberosUtil.class);

  /**
   * Gets a reference to the oozie-krb5.conf file.
   *
   * @return The oozie-krb5.conf file
   */
  static File getKrb5File() {
    logger.debug("looking for oozie-krb5.conf file in ${System.getProperty('java.io.tmpdir')}");
    if (!getKrb5FileLocation().exists()) {
      logger.debug("oozie-krb5.conf was not found in ${System.getProperty('java.io.tmpdir')}");
      writeConfToTemp();
    }
    return getKrb5FileLocation();
  }

  /**
   * Gets a reference to the oozie-krb5.conf file inside a temp directory.
   *
   * @return Reference to the oozie-krb5.conf file
   */
  static File getKrb5FileLocation() {
    return new File(System.getProperty("java.io.tmpdir"), OOZIE_KRB5_CONF);
  }

  /**
   * Copies the oozie-krb5.conf file to a temporary location since we cannot point the property
   * "java.security.krb5.conf" to the file inside the jar.
   */
  static void writeConfToTemp() {
    logger.debug("writing oozie-krb5.conf file to ${System.getProperty("java.io.tmpdir")}");
    Thread.currentThread().getContextClassLoader().getResource(OOZIE_KRB5_CONF).withInputStream { inputStream ->
      getKrb5FileLocation().withOutputStream { outputStream ->
        outputStream << inputStream
      }
    }
  }
}