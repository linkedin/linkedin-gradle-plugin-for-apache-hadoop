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

import org.gradle.api.Project;

/**
 * This class contains the utility methods required for Kerberos authentication.
 **/
class LiKerberosUtil {

  static final String OOZIE_KRB5_CONF = "oozie-krb5.conf";

  /**
   * Gets a reference to the oozie-krb5.conf file.
   *
   * @param The Gradle project
   * @return The oozie-krb5.conf file
   */
  static File getKrb5File(Project project) {
    if (!getKrb5FileLocation().exists()) {
      writeConfToTemp(project);
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
   *
   * @param The Gradle project
   */
  static void writeConfToTemp(Project project) {
    project.logger.info("Writing oozie-krb5.conf file to ${System.getProperty('java.io.tmpdir')}");

    Thread.currentThread().getContextClassLoader().getResource(OOZIE_KRB5_CONF).withInputStream { inputStream ->
      getKrb5FileLocation().withOutputStream { outputStream ->
        outputStream << inputStream
      }
    }
  }
}