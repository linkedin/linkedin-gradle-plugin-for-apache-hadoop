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
package com.linkedin.gradle.lihadoopValidator.liPigValidator;

import com.linkedin.gradle.hadoopValidator.PigValidator.PigDataValidator;
import com.linkedin.gradle.lihadoopValidator.LiHdfsFileSystem;

class LiPigDataValidator extends PigDataValidator {
  /**
   * Initializes LiHdfsFilesystem for WebHdfsAccess in order to check validity of dependencies
   * Subclasses may override this method to provide their own LiHdfsFileSystem
   *
   * @param krb5 the kerberos configuration file to configure kerberos access
   */
  @Override
  void initHdfsFileSystem(File krb5) {
    fileSystem = new LiHdfsFileSystem(project, krb5);
  }

  /**
   * Gives the resolved pathName. Organizations may use their own path formats which need to be resolved to standard pathnames
   *
   * @param pathName The pathname to be resolved
   * @return pathName The resolved pathName
   */
  @Override
  String getPath(String pathName) {
    return fileSystem.getLatestPath(pathName);
  }
}
