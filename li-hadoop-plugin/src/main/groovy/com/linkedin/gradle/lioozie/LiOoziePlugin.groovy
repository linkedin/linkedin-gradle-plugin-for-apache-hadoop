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
package com.linkedin.gradle.lioozie;

import com.linkedin.gradle.lihadoop.LiHadoopProperties;
import com.linkedin.gradle.oozie.OozieCommandTask;
import com.linkedin.gradle.oozie.OoziePlugin;
import com.linkedin.gradle.oozie.OozieProject;
import com.linkedin.gradle.oozie.OozieUploadTask;

import org.gradle.api.Project;

/**
 * Linkedin specific customizations to the OoziePlugin.
 */
class LiOoziePlugin extends OoziePlugin {

  private static final String HLD_NAME_NODE_WEBHDFS = LiHadoopProperties.get(LiHadoopProperties.HLD_NAME_NODE_WEBHDFS)
  private static final String OOZIE_URI = LiHadoopProperties.get(LiHadoopProperties.OOZIE_URI)

  /**
   * Factory method to return the OozieUploadTask class. Subclasses can override this method to
   * return their own OozieUploadTask class.
   *
   * @return Class that implements the OozieUploadTask
   */
  @Override
  Class<? extends OozieUploadTask> getOozieUploadTaskClass() {
    return LiOozieUploadTask.class;
  }

  /**
   * Factory method to return the OozieCommandTask class. Subclasses can override this method to
   * return their own OozieUploadTask class;
   * @return The OozieCommandTask class
   */
  @Override
  Class<? extends OozieCommandTask> getOozieCommandTaskClass() {
    return LiOozieCommandTask.class;
  }

  /**
   * Factory method to build a default OozieProject for use with the writePluginJson method. Can be
   * overridden by subclasses.
   * <p>
   * The LinkedIn override of this method sets a couple of properties to common LinkedIn values.
   *
   * @param project The Gradle project
   * @return The OozieProject object
   */
  @Override
  OozieProject makeDefaultOozieProject(Project project) {
    OozieProject oozieProject = makeOozieProject(project);
    oozieProject.clusterURI = HLD_NAME_NODE_WEBHDFS;
    oozieProject.oozieURI = OOZIE_URI;
    oozieProject.oozieZipTask = "";
    oozieProject.projectName = "";
    oozieProject.uploadPath = "/user/${System.getProperty('user.name')}";
    return oozieProject;
  }
}
