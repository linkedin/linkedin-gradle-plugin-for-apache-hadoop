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
package com.linkedin.gradle.lizip;

import com.linkedin.gradle.zip.HadoopZipExtension;
import com.linkedin.gradle.zip.HadoopZipPlugin;

import org.gradle.api.Project;

/**
 * LinkedIn-specific customizations to the Hadoop Zip Plugin.
 */
class LiHadoopZipPlugin extends HadoopZipPlugin {
  /**
   * Applies the LiHadoopZipPlugin.
   * <p>
   * In the LiHadoopZipPlugin, any azkabanZip tasks (from the li-azkaban2 plugin in RUM) are made
   * to be dependent on the buildScmMetadata task, and to include the metadata JSON file.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    super.apply(project);

    // Enable users to skip the plugin
    if (project.hasProperty("disableHadoopZipPlugin")) {
      println("HadoopZipPlugin disabled");
      return;
    }
  }

  /**
   * Helper method to create the Hadoop zip extension. Having this method allows for the unit tests
   * to override it.
   *
   * @param project The Gradle project
   * @return The Hadoop zip extension
   */
  @Override
  HadoopZipExtension createZipExtension(Project project) {
    LiHadoopZipExtension extension = new LiHadoopZipExtension(project);
    project.extensions.add("hadoopZip", extension);
    return extension;
  }
}
