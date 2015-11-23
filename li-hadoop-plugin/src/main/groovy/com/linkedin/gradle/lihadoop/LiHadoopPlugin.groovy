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
package com.linkedin.gradle.lihadoop;

import com.linkedin.gradle.azkaban.AzkabanPlugin
import com.linkedin.gradle.hadoop.HadoopPlugin;
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin
import com.linkedin.gradle.oozie.OoziePlugin;
import com.linkedin.gradle.pig.PigPlugin;
import com.linkedin.gradle.scm.ScmPlugin;
import com.linkedin.gradle.spark.SparkPlugin;
import com.linkedin.gradle.zip.HadoopZipPlugin;

import com.linkedin.gradle.liazkaban.LiAzkabanPlugin;
import com.linkedin.gradle.lidependency.LiDependencyPlugin;
import com.linkedin.gradle.lihadoopdsl.LiHadoopDslPlugin;
import com.linkedin.gradle.lioozie.LiOoziePlugin;
import com.linkedin.gradle.lipig.LiPigPlugin;
import com.linkedin.gradle.liscm.LiScmPlugin;
import com.linkedin.gradle.lispark.LiSparkPlugin;
import com.linkedin.gradle.lizip.LiHadoopZipPlugin;

import org.gradle.api.Project
import org.gradle.api.Task;

/**
 * LinkedIn-specific customizations to the Hadoop Plugin.
 */
class LiHadoopPlugin extends HadoopPlugin {
  /**
   * Factory method to return the LiAzkabanPlugin class. Subclasses can override this method to
   * return their own AzkabanPlugin class.
   *
   * @return Class that implements the AzkabanPlugin
   */
  @Override
  Class<? extends AzkabanPlugin> getAzkabanPluginClass() {
    return LiAzkabanPlugin.class;
  }

  /**
   * Factory method to return the LiDependencyPlugin class. Subclasses can override this method to
   * return their own DependencyPlugin.
   *
   * @return Class that implements the DependencyPlugin
   */
  Class<? extends LiDependencyPlugin> getDependencyPluginClass() {
    return LiDependencyPlugin.class;
  }

  /**
   * Returns the LinkedIn-specific LiHadoopDslPlugin class. Subclasses can override this method to
   * return their own HadoopDslPlugin class.
   *
   * @return Class that implements the HadoopDslPlugin
   */
  @Override
  Class<? extends HadoopDslPlugin> getHadoopDslPluginClass() {
    return LiHadoopDslPlugin.class;
  }

  /**
   * Returns the LinkedIn-specific LiHadoopZipPlugin class. Subclasses can override this method to
   * return their own HadoopZipPlugin class.
   *
   * @return Class that implements the HadoopZipPlugin
   */
  @Override
  Class<? extends HadoopZipPlugin> getHadoopZipPluginClass() {
    return LiHadoopZipPlugin.class;
  }

  /**
   * Factory method to return the LiOoziePlugin class. Subclasses can override this method to return
   * their own OoziePlugin class.
   *
   * @return Class that implements the LiOoziePlugin
   */
  @Override
  Class<? extends OoziePlugin> getOoziePluginClass() {
    return LiOoziePlugin.class;
  }

  /**
   * Returns the LinkedIn-specific PigPlugin class. Subclasses can override this method to return
   * their own PigPlugin class.
   *
   * @return Class that implements the PigPlugin
   */
  @Override
  Class<? extends PigPlugin> getPigPluginClass() {
    return LiPigPlugin.class;
  }

  /**
   * Returns the LinkedIn-specific LiScmPlugin class. Subclasses can override this method to return
   * their own ScmPlugin class.
   *
   * @return Class that implements the ScmPlugin
   */
  @Override
  Class<? extends ScmPlugin> getScmPluginClass() {
    return LiScmPlugin.class;
  }

  /**
   * Returns the LinkedIn-specific LiSparkPlugin class. Subclasses can override this method to
   * return their own SparkPlugin class.
   *
   * @return Class that implements the SparkPlugin
   */
  @Override
  Class<? extends SparkPlugin> getSparkPluginClass() {
    return LiSparkPlugin.class;
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  @Override
  void setupDependencyPluginTaskDependencies(Project project) {
    super.setupDependencyPluginTaskDependencies(project);

    // Check dependencies before you start building any Hadoop zips.
    Task checkDependenciesTask = project.tasks.findByName("checkDependencies");
    Task startHadoopZipsTask = project.tasks.findByName("startHadoopZips");

    if (checkDependenciesTask != null && startHadoopZipsTask != null) {    
      startHadoopZipsTask.dependsOn(checkDependenciesTask);
    }
  }
}
