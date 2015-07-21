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
package com.linkedin.gradle.hadoop;

import com.linkedin.gradle.azkaban.AzkabanPlugin;
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin;
import com.linkedin.gradle.pig.PigPlugin;
import com.linkedin.gradle.scm.ScmPlugin;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * HadoopPlugin is the class that implements our Gradle Plugin.
 */
class HadoopPlugin implements Plugin<Project> {
  /**
   * Applies the Hadoop Plugin, which in turn applies the Hadoop DSL, Azkaban, Apache Pig and SCM
   * plugins.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    project.getPlugins().apply(getHadoopDslPluginClass());
    project.getPlugins().apply(getAzkabanPluginClass());
    project.getPlugins().apply(getPigPluginClass());
    project.getPlugins().apply(getScmPluginClass());
  }

  /**
   * Factory method to return the AzkabanPlugin class. Subclasses can override this method to
   * return their own AzkabanPlugin class.
   *
   * @return Class that implements the AzkabanPlugin
   */
  Class<? extends AzkabanPlugin> getAzkabanPluginClass() {
    return AzkabanPlugin.class;
  }

  /**
   * Factory method to return the HadoopDslPlugin class. Subclasses can override this method to
   * return their own HadoopDslPlugin class.
   *
   * @return Class that implements the HadoopDslPlugin
   */
  Class<? extends HadoopDslPlugin> getHadoopDslPluginClass() {
    return HadoopDslPlugin.class;
  }

  /**
   * Factory method to return the PigPlugin class. Subclasses can override this method to return
   * their own PigPlugin class.
   *
   * @return Class that implements the PigPlugin
   */
  Class<? extends PigPlugin> getPigPluginClass() {
    return PigPlugin.class;
  }

  /**
   * Factory method to return the ScmPlugin class. Subclasses can override this method to return
   * their own ScmPlugin class.
   *
   * @return Class that implements the ScmPlugin
   */
  Class<? extends ScmPlugin> getScmPluginClass() {
    return ScmPlugin.class;
  }
}