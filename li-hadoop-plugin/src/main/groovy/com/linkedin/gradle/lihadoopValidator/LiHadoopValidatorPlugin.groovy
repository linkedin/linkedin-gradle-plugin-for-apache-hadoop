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
package com.linkedin.gradle.lihadoopValidator;

import com.linkedin.gradle.hadoopValidator.HadoopValidatorPlugin;
import com.linkedin.gradle.hadoopValidator.PigValidator.PigValidatorPlugin;
import com.linkedin.gradle.hadoopValidator.ValidatorConstants;
import com.linkedin.gradle.lihadoopValidator.liPigValidator.LiPigValidatorPlugin;

/**
 * LinkedIn-specific customizations to the Hadoop Validator Plugin.
 */
class LiHadoopValidatorPlugin extends HadoopValidatorPlugin {

  /**
   * Factory method to return the LiPigValidatorPlugin class. Subclasses can override this method to return
   * their own LiPigValidatorPlugin class.
   *
   * @return Class that implements the LiPigValidatorPlugin
   */
  @Override
  Class<? extends PigValidatorPlugin> getPigValidatorPlugin() {
    return LiPigValidatorPlugin.class;
  }

  /**
   * Factory method which sets the Validator properties to configure the plugin.
   *
   * @param properties
   */
  @Override
  void setValidatorProperties(Properties properties) {
    properties.setProperty(ValidatorConstants.NAME_NODE, "hdfs://ltx1-holdemnn01.grid.linkedin.com:9000");
    properties.setProperty(ValidatorConstants.REPOSITORY_URL, "http://artifactory.corp.linkedin.com:8081/artifactory/ext-libraries");
  }
}
