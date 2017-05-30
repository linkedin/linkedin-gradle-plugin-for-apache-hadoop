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
package com.linkedin.gradle.livalidator.lipig

import com.linkedin.gradle.validator.pig.PigDataValidator
import com.linkedin.gradle.validator.pig.PigDependencyValidator
import com.linkedin.gradle.validator.pig.PigValidatorPlugin

/**
 * LinkedIn-specific customizations to the Hadoop Validator Plugin.
 */
class LiPigValidatorPlugin extends PigValidatorPlugin {
  /**
   * Factory method to return the LiPigDataValidator Task class. Subclasses can override this
   * method to return their own LiPigDataValidator Task class.
   *
   * @return Class that implements the PigDataValidator Task
   */
  @Override
  Class<? extends PigDataValidator> getDataValidatorClass() {
    return LiPigDataValidator.class
  }

  /**
   * Factory method to return the LiPigDependencyValidator Task class. Subclasses can override this
   * method to return their own LiPigDependencyValidator Task class.
   *
   * @return Class that implements the LiPigDependencyValidator Task
   */
  @Override
  Class<? extends PigDependencyValidator> getDependencyValidatorClass() {
    return LiPigDependencyValidator.class
  }
}
