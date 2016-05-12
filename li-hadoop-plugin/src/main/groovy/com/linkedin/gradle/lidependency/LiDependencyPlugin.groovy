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
package com.linkedin.gradle.lidependency;

import com.linkedin.gradle.dependency.CheckDependencyTask;
import com.linkedin.gradle.dependency.DependencyPlugin;
import com.linkedin.gradle.dependency.DisallowLocalDependencyTask;

/**
 * LinkedIn-specific customizations to the Dependency Plugin.
 */
class LiDependencyPlugin extends DependencyPlugin {
  /**
   * Method to check if the dependency check is enabled. We enable the dependency check for
   * LinkedIn.
   *
   * @return Whether or not the dependency check is enabled
   */
  @Override
  boolean isDependencyCheckEnabled() {
    return true;
  }

  /**
   * Factory method to return the CheckDependencyTask class. Subclasses can override this method to
   * return their own CheckDependencyTask class.
   *
   * @return Class that extends the CheckDependencyTask class
   */
  @Override
  Class<? extends CheckDependencyTask> getCheckDependencyTask() {
    return LiCheckDependencyTask.class;
  }

  /**
   * Factory method to return the DisallowLocalDependencyTask class. Subclasses can override this
   * method to return their own DisallowLocalDependencyTask class.
   *
   * @return Class that extends the DisallowLocalDependencyTask class
   */
  @Override
  Class<? extends DisallowLocalDependencyTask> getDisallowLocalDependencyTask() {
    return LiDisallowLocalDependencyTask.class;
  }
}
