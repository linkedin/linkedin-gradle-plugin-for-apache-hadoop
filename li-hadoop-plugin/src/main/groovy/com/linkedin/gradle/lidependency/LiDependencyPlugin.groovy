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
import org.gradle.api.Project;

class LiDependencyPlugin extends DependencyPlugin {

  /**
   * Method to enable or disable dependency check. We enable the dependency check for linkedin.
   * @return true Enable the dependency check for linkedin
   */
  @Override
  boolean isDependencyCheckEnabled() {
    return true;
  }


  /**
  * Factory method to return the CheckDependency class. Subclasses can override this method to
  * return their own CheckDependency class.
  *
  * @return Class that implements the CheckDependency
  */
  @Override
  Class<? extends CheckDependencyTask> getCheckDependencyTask() {
    return LiCheckDependencyTask.class;
  }
}
