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

package com.linkedin.gradle.oozie;

import com.linkedin.gradle.hadoopdsl.HadoopDslChecker;
import com.linkedin.gradle.oozie.checker.OozieRequiredFieldsChecker;
import org.gradle.api.Project;

class OozieDSLChecker extends HadoopDslChecker {

  /**
   * Constructor for the Hadoop DSL static checker.
   *
   * @param project The Gradle project
   */
  OozieDSLChecker(Project project) {
    super(project);
  }

  /**
   * Factory method to build the RequiredFieldsChecker check. Allows subclasses to provide a custom
   * implementation of this check.
   *
   * @return The RequiredFieldsChecker check
   */
  @Override
  OozieRequiredFieldsChecker makeRequiredFieldsChecker() {
    return new OozieRequiredFieldsChecker(project);
  }
}
