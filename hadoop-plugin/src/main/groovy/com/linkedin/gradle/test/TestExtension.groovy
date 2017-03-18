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
package com.linkedin.gradle.test;

import com.linkedin.gradle.hadoopdsl.HadoopDslExtension;
import com.linkedin.gradle.hadoopdsl.NamedScope;

import org.gradle.api.Project;

/**
 * Test extension for testing the hadoop block.
 *
 * The TestExtension is similar to the HadoopDslExtension with a name.
 */
class TestExtension extends HadoopDslExtension {
  String name;

  /**
   * Constructor for the TestExtension
   *
   * @param name The name of the test
   * @param project The Gradle Project
   * @param parentScope The parent scope
   */
  TestExtension(String name, Project project, NamedScope parentScope) {
    super(project, parentScope, name);
    this.name = name;
  }
}
