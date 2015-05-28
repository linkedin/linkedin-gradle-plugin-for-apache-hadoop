/*
 * Copyright 2014 LinkedIn Corp.
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
package com.linkedin.gradle.hadoopdsl;

import org.gradle.api.Project;

/**
 * A namespace is a convenience for establishing a new level of scoping in the
 * Hadoop DSL. You can use namespaces to prevent name clashes in the DSL.
 * <p>
 * In the DSL, a namespace can be specified with:
 * <pre>
 *   namespace('scopeName') {
 *     ...
 *   }
 * </pre>
 */
class Namespace extends BaseNamedScopeContainer {
  String name;

  /**
   * Base constructor for a Namespace.
   *
   * @param name The namespace name
   * @param project The Gradle project
   */
  Namespace(String name, Project project) {
    this(name, project, null);
  }

  /**
   * Constructor for a Namespace given a parent scope.
   *
   * @param name The namespace name
   * @param project The Gradle project
   * @param parentScope The parent scope
   */
  Namespace(String name, Project project, NamedScope parentScope) {
    super(project, parentScope, name);
    this.name = name;
  }

  /**
   * Clones the scope container given its new parent scope.
   *
   * @param parentScope The new parent scope
   * @return The cloned scope container
   */
  @Override
  Namespace clone(NamedScope parentScope) {
    return clone(new Namespace(name, project, parentScope));
  }
}
