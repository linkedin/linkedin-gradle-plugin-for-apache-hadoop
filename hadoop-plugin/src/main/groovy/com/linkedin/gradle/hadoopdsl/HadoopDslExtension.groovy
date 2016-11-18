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
package com.linkedin.gradle.hadoopdsl;

import org.gradle.api.Project;

/**
 * HadoopDslExtension is a Gradle Plugin extension for the Hadoop DSL. It contains member variables
 * for the things that are built in the DSL, such as workflows and properties.
 * <p>
 * In the DSL, the HadoopDslExtension is configured with the following syntax:
 * <pre>
 *   hadoop {
 *     buildPath 'conf/jobs'
 *     cleanPath true
 *
 *     // Declare workflows and properties
 *     ...
 *   }
 * </pre>
 */
class HadoopDslExtension extends BaseNamedScopeContainer {
  String buildDirectory;
  boolean cleanFirst;
  String oozieDirectory;

  /**
   * Base constructor for the HadoopDslExtension
   *
   * @param project The Gradle project
   */
  HadoopDslExtension(Project project) {
    this(project, null);
  }

  /**
   * Constructor for the HadoopDslExtension that is aware of its parent scope (global scope).
   *
   * @param project The Gradle project
   * @param parentScope The parent scope
   */
  HadoopDslExtension(Project project, NamedScope parentScope) {
    super(project, parentScope, "hadoop");
    this.buildDirectory = null;
    this.cleanFirst = true;
    this.oozieDirectory = null;

    // Bind the name hadoop in the parent scope so that we can do fully-qualified name lookups of
    // objects bound in the hadoop block.
    parentScope.bind("hadoop", this);
  }

  /**
   * DSL buildPath method sets the directory in which workflow files will be generated when the
   * extension is built. Both absolute and relative paths are accepted.
   *
   * @param buildDir The (relative or absolute) directory in which to build the generated files
   */
  void buildPath(String buildDir) {
    if (buildDir.startsWith("/")) {
      this.buildDirectory = buildDir;
    }
    else {
      this.buildDirectory = new File("${project.projectDir}", buildDir).getPath();
    }
  }

  /**
   * DSL cleanPath method specifies whether or not you want to (recursively) delete all the .job
   * and .properties from the buildPath directory before the DSL is built. This value is true by
   * default.
   *
   * @param cleanFirst Whether or not to clean the buildPath directory before the DSL is built
   */
  void cleanPath(boolean cleanFirst) {
    this.cleanFirst = cleanFirst;
  }

  /**
   * Clones the scope container given its new parent scope.
   *
   * @param parentScope The new parent scope
   * @return The cloned scope container
   */
  @Override
  protected HadoopDslExtension clone(NamedScope parentScope) {
    throw new Exception("The Hadoop DSL Extension is a singleton and cannot be cloned.")
  }

  /**
   * DSL ooziePath method sets the directory in which Oozie workflow files will be generated when
   * the extension is built. Both absolute and relative paths are accepted.
   *
   * @param buildDir The (relative or absolute) directory in which to build the generated files
   */
  void ooziePath(String buildDir) {
    if (buildDir.startsWith("/")) {
      this.oozieDirectory = buildDir;
    }
    else {
      this.oozieDirectory = new File("${project.projectDir}", buildDir).getPath();
    }
  }
}
