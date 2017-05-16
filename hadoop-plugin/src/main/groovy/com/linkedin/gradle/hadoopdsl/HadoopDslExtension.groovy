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

import com.linkedin.gradle.test.TestExtension;
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
  List<TestExtension> tests;

  /**
   * Base constructor for the HadoopDslExtension
   *
   * @param project The Gradle project
   */
  HadoopDslExtension(Project project) {
    this(project, null);
    this.tests = new ArrayList<TestExtension>();
  }

  /**
   * Constructor for the HadoopDslExtension
   * @param project The Gradle project
   * @param parentScope The parent scope
   * @param scopeName The name of the scope
   */
  HadoopDslExtension(Project project, NamedScope parentScope, String scopeName) {
    super(project, parentScope, scopeName);
    this.tests = new ArrayList<TestExtension>();
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
    this.tests = new ArrayList<TestExtension>();

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
   * Dsl extension to add tests for the workflow.
   *
   * @param name The name of the test
   * @param configure Test configuration
   * @return The Test object
   */
  @HadoopDslMethod
  TestExtension workflowTestSuite(String name, @DelegatesTo(TestExtension) Closure configure) {
    return configureWorkflowTestSuite(factory.makeTest(name, project, scope), configure);
  }

  /**
   * Helper method to configure a Test in the DSL. Can be called by subclasses to configure custom
   * Test subclass types.
   *
   * @param test The test to configure
   * @param configure The configuration closure
   * @return The input test, which is now configured
   */
  protected TestExtension configureWorkflowTestSuite(TestExtension test, Closure configure) {
    scope.bind(test.name, test);
    project.configure(test, configure);
    this.tests.add(test);
    return test;
  }
}
