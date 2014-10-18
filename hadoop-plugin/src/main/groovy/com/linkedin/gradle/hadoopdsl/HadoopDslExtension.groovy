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
class HadoopDslExtension implements NamedScopeContainer {
  String buildDirectory;
  boolean cleanFirst;
  HadoopDslFactory factory;
  NamedScope scope;
  Project project;
  List<Properties> properties;
  List<Workflow> workflows;

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
    this.buildDirectory = null;
    this.factory = project.extensions.hadoopDslFactory;
    this.scope = new NamedScope("hadoop", parentScope);
    this.cleanFirst = true;
    this.project = project;
    this.properties = new ArrayList<Properties>();
    this.workflows = new ArrayList<Job>();

    // Bind the name hadoop in the parent scope so that we can do fully-qualified name lookups of
    // objects bound in the hadoop block.
    parentScope.bind("hadoop", this);
  }

  /**
   * Returns the scope at this level.
   *
   * @return The scope at this level
   */
  @Override
  public NamedScope getScope() {
    return scope;
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
   * Helper method to configure Properties objects in the DSL. Can be called by subclasses to
   * configure custom Properties subclass types.
   *
   * @param props The properties to configure
   * @param configure The configuration closure
   * @return The input properties, which is now configured
   */
  Properties configureProperties(Properties props, Closure configure) {
    Methods.configureProperties(project, props, configure, scope);
    properties.add(props);
    return props;
  }

  /**
   * Helper method to configure a Workflow in the DSL. Can be called by subclasses to configure
   * custom Workflow subclass types.
   *
   * @param workflow The workflow to configure
   * @param configure The configuration closure
   * @return The input workflow, which is now configured
   */
  Workflow configureWorkflow(Workflow workflow, Closure configure) {
    Methods.configureWorkflow(project, workflow, configure, scope);
    workflows.add(workflow);
    return workflow;
  }

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, configures the
   * clone with the given configuration closure and binds the clone in scope.
   *
   * @param name The properties name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured properties object that was bound in scope
   */
  Properties addPropertyFile(String name, Closure configure) {
    return configureProperties(Methods.clonePropertyFile(name, scope), configure);
  }

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, renames the
   * clone to the specified name, configures the clone with the given configuration closure and
   * binds the clone in scope.
   *
   * @param name The properties name to lookup
   * @param rename The new name to give the cloned properties object
   * @param configure The configuration closure
   * @return The cloned, renamed and configured properties object that was bound in scope
   */
  Properties addPropertyFile(String name, String rename, Closure configure) {
    return configureProperties(Methods.clonePropertyFile(name, rename, scope), configure);
  }

  /**
   * DSL addWorkflow method. Looks up the workflow with given name, clones it, configures the clone
   * with the given configuration closure and binds the clone in scope.
   *
   * @param name The workflow name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured workflow that was bound in scope
   */
  Workflow addWorkflow(String name, Closure configure) {
    return configureWorkflow(Methods.cloneWorkflow(name, scope), configure);
  }

  /**
   * DSL addWorkflow method. Looks up the workflow with given name, clones it, renames the clone to
   * the specified name, configures the clone with the given configuration closure and binds the
   * clone in scope.
   *
   * @param name The workflow name to lookup
   * @param rename The new name to give the cloned workflow
   * @param configure The configuration closure
   * @return The cloned, renamed and configured workflow that was bound in scope
   */
  Workflow addWorkflow(String name, String rename, Closure configure) {
    return configureWorkflow(Methods.cloneWorkflow(name, rename, scope), configure);
  }

  /**
   * DSL lookup method. Looks up an object in scope.
   *
   * @param name The name to lookup
   * @return The object that is bound in scope to the given name, or null if no such name is bound in scope
   */
  Object lookup(String name) {
    return Methods.lookup(name, scope);
  }

  /**
   * DSL lookup method. Looks up an object in scope and then applies the given configuration
   * closure.
   *
   * @param name The name to lookup
   * @param configure The configuration closure
   * @return The object that is bound in scope to the given name, or null if no such name is bound in scope
   */
  Object lookup(String name, Closure configure) {
    return Methods.lookup(project, name, scope, configure);
  }

  /**
   * DSL propertyFile method. Creates a Properties object in scope with the given name and
   * configuration.
   *
   * @param name The properties name
   * @param configure The configuration closure
   * @return The new properties object
   */
  Properties propertyFile(String name, Closure configure) {
    return configureProperties(factory.makeProperties(name), configure);
  }

  /**
   * DSL workflow method. Creates a Workflow in scope with the given name and configuration.
   *
   * @param name The workflow name
   * @param configure The configuration closure
   * @return The new workflow
   */
  Workflow workflow(String name, Closure configure) {
    return configureWorkflow(factory.makeWorkflow(name, project, scope), configure);
  }
}