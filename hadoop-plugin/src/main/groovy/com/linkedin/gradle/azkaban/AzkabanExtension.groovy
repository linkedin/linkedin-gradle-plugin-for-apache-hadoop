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
package com.linkedin.gradle.azkaban;

import org.gradle.api.Project;

/**
 * AzkabanExtension is a Gradle Plugin extension for the Azkaban DSL. It contains member variables
 * for the things that are built in the DSL, such as workflows and properties.
 * <p>
 * In the DSL, the AzkabanExtension is configured with the following syntax:
 * <pre>
 *   azkaban {
 *     buildPath 'conf/jobs'
 *     cleanPath true
 *
 *     // Declare workflows and properties
 *     ...
 *   }
 * </pre>
 */
class AzkabanExtension implements NamedScopeContainer {
  AzkabanFactory azkabanFactory;
  NamedScope azkabanScope;
  boolean cleanFirst;
  String jobConfDir;
  Project project;
  List<AzkabanProperties> properties;
  List<AzkabanWorkflow> workflows;

  /**
   * Base constructor for the AzkabanExtension
   *
   * @param project The Gradle project
   */
  AzkabanExtension(Project project) {
    this(project, null);
  }

  /**
   * Constructor for the AzkabanExtension that is aware of global scope.
   *
   * @param project The Gradle project
   * @param globalScope The global scope
   */
  AzkabanExtension(Project project, NamedScope globalScope) {
    this.azkabanFactory = project.extensions.azkabanFactory;
    this.azkabanScope = new NamedScope("azkaban", globalScope);
    this.cleanFirst = true;
    this.jobConfDir = null;
    this.project = project;
    this.properties = new ArrayList<AzkabanProperties>();
    this.workflows = new ArrayList<AzkabanJob>();

    // Bind the name azkaban in the global scope so that we can do fully-qualified name lookups
    // starting from the global scope.
    globalScope.bind("azkaban", this);
  }

  /**
   * Returns the scope at this level.
   *
   * @return The scope at this level
   */
  @Override
  public NamedScope getScope() {
    return azkabanScope;
  }

  /**
   * Builds the Azkaban extension, which builds the workflows and properties that have been
   * specified in the DSL and added to the extension with the azkaban { ... } DSL syntax.
   */
  void build() throws IOException {
    if (jobConfDir == null || jobConfDir.isEmpty()) {
      throw new IOException("You must set the property jobConfDir to use the Azkaban DSL");
    }

    File file = new File(jobConfDir);

    if (file.exists()) {
      if (!file.isDirectory()) {
        throw new IOException("Directory ${jobConfDir} must specify a directory");
      }
    }
    else {
      // Try to make the directory automatically if we can. For git users, this is convenient as
      // git will not push empty directories in the repository (and users will often add the
      // generated job files to their gitignore).
      if (!file.mkdir()) {
        throw new IOException("Directory ${jobConfDir} does not exist and could not be created");
      }
    }

    if (cleanFirst) {
      file.eachFileRecurse(groovy.io.FileType.FILES) { f ->
        String fileName = f.getName().toLowerCase();
        if (fileName.endsWith(".job") || fileName.endsWith(".properties")) {
          f.delete();
        }
      }
    }

    workflows.each() { workflow ->
      workflow.build(jobConfDir, null);
    }

    properties.each() { props ->
      props.build(jobConfDir, null);
    }
  }

  /**
   * DSL buildPath method sets the directory in which Azkaban files will be generated when the
   * extension is built. Both absolute and relative paths are accepted.
   *
   * @param buildDir The (relative or absolute) directory in which to build the Azkaban files
   */
  void buildPath(String buildDir) {
    if (buildDir.startsWith("/")) {
      jobConfDir = buildDir;
    }
    else {
      jobConfDir = new File("${project.projectDir}", buildDir).getPath();
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
   * Helper method to configure AzkabanProperties in the DSL. Can be called by subclasses to
   * configure custom AzkabanProperties subclass types.
   *
   * @param props The properties to configure
   * @param configure The configuration closure
   * @return The input properties, which is now configured
   */
  AzkabanProperties configureProperties(AzkabanProperties props, Closure configure) {
    AzkabanMethods.configureProperties(project, props, configure, azkabanScope);
    properties.add(props);
    return props;
  }

  /**
   * Helper method to configure an AzkabanWorkflow in the DSL. Can be called by subclasses to
   * configure custom AzkabanWorkflow subclass types.
   *
   * @param workflow The workflow to configure
   * @param configure The configuration closure
   * @return The input workflow, which is now configured
   */
  AzkabanWorkflow configureWorkflow(AzkabanWorkflow workflow, Closure configure) {
    AzkabanMethods.configureWorkflow(project, workflow, configure, azkabanScope);
    workflows.add(workflow);
    return workflow;
  }

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, configures the
   * clone with the given configuration closure and adds the clone to azkaban scope.
   *
   * @param name The properties name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured properties object that was added to azkaban scope
   */
  AzkabanProperties addPropertyFile(String name, Closure configure) {
    return configureProperties(AzkabanMethods.clonePropertyFile(name, azkabanScope), configure);
  }

  /**
   * DSL addPropertyFile method. Looks up the properties with given name, clones it, renames the
   * clone to the specified name, configures the clone with the given configuration closure and
   * adds the clone to azkaban scope.
   *
   * @param name The properties name to lookup
   * @param rename The new name to give the cloned properties object
   * @param configure The configuration closure
   * @return The cloned, renamed and configured properties object that was added to azkaban scope
   */
  AzkabanProperties addPropertyFile(String name, String rename, Closure configure) {
    return configureProperties(AzkabanMethods.clonePropertyFile(name, rename, azkabanScope), configure);
  }

  /**
   * DSL addWorkflow method. Looks up the workflow with given name, clones it, configures the clone
   * with the given configuration closure and adds the clone to azkaban scope.
   *
   * @param name The workflow name to lookup
   * @param configure The configuration closure
   * @return The cloned and configured workflow that was added to azkaban scope
   */
  AzkabanWorkflow addWorkflow(String name, Closure configure) {
    return configureWorkflow(AzkabanMethods.cloneWorkflow(name, azkabanScope), configure);
  }

  /**
   * DSL addWorkflow method. Looks up the workflow with given name, clones it, renames the clone to
   * the specified name, configures the clone with the given configuration closure and adds the
   * clone to azkaban scope.
   *
   * @param name The workflow name to lookup
   * @param rename The new name to give the cloned workflow
   * @param configure The configuration closure
   * @return The cloned, renamed and configured workflow that was added to azkaban scope
   */
  AzkabanWorkflow addWorkflow(String name, String rename, Closure configure) {
    return configureWorkflow(AzkabanMethods.cloneWorkflow(name, rename, azkabanScope), configure);
  }

  /**
   * DSL lookup method. Looks up an object in azkaban scope.
   *
   * @param name The name to lookup
   * @return The object that is bound in azkaban scope to the given name, or null if no such name is bound in azkaban scope
   */
  Object lookup(String name) {
    return AzkabanMethods.lookup(name, azkabanScope);
  }

  /**
   * DSL lookup method. Looks up an object in azkaban scope and then applies the given
   * configuration closure.
   *
   * @param name The name to lookup
   * @param configure The configuration closure
   * @return The object that is bound in azkaban scope to the given name, or null if no such name is bound in azkaban scope
   */
  Object lookup(String name, Closure configure) {
    return AzkabanMethods.lookup(project, name, azkabanScope, configure);
  }

  /**
   * DSL propertyFile method. Creates an AzkabanProperties object in azkaban scope with the given
   * name and configuration.
   *
   * @param name The properties name
   * @param configure The configuration closure
   * @return The new properties object
   */
  AzkabanProperties propertyFile(String name, Closure configure) {
    return configureProperties(azkabanFactory.makeAzkabanProperties(name), configure);
  }

  /**
   * DSL workflow method. Creates an AzkabanWorkflow in azkaban scope with the given name and
   * configuration.
   *
   * @param name The workflow name
   * @param configure The configuration closure
   * @return The new workflow
   */
  AzkabanWorkflow workflow(String name, Closure configure) {
    return configureWorkflow(azkabanFactory.makeAzkabanWorkflow(name, project, azkabanScope), configure);
  }
}
