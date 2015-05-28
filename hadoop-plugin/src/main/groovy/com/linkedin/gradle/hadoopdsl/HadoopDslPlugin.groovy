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

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * HadoopDslPlugin implements features for the Hadoop DSL.
 */
class HadoopDslPlugin extends BaseNamedScopeContainer implements Plugin<Project> {
  HadoopDslExtension extension;

  // Member variables for Hadoop definition sets
  String currentDefinitionSetName;
  Map<String, Map<String, Object>> definitionSetMap;

  /**
   * Constructor for the Hadoop DSL Plugin.
   */
  HadoopDslPlugin() {
    super(null, "");
    currentDefinitionSetName = "default";
    definitionSetMap = new HashMap<String, Map<String, Object>>();
    definitionSetMap.put(currentDefinitionSetName, new HashMap<String, Map<String, Object>>());
  }

  /**
   * Applies the Hadoop DSL Plugin, which sets up the extensions and methods necessary to use the
   * Hadoop DSL.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    // Enable users to skip the plugin
    if (project.hasProperty("disableHadoopDslPlugin")) {
      println("HadoopDslPlugin disabled");
      return;
    }

    this.factory = makeFactory();
    this.project = project;

    project.extensions.add("hadoopDslFactory", factory);
    project.extensions.add("hadoopDslPlugin", this);

    // You must have the hadoopDslFactory extension set before you can make the HadoopDslExtension
    this.extension = factory.makeExtension(project, scope);
    project.extensions.add("hadoop", extension);

    // Expose the DSL global method, which is only implemented by the HadoopDslPlugin class.
    project.extensions.add("global", this.&global);

    // Expose the DSL methods for using Hadoop definition sets.
    project.extensions.add("definitionSet", this.&definitionSet);
    project.extensions.add("definitionSetName", this.&definitionSetName);
    project.extensions.add("lookupDef", this.&lookupDef);
    project.extensions.add("setDefinitionSet", this.&setDefinitionSet);

    // Add the extensions that expose the DSL to users. Specifically, expose all of the DSL
    // functions on the NamedScopeContainer interface.
    project.extensions.add("addJob", this.&addJob);
    project.extensions.add("addNamespace", this.&addNamespace);
    project.extensions.add("addPropertyFile", this.&addPropertyFile);
    project.extensions.add("addPropertySet", this.&addPropertySet);
    project.extensions.add("addWorkflow", this.&addWorkflow);
    project.extensions.add("lookup", this.&lookup);
    project.extensions.add("namespace", this.&namespace);
    project.extensions.add("propertyFile", this.&propertyFile);
    project.extensions.add("propertySet", this.&propertySet);
    project.extensions.add("workflow", this.&workflow);
    project.extensions.add("commandJob", this.&commandJob);
    project.extensions.add("hadoopJavaJob", this.&hadoopJavaJob);
    project.extensions.add("hiveJob", this.&hiveJob);
    project.extensions.add("javaJob", this.&javaJob);
    project.extensions.add("javaProcessJob", this.&javaProcessJob);
    project.extensions.add("job", this.&job);
    project.extensions.add("kafkaPushJob", this.&kafkaPushJob);
    project.extensions.add("noOpJob", this.&noOpJob);
    project.extensions.add("pigJob", this.&pigJob);
    project.extensions.add("voldemortBuildPushJob", this.&voldemortBuildPushJob);
  }

  /**
   * Clones the scope container given its new parent scope.
   *
   * @param parentScope The new parent scope
   * @return The cloned scope container
   */
  @Override
  HadoopDslPlugin clone(NamedScope parentScope) {
    throw new Exception("The Hadoop DSL Plugin is a singleton and cannot be cloned.")
  }

  /**
   * DSL definitionSet method. Adds the given definitions to either the specified definition set or
   * to the default definition set (if no name is specified).
   * <p>
   * This convenience method is intended to help users deal with the problem that the scope of a
   * regular Groovy def is limited to the Gradle file in which it is declared.
   *
   * @param args A map whose optional key "name" specifies the definition set and whose key "defs" specifies the definitions to add
   */
  void definitionSet(Map args) {
    Map<String, Object> defs = args.defs;

    if (!args.containsKey("name")) {
      Map<String, Object> defaultSet = definitionSetMap.get("default");
      defaultSet.putAll(defs);
    }
    else {
      String name = args.name;
      definitionSet(name, defs);
    }
  }

  /**
   * DSL definitionSet method. Adds the given definitions to the specified definition set.
   * <p>
   * This convenience method is intended to help users deal with the problem that the scope of a
   * regular Groovy def is limited to the Gradle file in which it is declared.
   *
   * @param name The name of the definition set to create or update
   * @param defs The definitions to add to the definition set
   */
  void definitionSet(String name, Map<String, Object> defs) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Cannot declare a definitionSet with a name that is null or empty.");
    }

    Map<String, Object> definitionSet = null;

    if (definitionSetMap.containsKey(name)) {
      definitionSet = definitionSetMap.get(name);
    }
    else {
      definitionSet = new HashMap<String, Object>();
      definitionSetMap.put(name, definitionSet);
    }

    definitionSet.putAll(defs);
  }

  /**
   * DSL definitionSetName method. Gets the name of the current definition set. The name of the
   * default definition set is "default".
   *
   * @return The name of the current definition set, or the empty string if it is the default set
   */
  String definitionSetName() {
    return currentDefinitionSetName;
  }

  /**
   * @deprecated This method has been deprecated in favor of methods for Hadoop definition sets.
a  *
   * DSL global method. Binds the object in global scope.
   *
   * @param object The object to bind in global scope
   * @return The object, now bound in global scope
   */
  @Deprecated
  Object global(Object object) {
    if (scope.contains(object.name)) {
      throw new Exception("An object with name ${object.name} requested to be global is already bound in scope ${scope.levelName}");
    }
    scope.bind(object.name, object);
    return object;
  }

  /**
   * DSL lookupDef method. Looks up the value for the given name in the current definition set. If
   * the given name was not declared in the current definition set (or the current set is the
   * default set) the the function will look for the name in the default set.
   *
   * @param name The definition name
   * @return The definition value
   */
  Object lookupDef(String name) {
    if (!"default".equals(currentDefinitionSetName)) {
      Map<String, Object> currentDefinitionSet = definitionSetMap.get(currentDefinitionSetName);

      if (currentDefinitionSet.containsKey(name)) {
        return currentDefinitionSet.get(name);
      }
    }

    Map<String, Object> defaultDefinitionSet = definitionSetMap.get("default");

    if (defaultDefinitionSet.containsKey(name)) {
      return defaultDefinitionSet.get(name);
    }

    throw new Exception("No definition with the name ${name} has been defined in the current or default definition set.");
  }

  /**
   * Factory method to return the Hadoop DSL Factory. Can be overridden by subclasses that wish to
   * provide their own factory.
   *
   * @return The factory to use
   */
  HadoopDslFactory makeFactory() {
    return new HadoopDslFactory();
  }

  /**
   * DSL setDefinitionSet method. Sets the current definition set to the definition set with the
   * given name.
   *
   * @param name The name of the definition set to use as the current definition set
   */
  void setDefinitionSet(String name) {
    if (!definitionSetMap.containsKey(name)) {
      throw new Exception("No definitionSet with the name ${name} has been defined");
    }
    currentDefinitionSetName = name;
  }
}
