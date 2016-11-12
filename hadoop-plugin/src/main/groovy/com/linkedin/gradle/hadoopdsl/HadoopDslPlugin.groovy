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

  // Member variables for Hadoop closures. The list is for anonymous (unnamed) closures while the
  // map is for named closures.
  List<Closure> hadoopClosures;
  Map<String, Closure> namedHadoopClosures;

  /**
   * Constructor for the Hadoop DSL Plugin.
   */
  HadoopDslPlugin() {
    super(null, "");
    currentDefinitionSetName = "default";
    definitionSetMap = new HashMap<String, Map<String, Object>>();
    definitionSetMap.put(currentDefinitionSetName, new HashMap<String, Map<String, Object>>());
    hadoopClosures = new ArrayList<Closure>();
    namedHadoopClosures = new HashMap<String, Closure>();
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

    // Expose the DSL global and applyProfile methods, which are only implemented by the HadoopDslPlugin class.
    project.extensions.add("applyProfile", this.&applyProfile);
    project.extensions.add("global", this.&global);

    // Expose the DSL methods for using Hadoop definition sets.
    project.extensions.add("definitionSet", this.&definitionSet);
    project.extensions.add("definitionSetName", this.&definitionSetName);
    project.extensions.add("lookupDef", this.&lookupDef);
    project.extensions.add("setDefinitionSet", this.&setDefinitionSet);

    // Expose the DSL methods for using Hadoop closures.
    project.extensions.add("evalHadoopClosure", this.&evalHadoopClosure);
    project.extensions.add("evalHadoopClosures", this.&evalHadoopClosures);
    project.extensions.add("hadoopClosure", this.&hadoopClosure);

    // Add the extensions that expose the DSL to users. Specifically, expose all of the DSL
    // functions on the NamedScopeContainer interface.
    project.extensions.add("addJob", this.&addJob);
    project.extensions.add("addNamespace", this.&addNamespace);
    project.extensions.add("addPropertyFile", this.&addPropertyFile);
    project.extensions.add("addPropertySet", this.&addPropertySet);
    project.extensions.add("addWorkflow", this.&addWorkflow);
    project.extensions.add("lookup", this.&lookup);
    project.extensions.add("lookupRef", this.&lookupRef);
    project.extensions.add("namespace", this.&namespace);
    project.extensions.add("propertyFile", this.&propertyFile);
    project.extensions.add("propertySet", this.&propertySet);
    project.extensions.add("workflow", this.&workflow);
    project.extensions.add("commandJob", this.&commandJob);
    project.extensions.add("gobblinJob", this.&gobblinJob);
    project.extensions.add("hadoopJavaJob", this.&hadoopJavaJob);
    project.extensions.add("hadoopShellJob", this.&hadoopShellJob);
    project.extensions.add("hdfsToEspressoJob", this.&hdfsToEspressoJob);
    project.extensions.add("hdfsToTeradataJob", this.&hdfsToTeradataJob);
    project.extensions.add("sqlJob", this.&sqlJob);
    project.extensions.add("hiveJob", this.&hiveJob);
    project.extensions.add("javaJob", this.&javaJob);
    project.extensions.add("javaProcessJob", this.&javaProcessJob);
    project.extensions.add("job", this.&job);
    project.extensions.add("kafkaPushJob", this.&kafkaPushJob);
    project.extensions.add("noOpJob", this.&noOpJob);
    project.extensions.add("pigJob", this.&pigJob);
    project.extensions.add("sparkJob", this.&sparkJob);
    project.extensions.add("teradataToHdfsJob", this.&teradataToHdfsJob);
    project.extensions.add("voldemortBuildPushJob", this.&voldemortBuildPushJob);
  }

  /**
   * DSL applyProfile method. Helper method to apply an external Gradle script, but only if it
   * exists.
   *
   * @param args Args whose required key 'from' specifies the path to the external Gradle script
   * @return True if the external Gradle script exists and was applied; otherwise False
   */
  boolean applyProfile(Map args) {
    if (!args.containsKey("from")) {
      throw new Exception("Syntax for using applyProfile is 'applyProfile from: \"filePath\"'");
    }

    String filePath = args.from;
    File file = new File(filePath);

    if (file.exists()) {
      project.apply(['from' : file.getAbsolutePath()]);
      return true;
    }
    return false;
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
   * @return The updated definitions
   */
  Map<String, Object> definitionSet(Map args) {
    Map<String, Object> defs = args.defs;

    if (!args.containsKey("name")) {
      Map<String, Object> defaultSet = definitionSetMap.get("default");
      defaultSet.putAll(defs);
      return defaultSet;
    }

    String name = args.name;
    return definitionSet(name, defs);
  }

  /**
   * DSL definitionSet method. Adds the given definitions to the specified definition set.
   * <p>
   * This convenience method is intended to help users deal with the problem that the scope of a
   * regular Groovy def is limited to the Gradle file in which it is declared.
   *
   * @param name The name of the definition set to create or update
   * @param defs The definitions to add to the definition set
   * @return The updated definitions
   */
  Map<String, Object> definitionSet(String name, Map<String, Object> defs) {
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
    return definitionSet;
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
   * Evaluates the specified hadoopClosure against the specified definition set and target.
   * <p>
   * This method actually clones each closure before evaluating it, so that the originally declared
   * closure is left unevaluated. This is to minimize side-effects that might potentially arise from
   * lazily-evaluated values within the closure. Otherwise, the lazy values would be evaluated on
   * the first evaluation of the closure, but not on subsequent evaluations.
   *
   * @param closureName The named hadoopClosure to evaluate
   * @param definitionSetName The definition set name to use as the current definition set before evaluating the closure
   * @param target The object to set as the closure delegate before evaluating the closure
   */
  void evalHadoopClosure(String closureName, String definitionSetName, Object target) {
    setDefinitionSet(definitionSetName);

    if (!namedHadoopClosures.containsKey(closureName)) {
      throw new Exception("There is no named hadoopClosure defined with the name ${closureName}");
    }

    Closure f = namedHadoopClosures.get(closureName);
    Closure g = f.clone();
    // The "magic" in this method is that the "this" pointer of the closure is altered to the
    // target object, cause it to resolve Hadoop DSL methods correctly, starting from the target.
    project.configure(target, g);
  }

  /**
   * Evaluates all the anonymous hadoopClosure closures against the specified definition set and
   * target.
   * <p>
   * This method actually clones each closure before evaluating it, so that the originally declared
   * closure is left unevaluated. This is to minimize side-effects that might potentially arise from
   * lazily-evaluated values within the closure. Otherwise, the lazy values would be evaluated on
   * the first evaluation of the closure, but not on subsequent evaluations.
   *
   * @param definitionSetName The definition set name to use as the current definition set before evaluating the closures
   * @param target The object to set as the closure delegate before evaluating the closure
   */
  void evalHadoopClosures(String definitionSetName, Object target) {
    setDefinitionSet(definitionSetName);

    hadoopClosures.each { Closure f ->
      Closure g = (Closure)f.clone();
      // The "magic" in this method is that the "this" pointer of the closure is altered to the
      // HadoopDslExtension instance, so that evaluating the closure will cause it to resolve Hadoop
      // DSL methods correctly.
      g.delegate = target;
      g();
    }
  }

  /**
   * @deprecated This method has been deprecated in favor of methods for Hadoop definition sets.
   *
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
   * DSL hadoopClosure method. Declares the given (unevaluated) closure as a hadoopClosure.
   * <p>
   * The Hadoop Plugin exposes build tasks will evaluate these closures against a specified
   * definition set. The usefulness of this feature stems from the fact that the closures may be
   * re-evaluated against several different definition sets (or the default definition set).
   *
   * @param args A map whose required key "closure" specifies the closure to save and whose
   *             optional key "name" specifies the closure name. If the name is not specified, the
   *             the closure is treated as an anonymous closure.
   * @return The declared closure
   */
  Closure hadoopClosure(Map args) {
    Closure closure = args.closure;
    String name = args.containsKey("name") ? args.name : "";
    return hadoopClosure(closure, name);
  }

  /**
   * DSL hadoopClosure method. Declares the given (unevaluated) closure as a hadoopClosure.
   * <p>
   * The Hadoop Plugin exposes build tasks will evaluate these closures against a specified
   * definition set. The usefulness of this feature stems from the fact that the closures may be
   * re-evaluated against several different definition sets (or the default definition set).
   *
   * @param closure The closure to save
   * @param name The name of the closure, or the empty string if the closure should be treated as
   *             an anonymous closure
   * @return The declared closure
   */
  Closure hadoopClosure(Closure closure, String name) {
    if ("".equals(name)) {
      hadoopClosures.add(closure);
    }
    else {
      if (namedHadoopClosures.containsKey(name)) {
        throw new Exception("There is already a hadoopClosure defined with the name ${name}")
      }
      namedHadoopClosures.put(name, closure);
    }
    return closure;
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
