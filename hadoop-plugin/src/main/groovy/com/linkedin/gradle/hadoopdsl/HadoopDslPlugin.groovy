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

import com.linkedin.gradle.azkaban.AzkabanDslCompiler;
import com.linkedin.gradle.azkaban.AzkabanDslYamlCompiler;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * HadoopDslPlugin implements features for the Hadoop DSL.
 */
class HadoopDslPlugin extends BaseNamedScopeContainer implements Plugin<Project> {
  HadoopDslExtension extension;
  HadoopDslAutoBuild hadoopDslBuild;

  // Member variables for Hadoop definition sets
  String currentDefinitionSetName;
  Map<String, Map<String, Object>> definitionSetMap;

  // Member variables for Hadoop closures. The list is for anonymous (unnamed) closures while the
  // map is for named closures.
  List<Closure> hadoopClosures;
  Map<String, Closure> namedHadoopClosures;

  static final String GENERATE_YAML_OUTPUT_FLAG_LOCATION = ".hadoop.generate_yaml_output";

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

    // You must have the HadoopDslExtension before you can make the hadoopDslBuild
    this.hadoopDslBuild = makeHadoopDslBuild(extension);
    project.extensions.add("hadoopDslBuild", hadoopDslBuild);

    // Expose the DSL global, applyProfile and applyUserProfile methods, which are only implemented
    // by the HadoopDslPlugin class.
    project.extensions.add("applyProfile", this.&applyProfile);
    project.extensions.add("applyUserProfile", this.&applyUserProfile);
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

    // Add ability for users to toggle .job/.properties or .flow/.project output types
    project.extensions.add("generateYamlOutput", this.&generateYamlOutput);

    // Add the extensions that expose the DSL to users. Specifically, expose all of the DSL
    // functions on the NamedScopeContainer interface.
    project.extensions.add("addJob", this.&addJob);
    project.extensions.add("addNamespace", this.&addNamespace);
    project.extensions.add("addPropertyFile", this.&addPropertyFile);
    project.extensions.add("addPropertySet", this.&addPropertySet);
    project.extensions.add("addWorkflow", this.&addWorkflow);
    project.extensions.add("addTrigger", this.&addTrigger);
    project.extensions.add("lookup", this.&lookup);
    project.extensions.add("lookupRef", this.&lookupRef);
    project.extensions.add("namespace", this.&namespace);
    project.extensions.add("propertyFile", this.&propertyFile);
    project.extensions.add("propertySet", this.&propertySet);
    project.extensions.add("workflow", this.&workflow);
    project.extensions.add("trigger", this.&trigger);
    project.extensions.add("commandJob", this.&commandJob);
    project.extensions.add("gobblinJob", this.&gobblinJob);
    project.extensions.add("hadoopJavaJob", this.&hadoopJavaJob);
    project.extensions.add("hadoopShellJob", this.&hadoopShellJob);
    project.extensions.add("hdfsToEspressoJob", this.&hdfsToEspressoJob);
    project.extensions.add("hdfsToTeradataJob", this.&hdfsToTeradataJob);
    project.extensions.add("hdfsWaitJob", this.&hdfsWaitJob);
    project.extensions.add("sqlJob", this.&sqlJob);
    project.extensions.add("hiveJob", this.&hiveJob);
    project.extensions.add("javaJob", this.&javaJob);
    project.extensions.add("javaProcessJob", this.&javaProcessJob);
    project.extensions.add("job", this.&job);
    project.extensions.add("kafkaPushJob", this.&kafkaPushJob);
    project.extensions.add("noOpJob", this.&noOpJob);
    project.extensions.add("pigJob", this.&pigJob);
    project.extensions.add("pinotBuildAndPushJob", this.&pinotBuildAndPushJob);
    project.extensions.add("sparkJob", this.&sparkJob);
    project.extensions.add("tableauJob", this.&tableauJob);
    project.extensions.add("teradataToHdfsJob", this.&teradataToHdfsJob);
    project.extensions.add("venicePushJob", this.&venicePushJob);
    project.extensions.add("voldemortBuildPushJob", this.&voldemortBuildPushJob);
    project.extensions.add("wormholePushJob", this.&wormholePushJob);
    project.extensions.add("tensorFlowJob", this.&tensorFlowJob);
    project.extensions.add("tonyJob", this.&tonyJob);
  }

  /**
   * DSL applyProfile method. Helper method to apply an external Gradle script, but only if it
   * exists.
   *
   * @param args Args whose required key 'from' specifies the path to the external Gradle script
   * @return True if the external Gradle script exists and was applied; otherwise False
   */
  @HadoopDslMethod
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

    println("Could not find the Hadoop DSL profile file ${filePath}. Ignoring this file.");
    return false;
  }

  /**
   * DSL applyUserProfile method. Helper method to apply an external Gradle script for the current
   * user, but only if it exists. The name of the script to apply and the path in which it lives
   * can be overridden on the command line.
   *
   * @param args Args whose optional key 'profileName' specifies the name of the Gradle script to
   *             apply; whose optional key 'profilePath' specifies the path in which this script
   *             lives; and whose optional key 'skipProfile' specifies whether or not to skip
   *             applying the Gradle script completely.
   * @return True if the external Gradle script exists and was applied; otherwise False
   */
  @HadoopDslMethod
  boolean applyUserProfile(Map args) {
    String profileName = System.properties['user.name'];             // Default to current user name
    String profilePath = "${project.projectDir}/src/main/profiles";  // The default path
    boolean skipProfile = false;

    // Enable the user to override the profile to apply in the DSL or on the command line
    profileName = args && args.containsKey("profileName") ? args.profileName : profileName;
    profileName = project.hasProperty("profileName") ? project.profileName : profileName;

    // Enable the user to override the profile path in the DSL or on the command line
    profilePath = args && args.containsKey("profilePath") ? args.profilePath : profilePath;
    profilePath = project.hasProperty("profilePath") ? project.profilePath : profilePath;

    // Enable the user to override whether or not to skip applying the profile in the DSL and on
    // the command line
    skipProfile = args && args.containsKey("skipProfile") ? args.skipProfile.toBoolean() : skipProfile;
    skipProfile = project.hasProperty("skipProfile") ? project.skipProfile.toBoolean() : skipProfile;

    // If the user specifies to skip applying the profile or the profile name is empty, stop here
    if (skipProfile || !profileName) {
      return false;
    }

    // Form the path of the Gradle file to apply
    String fileName = profileName.endsWith(".gradle") ? profileName : "${profileName}.gradle";
    File profileFile = new File(profilePath, fileName);

    if (profileFile.exists()) {
      project.apply(['from' : profileFile.getAbsolutePath()]);
      return true;
    }

    println("Could not find the Hadoop DSL profile file ${fileName} in the path ${profilePath}. Ignoring this file.");
    return false;
  }

  /**
   * Helper method to coordinate statically checking and building the Hadoop DSL.
   *
   * @param compiler The compiler implementation to use to build the Hadoop DSL
   */
  void buildHadoopDsl(HadoopDslCompiler compiler) {
    // First, run the static checker on the DSL
    HadoopDslChecker checker = factory.makeChecker(project);
    checker.check(this);

    if (checker.failedCheck()) {
      throw new Exception("Hadoop DSL static checker FAILED");
    }
    else {
      project.logger.lifecycle("Hadoop DSL static checker PASSED");
    }

    // If the static checker passes, build the Hadoop DSL
    compiler.compile(this);
  }

  /**
   * Clears the known state of the Hadoop DSL. All known Hadoop DSL elements bound in scope are
   * cleared, as are all Hadoop DSL definition sets and Hadoop closures.
   * <p>
   * This method is intended to be used for writing Hadoop DSL utilities that need to copy, clear
   * and restore the state of the Hadoop DSL. It is not intended for use by end users.
   */
  void clearHadoopDslState() {
    // Clear the Hadoop DSL plugin scope container state
    super.clear();

    // Clear the hadoop { ... } block state
    extension.clear();

    // Clear the definition sets and restore the default definition set
    currentDefinitionSetName = "default";
    definitionSetMap.clear();
    definitionSetMap.put(currentDefinitionSetName, new HashMap<String, Map<String, Object>>());

    // Clear the declared hadoopClosures
    hadoopClosures.clear();
    namedHadoopClosures.clear();
  }

  /**
   * Clones the scope container given its new parent scope.
   *
   * @param parentScope The new parent scope
   * @return The cloned scope container
   */
  @Override
  protected HadoopDslPlugin clone(NamedScope parentScope) {
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
  @HadoopDslMethod
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
  @HadoopDslMethod
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
  @HadoopDslMethod
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
    // Save the previous definition set name and then change to the new definition set
    String oldDefinitionSetName = currentDefinitionSetName;
    setDefinitionSet(definitionSetName);

    if (!namedHadoopClosures.containsKey(closureName)) {
      throw new Exception("There is no named hadoopClosure defined with the name ${closureName}");
    }

    Closure f = namedHadoopClosures.get(closureName);
    Closure g = (Closure)f.clone();
    // The "magic" in this method is that the "this" pointer of the closure is altered to the
    // target object, cause it to resolve Hadoop DSL methods correctly, starting from the target.
    project.configure(target, g);

    // After evaluating the closure, restore the original definition set
    setDefinitionSet(oldDefinitionSetName);
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
  @HadoopDslMethod
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
  @HadoopDslMethod
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
  @HadoopDslMethod
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
  @HadoopDslMethod
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
  protected HadoopDslFactory makeFactory() {
    return new HadoopDslFactory();
  }

  /**
   * Factory method to return a HadoopDslAutoBuild instance. Can be overridden by subclasses that
   * wish to provide their own instance.
   *
   * @param extension The HadoopDslExtension
   * @return The HadoopDslAutoBuild instance
   */
  protected HadoopDslAutoBuild makeHadoopDslBuild(HadoopDslExtension extension) {
    return new HadoopDslAutoBuild(extension, this);
  }

  /**
   * DSL setDefinitionSet method. Sets the current definition set to the definition set with the
   * given name.
   *
   * @param name The name of the definition set to use as the current definition set
   */
  @HadoopDslMethod
  void setDefinitionSet(String name) {
    if (!definitionSetMap.containsKey(name)) {
      throw new Exception("No definitionSet with the name ${name} has been defined");
    }
    currentDefinitionSetName = name;
  }

  /**
   * Based on whether or not the flag generate_yaml_output is set to true in the hadoop scope
   * (i.e. within the hadoop { } closure), select between AzkabanDslYamlCompiler and AzkabanDslCompiler.
   *
   * Default is AzkabanDslCompiler for now.
   *
   * Flag is called 'generate_yaml_output' because underscores are not allowed in the names of
   * other Hadoop objects, so it is highly unlikely for there to be unintentional collisions.
   *
   * @param project The Gradle project
   * @return The User-configured Compiler, default is AzkabanDslCompiler
   */
  HadoopDslCompiler selectCompilerType(Project project) {
    return scope.lookup(GENERATE_YAML_OUTPUT_FLAG_LOCATION) ?
            new AzkabanDslYamlCompiler(project) : new AzkabanDslCompiler(project);
  }
}
