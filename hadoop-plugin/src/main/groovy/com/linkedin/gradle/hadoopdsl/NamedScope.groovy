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

/**
 * The DSL implements explicit scope, allowing us to refer to workflows and jobs by name instead of
 * by object reference.
 */
class NamedScope {
  String levelName;
  NamedScope nextLevel;
  Map<String, Object> thisLevel;

  // Indicates that this scope should be skipped when generating the fully qualified name of an
  // object bound in scope (which can be useful when building files name for compiled files)
  boolean hidden;

  /**
   * Base constructor for NamedScope.
   *
   * @param levelName The name for scope at this level
   */
  NamedScope(String levelName) {
    this(levelName, null);
  }

  /**
   * Constructor for NamedScope that is aware of parent scope.
   *
   * @param levelName The name for scope at this level
   * @param nextLevel The parent scope
   */
  NamedScope(String levelName, NamedScope nextLevel) {
    this.levelName = levelName;
    this.nextLevel = nextLevel;
    this.thisLevel = new HashMap<String, Object>();
  }

  /**
   * Binds a name into scope at this level.
   *
   * @param name The name to bind into scope
   * @param val The value to bind for this name
   */
  void bind(String name, Object val) {
    if (thisLevel.containsKey(name)) {
      throw new Exception("An object with the name ${name} is already declared in the scope ${levelName}");
    }
    if (val == null) {
      throw new Exception("Attempted to bind the name ${name} to a null value at the scope ${levelName}");
    }
    thisLevel.put(name, val);
  }

  /**
   * Clones the scope.
   *
   * @return A clone of the scope
   */
  NamedScope clone() {
    NamedScope namedScope = new NamedScope(levelName, nextLevel);
    namedScope.thisLevel = new HashMap<String, Object>(thisLevel);
    namedScope.hidden = hidden;
    return namedScope;
  }

  /**
   * From this level, recursively check if the given name is bound in scope.
   *
   * @param name The name to check if bound in scope
   * @return Whether the name is bound in scope or not
   */
  boolean contains(String name) {
    return (lookup(name) != null);
  }

  /**
   * From the current scope, walks up scope levels until it reaches global scope.
   *
   * @return A reference to the global scope
   */
  NamedScope findGlobalScope() {
    return (nextLevel == null) ? this : nextLevel.findGlobalScope();
  }

  /**
   * Returns the fully-qualified name for this scope (without an initial "." character).
   *
   * @return The fully-qualified name for the scope (without an initial "." character).
   */
  String getQualifiedName() {
    return getQualifiedName(false);
  }

  /**
   * Returns the fully-qualified name for this scope (without an initial "." character), skipping
   * any hidden scopes (if specified).
   *
   * @param skipHiddenScopes Whethr or not to ignore the names of hidden scopes
   * @return The fully-qualified name for the scope (without an initial "." character).
   */
  String getQualifiedName(boolean skipHiddenScopes) {
    String nextLevelName = (nextLevel == null) ? "" : nextLevel.getQualifiedName(skipHiddenScopes);

    // If this scope is "hidden", do not include it as part of the fully qualified name. This
    // functionality is useful for generating shorter file names during Hadoop DSL auto builds.
    if (hidden && skipHiddenScopes) {
      return nextLevelName;
    }

    return (nextLevelName.isEmpty()) ? levelName : "${nextLevelName}.${levelName}";
  }

  /**
   * From this level, recursively looks up the given name in scope.
   * <p>
   * If the name is a qualified name (a name with a dot), the procedure will first look for the
   * scope matching the qualified portion of the name before looking for the unqualified portion of
   * the name.
   * <p>
   * For example, if the name is foo.bar.bazz, the procedure will start from the current scope and
   * find the scope identified by foo.bar, and then it will lookup the binding for the name bazz.
   * <p>
   * You can lookup objects starting from global scope by using a fully-qualified name, i.e. a name
   * that starts with a dot (instead of just containing a dot or dots).
   * <p>
   * For example, if you lookup ".hadoop.workflow1.job1", starting from global scope, the code will
   * lookup the "hadoop" object and verify that object is a scope container. It will then go to
   * hadoop scope and lookup the "workflow1" object and verify that it is a scope container.
   * Finally, it will go to workflow1 scope, notice that "job1" does not contain any dots, and look
   * only in workflow1 scope for the object bound to the name job1.
   *
   * @param name The name to lookup in scope
   * @return The value bound to the name in scope, or null if the name is not bound in scope
   */
  Object lookup(String name) {
    NamedScopeReference ref = lookupRef(name);
    return (ref == null) ? null : ref.entry;
  }

  /**
   * Performs a lookup on qualified name (i.e. a name with a dot).
   * <p>
   * If the name is a qualified name (a name with a dot), the procedure will first look for the
   * scope matching the qualified portion of the name before looking for the unqualified portion of
   * the name.
   * <p>
   * For example, if the name is foo.bar.bazz, the procedure will start from the current scope and
   * find the scope identified by foo.bar, and then it will lookup the binding for the name bazz.
   * <p>
   * You can lookup objects starting from global scope by using a fully-qualified name, i.e. a name
   * that starts with a dot (instead of just containing a dot or dots).
   * <p>
   * For example, if you lookup ".hadoop.workflow1.job1", starting from global scope, the code will
   * lookup the "hadoop" object and verify that object is a scope container. It will then go to
   * hadoop scope and lookup the "workflow1" object and verify that it is a scope container.
   * Finally, it will go to workflow1 scope, notice that "job1" does not contain any dots, and look
   * only in workflow1 scope for the object bound to the name job1.
   *
   * @param name The qualified name to lookup in scope
   * @return The value bound to the name in scope, or null if the name is not bound in scope
   * @throws Exception If the name is not qualified, or if the qualified portion of the name refers to an object that is not a scope container
   */
  Object lookupQualified(String name) {
    NamedScopeReference ref = lookupRefQualified(name);
    return (ref == null) ? null : ref.entry;
  }

  /**
   * From this level, recursively looks up the given name in scope.
   * <p>
   * If the name is a qualified name (a name with a dot), the procedure will first look for the
   * scope matching the qualified portion of the name before looking for the unqualified portion of
   * the name.
   * <p>
   * For example, if the name is foo.bar.bazz, the procedure will start from the current scope and
   * find the scope identified by foo.bar, and then it will lookup the binding for the name bazz.
   * <p>
   * You can lookup objects starting from global scope by using a fully-qualified name, i.e. a name
   * that starts with a dot (instead of just containing a dot or dots).
   * <p>
   * For example, if you lookup ".hadoop.workflow1.job1", starting from global scope, the code will
   * lookup the "hadoop" object and verify that object is a scope container. It will then go to
   * hadoop scope and lookup the "workflow1" object and verify that it is a scope container.
   * Finally, it will go to workflow1 scope, notice that "job1" does not contain any dots, and look
   * only in workflow1 scope for the object bound to the name job1.
   *
   * @param name The name to lookup in scope
   * @return A NamedScopeReference for the binding, or null if the name is not bound in scope
   */
  NamedScopeReference lookupRef(String name) {
    // If the name is fully-qualified (i.e. starts with a dot), start from global scope.
    if (name.startsWith(".")) {
      return findGlobalScope().lookupRef(name.substring(1));
    }

    // If the name is qualified (i.e. contains a dot), do a qualified lookup.
    if (name.contains(".")) {
      return lookupRefQualified(name);
    }

    // Look for the name in the current level.
    if (thisLevel.containsKey(name)) {
      return new NamedScopeReference(this, thisLevel.get(name), name);
    }

    // If you don't find the name in the current level, try again from the next level up.
    return (nextLevel == null) ? null : nextLevel.lookupRef(name);
  }

  /**
   * Performs a lookup on qualified name (i.e. a name with a dot).
   * <p>
   * If the name is a qualified name (a name with a dot), the procedure will first look for the
   * scope matching the qualified portion of the name before looking for the unqualified portion of
   * the name.
   * <p>
   * For example, if the name is foo.bar.bazz, the procedure will start from the current scope and
   * find the scope identified by foo.bar, and then it will lookup the binding for the name bazz.
   * <p>
   * You can lookup objects starting from global scope by using a fully-qualified name, i.e. a name
   * that starts with a dot (instead of just containing a dot or dots).
   * <p>
   * For example, if you lookup ".hadoop.workflow1.job1", starting from global scope, the code will
   * lookup the "hadoop" object and verify that object is a scope container. It will then go to
   * hadoop scope and lookup the "workflow1" object and verify that it is a scope container.
   * Finally, it will go to workflow1 scope, notice that "job1" does not contain any dots, and look
   * only in workflow1 scope for the object bound to the name job1.
   *
   * @param name The qualified name to lookup in scope
   * @return A NamedScopeReference for the binding, or null if the name is not bound in scope
   * @throws Exception If the name is not qualified, or if the qualified portion of the name refers to an object that is not a scope container
   */
  protected NamedScopeReference lookupRefQualified(String name) {
    if (!name.contains(".")) {
      throw new Exception("lookupRefQualified called with unqualified name ${name}, i.e. a name without a dot");
    }

    // First, look for the first part of the qualified name in the current level.
    String[] parts = name.split("\\.")
    String nextPart = parts[0];
    String restPart = name.replaceFirst("${nextPart}.", "");

    if (thisLevel.containsKey(nextPart)) {
      Object entry = thisLevel.get(nextPart);

      // Make sure the bound object is a scope container before we look there for the next part.
      if (entry instanceof NamedScopeContainer) {
        return ((NamedScopeContainer)entry).scope.lookupRefQualifiedHelper(restPart);
      }
      throw new Exception("Part ${nextPart} in qualified name ${name} referred to an object that is not a NamedScopeContainer: ${entry}");
    }

    // If you don't find the first part of the qualified name, try again from the next level up.
    return (nextLevel == null) ? null : nextLevel.lookupRefQualified(name);
  }

  /**
   * Helper function for looking up qualified names.
   *
   * @param name The qualified name to lookup in scope
   * @return A NamedScopeReference for the binding, or null if the name is not bound in scope
   * @throws Exception If the name is not qualified, or if the qualified portion of the name refers to an object that is not a scope container
   */
  protected NamedScopeReference lookupRefQualifiedHelper(String name) {
    // If the name is no longer qualified, look for it in this level.
    if (!name.contains(".")) {
      return thisLevel.containsKey(name) ? new NamedScopeReference(this, thisLevel.get(name), name) : null;
    }

    // Otherwise if the name is still qualified. Find the next scope level down and continue to look there.
    String[] parts = name.split("\\.")

    // If this part of the name consists of all dots, you won't have any parts in the split.
    if (parts.length == 0) {
      return null;
    }

    String nextPart = parts[0];
    String restPart = name.replaceFirst("${nextPart}.", "");

    if (thisLevel.containsKey(nextPart)) {
      Object entry = thisLevel.get(nextPart);

      // Make sure the bound object is a scope container before we look there for the next part.
      if (entry instanceof NamedScopeContainer) {
        return ((NamedScopeContainer)entry).scope.lookupRefQualifiedHelper(restPart);
      }
      throw new Exception("Part ${nextPart} in qualified name ${name} referred to an object that is not a NamedScopeContainer: ${entry}");
    }

    // If you don't find the next part, the qualified name was not bound in scope.
    return null;
  }

  /**
   * Returns a string representation of the scope.
   *
   * @return A string representation of the scope
   */
  @Override
  String toString() {
    return "(NamedScope: levelName = ${levelName}, nextLevel.levelName = ${nextLevel?.levelName}, thisLevel = ${thisLevel.toString()})";
  }

  /**
   * Removes a bound element from scope at this level.
   *
   * @param name The name to unbind from scope
   * @return The (now unbound) element
   */
  Object unbind(String name) {
    if (!thisLevel.containsKey(name)) {
      throw new Exception("An object with the name ${name} is not declared in the scope ${levelName}");
    }
    return thisLevel.remove(name);
  }
}

/**
 * Helper class for objects bound in scope. This helper class makes it possible for clients of the
 * lookup and contains methods of NamedScope to distinguish between names that are bound in scope
 * to the value null (in which case the entry field of the NamedScopeReference is set to null) and
 * objects that are unbound (in which case the NamedScope methods return null).
 */
class NamedScopeReference {
  NamedScope declaringScope;
  Object entry;
  String name;
  String qualifiedName;

  NamedScopeReference(NamedScope declaringScope, Object entry, String name) {
    String scopeName = declaringScope.getQualifiedName();
    this.declaringScope = declaringScope;
    this.entry = entry;
    this.name = name;
    this.qualifiedName = scopeName.isEmpty() ? name : "${scopeName}.${name}";
  }
}
