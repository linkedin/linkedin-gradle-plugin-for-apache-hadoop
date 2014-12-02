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

import org.gradle.api.GradleException


/**
 * The DSL implements explicit scope, allowing us to refer to workflows and jobs by name instead of
 * by object reference.
 */
class NamedScope {
  String levelName;
  NamedScope nextLevel;
  Map<String, Object> thisLevel;

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
    return namedScope;
  }

  /**
   * From this level, recursively check if the given name is bound in scope.
   *
   * @param name The name to check if bound in scope
   * @return Whether the name is bound in scope or not
   */
  boolean contains(String name) {
    if (thisLevel.containsKey(name)) {
      return true;
    }
    return (nextLevel == null) ? false : nextLevel.contains(name);
  }

  /**
   * From this level, recursively looks up the object in scope.
   * <p>
   * If you pass a name that starts with a dot, this method does a fully-qualified name lookup
   * instead of using the lookdown method.
   *
   * @param name The name to lookup in scope
   * @return The value bound to the name in scope, or null if the name is not bound in scope
   */
  Object lookup(String name) {
    if (name.startsWith(".")) {
      return lookdown(name);
    }
    if (thisLevel.containsKey(name)) {
      return thisLevel.get(name);
    }
    return (nextLevel == null) ? null : nextLevel.lookup(name);
  }

  /**
   * Special lookup method for fully-qualified names. Fully-qualified names in the DSL are any
   * names starting with a "." character, e.g. .hadoop.workflow1.job1 or .workflow1.job2.
   * <p>
   * To perform a lookup on a fully-qualified name, we walk up to global scope, and then "look
   * down" scopes to get to the exact scope specified by the fully-qualified name. We look to see
   * if the object is bound in scope only at that specified level. See the lookdownHelper method
   * for an example.
   *
   * @param name The fully-qualified name to lookup
   * @return The value bound to the name in scope, or null if the name is not bound in scope
   */
  Object lookdown(String name) {
    if (!name.startsWith(".")) {
      throw new GradleException("The lookdown method can only be used with fully-qualified names. The given name ${name} does not start with a dot.");
    }
    // Keep walking up until you get to global scope.
    if (nextLevel != null) {
      return nextLevel.lookdown(name);
    }
    // Once you get to global scope, use the lookdown helper to resolve the fully-qualified name.
    return lookdownHelper(name);
  }

  /**
   * Helper method to perform a lookup on fully-qualified name starting from global scope.
   * <p>
   * To perform a lookup on a fully-qualified name, we start from global scope, and then "look
   * down" scopes to get to the exact scope specified by the fully-qualified name. We look to see
   * if the object is bound in scope only at that specified level.
   * <p>
   * For example, if you lookup ".hadoop.workflow1.job1", starting from global scope, the code will
   * lookup the "hadoop" object and verify that object is a scope container. It will then go to
   * hadoop scope and lookup the "workflow1" object and verify that it is a scope container.
   * Finally, it will go to workflow1 scope, notice that "job1" does not contain any dots, and look
   * only in workflow1 scope for the object bound to the name job1.
   *
   * @param name The fully-qualified name to lookup
   * @return The value bound to the name in scope, or null if the name is not bound in scope
   */
  Object lookdownHelper(String name) {
    // Start by checking if the scope name prefixes the lookup name.
    if (!name.startsWith("${levelName}.")) {
      return null;
    }

    // Then remove the scope prefix from the lookup name.
    String lookupName = name.replaceFirst("${levelName}.", "");

    // If the name is in this scope, look for the value in this level.
    if (!lookupName.contains(".")) {
      return thisLevel.get(lookupName);
    }

    // If the lookup name still has a dot, look for the next level name and continue to look down.
    String[] parts = lookupName.split("\\.")
    String nextPart = parts[0];

    for (Map.Entry<String, Object> entry : thisLevel.entrySet()) {
      if (entry.getKey().equals(nextPart)) {
        if (entry.getValue() instanceof NamedScopeContainer) {
          return ((NamedScopeContainer)entry.getValue()).scope.lookdownHelper(lookupName);
        }
        throw new GradleException("Part ${nextPart} in fully qualified name ${name} referred to an object that is not a NamedScopeContainer: ${entry.getValue()}");
      }
    }

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
}