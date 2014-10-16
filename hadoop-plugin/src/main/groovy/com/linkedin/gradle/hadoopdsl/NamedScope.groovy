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
 * Interface that enables us to declare which DSL classes can declare a new scope (i.e. that have a
 * NamedScope object as a member variable).
 */
interface NamedScopeContainer {
  /**
   * Returns the scope at this level.
   *
   * @return The scope at this level
   */
  NamedScope getScope();
}

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
   *
   * @param name The name to lookup in scope
   * @return The value bound to the name in scope, or null if the name is not bound in scope
   */
  Object lookup(String name) {
    if (thisLevel.containsKey(name)) {
      return thisLevel.get(name);
    }
    return (nextLevel == null) ? null : nextLevel.lookup(name);
  }

  /**
   * Special lookup method for fully-qualified names. Fully-qualified names in the DSL are any
   * names containing a "." character, e.g. hadoop.workflow1.job1 or workflow1.job2. To perform a
   * lookup on a fully-qualified name, we start from global scope, and then "look down" scopes
   * to get to the exact scope specified by the fully-qualified name. We look to see if the object
   * is bound in scope only at the specified level.
   *
   * @param name The fully-qualified name to lookup
   * @return The value bound to the name in scope, or null if the name is not bound in scope
   */
  Object lookdown(String name) {
    if (!name.contains(".")) {
      return thisLevel.get(name);
    }

    // Handle the global scope (that has an empty level name) as a special case.
    String lookupPrefix = levelName.equals("") ? "" : "${levelName}.";
    String lookupName = name;

    if (!lookupName.startsWith(lookupPrefix)) {
      return null;
    }

    lookupName = lookupName.replaceFirst(lookupPrefix, "");

    if (!lookupName.contains(".")) {
      return thisLevel.get(lookupName);
    }

    String[] parts = lookupName.split(".")
    String nextPart = parts[0];

    thisLevel.each() { String key, Object val ->
      if (key.equals(nextPart)) {
        if (val instanceof NamedScopeContainer) {
          return ((NamedScopeContainer)val).scope.lookdown(lookupName);
        }
        throw new GradleException("Part ${nextPart} in fully qualified name ${name} referred to an object that is not a NamedScopeContainer: ${val}");
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