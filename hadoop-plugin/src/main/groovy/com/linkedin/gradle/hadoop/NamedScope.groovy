package com.linkedin.gradle.hadoop;

import org.gradle.api.GradleException

/**
 * Interface that enables us to declare what DSL classes can declare a new
 * scope (i.e. that thave a NamedScope object as a member variable).
 */
interface NamedScopeContainer {
  NamedScope getScope();
}

/**
 * The DSL implements explicit scope, allowing us to refer to workflows and
 * jobs by name instead of by object reference.
 */
class NamedScope {
  String levelName;
  NamedScope nextLevel;
  Map<String, Object> thisLevel;

  NamedScope(String levelName) {
    this(levelName, null);
  }

  NamedScope(String levelName, NamedScope nextLevel) {
    this.levelName = levelName;
    this.nextLevel = nextLevel;
    this.thisLevel = new HashMap<String, Object>();
  }

  void bind(String name, Object val) {
    if (thisLevel.containsKey(name)) {
      throw new Exception("An object with the name ${name} is already declared in the scope ${levelName}");
    }
    thisLevel.put(name, val);
  }

  NamedScope clone() {
    NamedScope namedScope = new NamedScope(levelName, nextLevel);
    namedScope.thisLevel = new HashMap<String, Object>(thisLevel);
    return namedScope;
  }

  boolean contains(String name) {
    if (thisLevel.containsKey(name)) {
      return true;
    }
    return (nextLevel == null) ? false : nextLevel.contains(name);
  }

  Object lookup(String name) {
    if (thisLevel.containsKey(name)) {
      return thisLevel.get(name);
    }
    return (nextLevel == null) ? null : nextLevel.lookup(name);
  }

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

  String toString() {
    return "(NamedScope: levelName = ${levelName}, nextLevel.levelName = ${nextLevel?.levelName}, thisLevel = ${thisLevel.toString()})";
  }
}