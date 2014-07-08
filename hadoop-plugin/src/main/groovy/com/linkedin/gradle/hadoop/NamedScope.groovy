package com.linkedin.gradle.hadoop;

import org.gradle.api.GradleException

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
        if (val instanceof AzkabanExtension) {
          return ((AzkabanExtension)val).azkabanScope.lookdown(lookupName);
        }
        if (val instanceof AzkabanWorkflow) {
          return ((AzkabanWorkflow)val).workflowScope.lookdown(lookupName);
        }
        throw new GradleException("Part ${nextPart} in fully qualified name ${name} referred to a non-container object: ${val}");
      }
    }

    return null;
  }
}