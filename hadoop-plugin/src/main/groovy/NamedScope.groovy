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
}