package com.linkedin.gradle.hadoopdsl.triggerDependency;

/**
 * Abstract Trigger Dependency class extended by concrete dependency classes (i.e. DaliDependency).
 */
abstract class TriggerDependency {
  String name;
  String type;
  Map params;

  TriggerDependency(String name) {
    this.name = name;
    this.type = null;
    this.params = [:];
  }
}
