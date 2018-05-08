package com.linkedin.gradle.hadoopdsl.triggerDependency;

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
