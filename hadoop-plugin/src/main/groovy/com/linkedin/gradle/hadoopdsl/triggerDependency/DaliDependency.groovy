package com.linkedin.gradle.hadoopdsl.triggerDependency;

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;

/**
 * This dependency is satisfied when the dali dataset view becomes fully available.
 */
class DaliDependency extends TriggerDependency {

  DaliDependency(String name) {
    super(name);
    this.type = "dali";
  }

  @HadoopDslMethod
  void view(String view) {
    this.params["view"] = view;
  }

  @HadoopDslMethod
  void delay(int delay) {
    this.params["delay"] = delay;
  }

  @HadoopDslMethod
  void window(int window) {
    this.params["window"] = window;
  }

  @HadoopDslMethod
  void unit(String unit) {
    this.params["unit"] = unit;
  }

  @HadoopDslMethod
  void ignoreLocation(boolean ignoreLocation) {
    this.params["ignoreLocation"] = ignoreLocation;
  }
}
