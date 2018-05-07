package com.linkedin.gradle.hadoopdsl.triggerDependency;

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;

// validation?
//daliDataset('search-impression') {
//  view 'search_mp_versioned.search_impression_event_0_0_47'
//  delay 1
//  window 1
//  unit 'daily'
//  filter 'isguest=0'
//}
class DaliDatasetDependency extends TriggerDependency {

  DaliDatasetDependency(String name) {
    super(name);
    this.type = "dali-dataset";
    this.params = [:];
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

  // TODO validate daily or weekly
  @HadoopDslMethod
  void unit(String unit) {
    this.params["unit"] = unit;
  }

  // TODO validate true or false
  @HadoopDslMethod
  void ignoreLocation(String ignoreLocation) {
    this.params["ignoreLocation"] = ignoreLocation;
  }
}
