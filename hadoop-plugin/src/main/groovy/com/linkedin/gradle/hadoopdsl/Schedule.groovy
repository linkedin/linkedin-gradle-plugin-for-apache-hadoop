package com.linkedin.gradle.hadoopdsl;

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;

// check to see if valid schedule?
class Schedule {
  String name;
  String type;
  String value;

  Schedule(String name) {
    this.name = name;
    this.type = "cron";
    this.value = null;
  }

  @HadoopDslMethod
  void value(String value) {
    this.value = value;
  }
}
