package com.linkedin.gradle.hadoopdsl;

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;

/**
 * Represents the cron schedule that Azkaban uses to schedule jobs by time.
 *
 * If paired with a dependency in a Trigger, the schedule signifies when the trigger is created.
 * At this point, the trigger will start looking to see if the dependencies are met which then
 * launches the flow.
 */
class Schedule {
  String type;
  String value;

  Schedule() {
    this.type = "cron";
    this.value = null;
  }

  @HadoopDslMethod
  void value(String value) {
    this.value = value;
  }
}
