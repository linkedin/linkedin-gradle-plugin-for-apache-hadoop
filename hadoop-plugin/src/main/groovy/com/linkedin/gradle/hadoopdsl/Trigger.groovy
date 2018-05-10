package com.linkedin.gradle.hadoopdsl

import com.linkedin.gradle.hadoopdsl.triggerDependency.DaliDependency;
import com.linkedin.gradle.hadoopdsl.triggerDependency.TriggerDependency;

import org.gradle.api.Project;

/**
 * Triggers are used by Azkaban to know when to automatically launch flows (aka trigger them).
 *
 * In Azkaban, triggers are created based on their defined schedule (cron schedule) then actually
 * launches the flow when all the dependencies in the trigger are fulfilled.
 */
class Trigger {
  String name;
  Project project;

  // The number of minutes the trigger will exist within Azkaban before being deleted.
  int maxWaitMins;

  // Used to determine when the triggerInstance is created in Azkaban.
  // Should only be 1 defined.
  List<Schedule> schedules;

  // Used to determine what is necessary for the triggerInstance in Azkaban to be satisfied.
  // Once satisfied, the triggerInstance will launch the Azkaban flow.
  List<TriggerDependency> triggerDependencies;

  Trigger(String name, Project project){
    this.name = name;
    this.project = project;
    this.maxWaitMins = 0;
    this.schedules = new ArrayList<Schedule>();
    this.triggerDependencies = new ArrayList<TriggerDependency>();
  }

  protected Trigger clone() {
    return clone(new Trigger(name, project));
  }

  protected Trigger clone(Trigger cloneTrigger) {
    cloneTrigger.maxWaitMins = maxWaitMins;
    cloneTrigger.schedules = schedules;
    cloneTrigger.triggerDependencies = triggerDependencies;
    return cloneTrigger;
  }

  @HadoopDslMethod
  void maxWaitMins(int mins) {
    maxWaitMins = mins;
  }

  @HadoopDslMethod
  void schedule(@DelegatesTo(Schedule) Closure configure) {
    Schedule schedule = new Schedule();
    project.configure(schedule, configure);
    this.schedules.add(schedule);
  }

  @HadoopDslMethod
  void daliDependency(String name, @DelegatesTo(DaliDependency) Closure configure) {
    DaliDependency daliDependency = new DaliDependency(name);
    project.configure(daliDependency, configure);
    triggerDependencies.add(daliDependency);
  }
}
