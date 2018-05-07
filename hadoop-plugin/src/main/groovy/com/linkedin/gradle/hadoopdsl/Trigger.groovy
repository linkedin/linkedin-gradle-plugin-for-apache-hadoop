package com.linkedin.gradle.hadoopdsl;

import com.linkedin.gradle.hadoopdsl.triggerDependency.DaliDatasetDependency;
import com.linkedin.gradle.hadoopdsl.triggerDependency.TriggerDependency;

import org.gradle.api.Project;

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
  // For example, a Data Dependency will be included here.
  List<TriggerDependency> triggerDependencies;

  Trigger(String name, Project project){
    this.name = name;
    this.project = project;
    this.maxWaitMins = 0; // TODO reallocf verify >0 (required to be set by user)
    this.schedules = new ArrayList<Schedule>();
    this.triggerDependencies = new ArrayList<TriggerDependency>(); // TODO reallocf any verify?
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
  void schedule(String name, @DelegatesTo(Schedule) Closure configure) {
    Schedule schedule = new Schedule(name);
    project.configure(schedule, configure);
    this.schedules.add(schedule);
  }

  @HadoopDslMethod
  void daliDatasetDependency(String name, @DelegatesTo(DaliDatasetDependency) Closure configure) {
    DaliDatasetDependency daliDatasetDependency = new DaliDatasetDependency(name);
    project.configure(daliDatasetDependency, configure);
    triggerDependencies.add(daliDatasetDependency);
  }
}
