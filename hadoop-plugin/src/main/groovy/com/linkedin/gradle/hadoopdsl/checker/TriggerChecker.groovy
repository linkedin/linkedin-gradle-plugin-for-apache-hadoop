package com.linkedin.gradle.hadoopdsl.checker;

import com.linkedin.gradle.hadoopdsl.BaseStaticChecker;
import com.linkedin.gradle.hadoopdsl.Schedule;
import com.linkedin.gradle.hadoopdsl.Trigger;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.triggerDependency.DaliDependency;
import com.linkedin.gradle.hadoopdsl.triggerDependency.TriggerDependency;
import org.gradle.api.Project;
import org.quartz.CronExpression;

/**
 * The TriggerChecker rule checks the following:
 * <ul>
 *   <li>Trigger specific
 *   <li>ERROR if more than 1 trigger is defined in a workflow.</li>
 *   <li>ERROR if there are dependencies and a trigger maxWaitMins doesn't exist or is less than 1.</li>
 *   <li>WARN if a trigger maxWaitMins is greater than 10 days (14400) - will be set to 10 days
 *       by Azkaban.</li>
 *   <li>WARN if a trigger maxWaitMins is set and there are no dependencies</li>
 *   <li>ERROR if a trigger schedule doesn't exist.</li>
 *   <li>ERROR if more than one trigger schedule is defined for a trigger.</li>
 *   <li>ERROR if a trigger schedule doesn't have a value.</li>
 *   <li>ERROR if a trigger schedule value is not a valid cron expression.</li>
 *   <li>ERROR if a trigger schedule value has an interval smaller than 1 minute.</li>
 *   <li>ERROR if a trigger dependency name is not unique.</li>
 *   <li>ERROR if a trigger dependency config (type + params) is not unique.</li>
 *   <li>
 *   <li>DaliDependency specific</li>
 *   <li>ERROR if a trigger dali dependency doesn't have a view.</li>
 *   <li>ERROR if a trigger dali dependency doesn't have a window.</li>
 *   <li>ERROR if a trigger dali dependency doesn't have a delay.</li>
 *   <li>ERROR if a trigger dali dependency doesn't have a unit.</li>
 *   <li>ERROR if a trigger dali dependency window less than one.</li>
 *   <li>ERROR if a trigger dali dependency delay less than zero.</li>
 *   <li>ERROR if a trigger dali dependency unit is not 'daily' or 'hourly'.</li>
 * </ul>
 */
class TriggerChecker extends BaseStaticChecker {
  private final int MIN_FLOW_TRIGGER_WAIT_TIME = 1;
  private final int MAX_FLOW_TRIGGER_WAIT_TIME = 14400;

  Workflow workflow;
  Trigger trigger;

  /**
   * Constructor for the TriggerChecker.
   *
   * @param project The Gradle project
   */
  TriggerChecker(Project project) {
    super(project);
  }

  @Override
  void visitWorkflow(Workflow workflow) {
    boolean workflowError = false;
    this.workflow = workflow;

    // ERROR if more than 1 trigger
    if (workflow.triggers.size() > 1) {
      project.logger.lifecycle("TriggerChecker ERROR: Workflow ${workflow.name} contains more than 1 trigger.");
      workflowError = true;
    }
    // Test trigger (or multiple if defined) to see if properly defined
    workflow.triggers.each { Trigger trigger ->
      this.trigger = trigger;
      workflowError |= checkTrigger(trigger);
    }

    // Indicate to the static checker whether or not we passed all the static checks
    foundError |= workflowError;
  }

  boolean checkTrigger(Trigger trigger) {
    boolean triggerError = false;
    this.trigger = trigger;
    Set<String> triggerDependencyNames = [];
    Set<String> triggerDependencyConfigs = [];

    triggerError |= checkWaitMins();
    triggerError |= checkSchedule();
    trigger.triggerDependencies.each { TriggerDependency triggerDependency ->
      // ERROR if dependency name is not unique
      if (triggerDependency.name in triggerDependencyNames) {
        project.logger.lifecycle("TriggerChecker ERROR: Trigger ${trigger.name} in Workflow ${workflow.name} contains dependencies with the same name.");
        triggerError = true;
      }
      triggerDependencyNames.add(triggerDependency.name);
      // ERROR if dependency config (type + params) is not unique
      String triggerDependencyConfig = triggerDependency.type + triggerDependency.params.toString();
      if (triggerDependencyConfig in triggerDependencyConfigs) {
        project.logger.lifecycle("TriggerChecker ERROR: Trigger ${trigger.name} in Workflow ${workflow.name} contains duplicate dependencies (same type and parameters).");
        triggerError = true;
      }
      triggerDependencyConfigs.add(triggerDependencyConfig);
      // Check for trigger specific errors
      if (triggerDependency.type == 'dali') {
        triggerError |= checkDaliDependency((DaliDependency) triggerDependency);
      }
    }


    return triggerError;
  }

  boolean checkWaitMins() {
    boolean checkWaitMinsError = false;

    // ERROR if there are dependencies and maxWaitMins doesn't exist or <1
    if (trigger.triggerDependencies.size() != 0 && trigger.maxWaitMins < MIN_FLOW_TRIGGER_WAIT_TIME) {
      project.logger.lifecycle("TriggerChecker ERROR: Trigger ${trigger.name} in Workflow ${workflow.name} must define 'maxWaitMins' and it must be greater than 0.");
      checkWaitMinsError = true;
    }
    // WARN if maxWaitMins >10 days (14400 mins)
    if (trigger.maxWaitMins > MAX_FLOW_TRIGGER_WAIT_TIME) {
      project.logger.lifecycle("TriggerChecker WARN: Trigger ${trigger.name} in Workflow ${workflow.name} defines 'maxWaitMins' to be greater than 10 days. Azkaban will automatically reduce to 10 days.");
    }
    // WARN if maxWaitMins is set when there are no dependencies
    if (trigger.triggerDependencies.size() == 0 && trigger.maxWaitMins != 0) {
      project.logger.lifecycle("TriggerChecker WARN: Trigger ${trigger.name} in Workflow ${workflow.name} defines 'maxWaitMins' when there are no dependencies. Azkaban will automatically launch the flow when the schedule is met, so this isn't necessary.");
    }

    return checkWaitMinsError;
  }

  boolean checkSchedule() {
    boolean checkScheduleError = false;

    // ERROR if no schedule defined
    if (trigger.schedules.isEmpty()) {
      project.logger.lifecycle("TriggerChecker ERROR: Trigger ${trigger.name} in Workflow ${workflow.name} does not contain a schedule.");
      checkScheduleError = true;
    }
    // ERROR if more than one schedule defined
    if (trigger.schedules.size() > 1) {
      project.logger.lifecycle("TriggerChecker ERROR: Trigger ${trigger.name} in Workflow ${workflow.name} contains more than one schedule.");
      checkScheduleError = true;
    }
    // Loop through each schedules (should only be 1) to see if properly defined
    trigger.schedules.each { Schedule schedule ->
      // ERROR if no schedule value defined
      if (schedule.value == null) {
        project.logger.lifecycle("TriggerChecker ERROR: Schedule in Trigger ${trigger.name} in Workflow ${workflow.name} must define 'value'.");
        checkScheduleError = true;
      }
      // ERROR if schedule value is not a valid cron expression
      else if (!CronExpression.isValidExpression(schedule.value)) {
        project.logger.lifecycle("TriggerChecker ERROR: Schedule in Trigger ${trigger.name} in Workflow ${workflow.name} must define 'value' as a valid cron expression.");
        checkScheduleError = true;
      }
      // ERROR if schedule value specifies a cron expression that runs more frequently than once per minute
      else if (schedule.value.split("\\s+")[0] != "0") {
        project.logger.lifecycle("TriggerChecker ERROR: Schedule in Trigger ${trigger.name} in Workflow ${workflow.name} must define interval of flow trigger to be larger than 1 minute.");
        checkScheduleError = true;
      }
    }

    return checkScheduleError;
  }

  boolean checkDaliDependency(DaliDependency daliDependency) {
    boolean checkDaliDependencyError = false;

    // ERROR if dali dependency doesn't have a view
    if (!daliDependency.params.containsKey("view")) {
      project.logger.lifecycle("TriggerChecker ERROR: DaliDependency ${daliDependency.name} in Trigger ${trigger.name} in Workflow ${workflow.name} must define a view.");
      checkDaliDependencyError = true;
    }
    // ERROR if dali dependency doesn't have a window
    if (!daliDependency.params.containsKey("window")) {
      project.logger.lifecycle("TriggerChecker ERROR: DaliDependency ${daliDependency.name} in Trigger ${trigger.name} in Workflow ${workflow.name} must define a window.");
      checkDaliDependencyError = true;
    }
    // ERROR if dali dependency doesn't have a delay
    if (!daliDependency.params.containsKey("delay")) {
      project.logger.lifecycle("TriggerChecker ERROR: DaliDependency ${daliDependency.name} in Trigger ${trigger.name} in Workflow ${workflow.name} must define a delay.");
      checkDaliDependencyError = true;
    }
    // ERROR if dali dependency doesn't have a unit
    if (!daliDependency.params.containsKey("unit")) {
      project.logger.lifecycle("TriggerChecker ERROR: DaliDependency ${daliDependency.name} in Trigger ${trigger.name} in Workflow ${workflow.name} must define a unit.");
      checkDaliDependencyError = true;
    }
    // ERROR if dali dependency has a window less than one.
    if (daliDependency.params["window"] < 1) {
      project.logger.lifecycle("TriggerChecker ERROR: DaliDependency ${daliDependency.name} in Trigger ${trigger.name} in Workflow ${workflow.name} must have a window greater than 0.");
      checkDaliDependencyError = true;
    }
    // ERROR if dali dependency has a delay less than zero.
    if (daliDependency.params["delay"] < 0) {
      project.logger.lifecycle("TriggerChecker ERROR: DaliDependency ${daliDependency.name} in Trigger ${trigger.name} in Workflow ${workflow.name} must have a nonnegative delay.");
      checkDaliDependencyError = true;
    }
    // ERROR if dali dependency has a unit that is neither 'daily' nor 'hourly'.
    String unit = daliDependency.params["unit"];
    if (unit != 'daily' && unit != 'hourly') {
      project.logger.lifecycle("TriggerChecker ERROR: 'unit' value in DaliDependency ${daliDependency.name} in Trigger ${trigger.name} in Workflow ${workflow.name} can only be set to 'daily' or 'hourly'.");
      checkDaliDependencyError = true;
    }

    return checkDaliDependencyError;
  }
}
