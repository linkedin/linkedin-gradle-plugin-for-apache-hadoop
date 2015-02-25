/*
 * Copyright 2014 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.gradle.hadoopdsl.checker;

import com.linkedin.gradle.hadoopdsl.BaseStaticChecker;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.Job;

import org.gradle.api.Project;

/**
 * The JobDependencyChecker makes the following checks:
 * <ul>
 *   <li>ERROR if there are cyclic target dependencies among jobs and subflows in a workflow</li>
 *   <li>ERROR if there are jobs that declare that they read paths that other jobs write but are
 *       not directly or transitively dependent on those jobs. This can lead to a
 *       "read-before-write" race condition.</li>
 * </ul>
 */
class JobDependencyChecker extends BaseStaticChecker {
  /**
   * Constructor for the JobDependencyChecker.
   *
   * @param project The Gradle project
   */
  JobDependencyChecker(Project project) {
    super(project);
  }

  @Override
  void visitWorkflow(Workflow workflow) {
    // First, recursively check the subflows
    workflow.workflows.each() { Workflow flow ->
      visitWorkflow(flow);
    }

    // Build a map of job names to jobs in the workflow and flow names to flows in the workflow
    Map<String, Workflow> flowMap = workflow.buildFlowMap();
    Map<String, Job> jobMap = workflow.buildJobMap();

    // ERROR if there are cyclic job dependencies among jobs and subflows in a workflow
    detectTargetCycles(workflow, flowMap, jobMap);

    // WARN if there are jobs that declare that they read paths that other jobs write but are not
    // directly or transitively dependent on those jobs
    detectReadWriteRaces(workflow, flowMap, jobMap);
  }

  /**
   * Helper function to detect cyclic target dependencies in a workflow. Targets in a workflow must
   * form a directed, acyclic graph.
   * <p>
   * This function performs a depth-first search of the target graph, starting from each job in the
   * workflow. If any job is encountered twice on a depth-first path, there is a cycle.
   *
   * @param workflow The workflow to check for dependency
   * @param flowMap The map of flow names to subflows in the workflow
   * @param jobMap The map of job names to jobs in the workflow
   */
  void detectTargetCycles(Workflow workflow, Map<String, Workflow> flowMap, Map<String, Job> jobMap) {
    Set<String> targetsChecked = new LinkedHashSet<String>();

    workflow.jobs.each() { Job job ->
      if (!targetsChecked.contains(job.name)) {
        Set<String> targetsOnPath = new LinkedHashSet<String>();
        detectTargetCycles(workflow, job.name, targetsChecked, targetsOnPath, flowMap, jobMap);
      }
    }

    workflow.workflows.each() { Workflow flow ->
      if (!targetsChecked.contains(flow.name)) {
        Set<String> targetsOnPath = new LinkedHashSet<String>();
        detectTargetCycles(workflow, flow.name, targetsChecked, targetsOnPath, flowMap, jobMap);
      }
    }
  }

  /**
   * Helper function called from the main detectTargetCycles method for workflows.
   * <p>
   * This helper function assumes that you have already checked that all declared workflow and job
   * target dependency names refer to jobs and subflows that belong to the workflow.
   *
   * @param workflow The workflow to check for target cycles
   * @param targetName The name of the current job or subflow being checked
   * @param targetsChecked The set of targets already checked for cycles
   * @param targetsOnPath The targets encountered on the path so far to the current target
   * @param flowMap The map of flow names to subflows in the workflow
   * @param jobMap The map of job names to jobs in the workflow
   */
  void detectTargetCycles(Workflow workflow, String targetName, Set<String> targetsChecked, Set<String> targetsOnPath, Map<String, Workflow> flowMap, Map<String, Job> jobMap) {
    if (targetsOnPath.contains(targetName)) {
      String cycleText = buildCyclesText(targetsOnPath, targetName);
      project.logger.lifecycle("JobDependencyChecker ERROR: workflow ${workflow.name} has a dependency cycle: ${cycleText}. The workflow dependencies must form a directed, acyclic graph.");
      foundError = true;
      return;
    }

    // Add this target to the path before we check its children.
    targetsOnPath.add(targetName);

    // Get the next set of target dependencies to check.
    Set<String> dependencyNames = jobMap.containsKey(targetName) ? jobMap.get(targetName).dependencyNames : flowMap.get(targetName).parentDependencies;

    dependencyNames.each() { String dependencyName ->
      if (!targetsChecked.contains(dependencyName)) {
        detectTargetCycles(workflow, dependencyName, targetsChecked, targetsOnPath, flowMap, jobMap);  // Assumes you have already checked that all dependency names refer to targets that belong to the workflow.
      }
    }

    // Now that we have checked its children, we are done checking this target for cycles.
    targetsChecked.add(targetName);
  }

  /**
   * Helper function to check if there are jobs in a workflow that declare that they read paths
   * that other jobs write but are not declared as directly or transitively dependent on those jobs.
   * This can lead to a "read-before-write" race condition.
   * <p>
   * This function first performs depth-first searches of the job graph to determine the set of
   * ancestors for each job. Then it builds a map of HDFS paths to the jobs that write those paths.
   * Next, it goes through each of the jobs and checks the paths read by each job. It verifies that
   * any other jobs that write to that path are ancestors of the job. If not, it sets the error
   * flag.
   *
   * @param workflow The workflow to check for read-before-write races
   * @param flowMap The map of flow names to subflows in the workflow
   * @param jobMap The map of job names to jobs in the workflow
   */
  void detectReadWriteRaces(Workflow workflow, Map<String, Workflow> flowMap, Map<String, Job> jobMap) {
    Map<String, Set<Job>> ancestorMap = buildAncestorMap(workflow, flowMap, jobMap);
    Map<String, Set<Job>> writeMap = buildWriteMap(workflow);

    // For each job, look at the paths declared as read by the job. For each read path, verify that
    // the jobs that write to that path are declared as (immediate or transitive) ancestors of the
    // job. If not, emit a warning.
    workflow.jobs.each() { Job job ->
      Set<Job> ancestors = ancestorMap.get(job.name);

      // Note that HDFS paths are case-sensitive, so we don't alter the path casing.
      job.reading.each() { String readPath ->
        if (writeMap.containsKey(readPath)) {
          writeMap.get(readPath).each() { Job writeJob ->
            if (job == writeJob) {
              project.logger.lifecycle("JobDependencyChecker WARNING: The job ${job.name} in the workflow ${workflow.name} declares that it both reads and writes the path ${readPath}. Please check that this is correct.");
            }
            else if (!ancestors.contains(writeJob)) {
              project.logger.lifecycle("JobDependencyChecker WARNING: The job ${job.name} in the workflow ${workflow.name} reads the path ${readPath} that is written by the job ${writeJob.name}, but does not have a direct or transitive dependency on this job. This is a potential read-before-write race condition.");
            }
          }
        }
      }
    }
  }

  /**
   * Helper function to build a map of target names in a workflow to the set of (direct or
   * transitive) job ancestors for each target.
   *
   * @param workflow The workflow for which to build the ancestor map
   * @param flowMap The map of flow names to subflows in the workflow
   * @param jobMap The map of job names to jobs in the workflow
   * @return The map of target names to the set of (direct or transitive) job ancestors for each target
   */
  Map<String, Set<Job>> buildAncestorMap(Workflow workflow, Map<String, Workflow> flowMap, Map<String, Job> jobMap) {
    Map<String, Set<Job>> ancestorMap = new HashMap<String, Set<Job>>();

    workflow.jobs.each() { Job job ->
      if (!ancestorMap.containsKey(job)) {
        buildAncestorSet(job.name, ancestorMap, flowMap, jobMap);
      }
    }

    workflow.workflows.each() { Workflow flow ->
      if (!ancestorMap.containsKey(flow)) {
        buildAncestorSet(flow.name, ancestorMap, flowMap, jobMap);
      }
    }

    return ancestorMap;
  }

  /**
   * Helper function to build the set of (direct or transitive) ancestors for a target in a
   * workflow.
   *
   * @param targetName The target for which to build the set of (direct or transitive) ancestors
   * @param ancestorMap The map of target names to the set of (direct or transitive) ancestor jobs for each target
   * @param flowMap The map of flow names to subflows in the workflow
   * @param jobMap The map of job names to jobs in the workflow
   * @return The set of (direct or transitive) job ancestors for the target
   */
  Set<Job> buildAncestorSet(String targetName, Map<String, Set<Job>> ancestorMap, Map<String, Workflow> flowMap, Map<String, Job> jobMap) {
    Set<Job> ancestors = new HashSet<Job>();

    // Put the set in the ancestor map right away. This way, if you have a cycle, when you get back
    // to this target you will find the set instead of going into an infinite loop.
    ancestorMap.put(targetName, ancestors);

    // Get the next set of target dependencies to check
    Set<String> dependencyNames = jobMap.containsKey(targetName) ? jobMap.get(targetName).dependencyNames : flowMap.get(targetName).parentDependencies;

    dependencyNames.each() { String dependencyName ->
      if (jobMap.containsKey(dependencyName)) {
        Job parentJob = jobMap.get(dependencyName);             // Assumes you have already checked that all dependency names refer to jobs that belong to the workflow
        Set<Job> parentAncestors = ancestorMap.get(parentJob.name);

        if (parentAncestors == null) {
          parentAncestors = buildAncestorSet(parentJob.name, ancestorMap, flowMap, jobMap);
        }

        ancestors.addAll(parentAncestors);
        ancestors.add(parentJob);
      }
      else {
        Workflow parentWorkflow = flowMap.get(dependencyName);  // Assumes you have already checked that all dependency names refer to jobs that belong to the workflow
        Set<Job> parentAncestors = ancestorMap.get(parentWorkflow.name);

        if (parentAncestors == null) {
          parentAncestors = buildAncestorSet(parentWorkflow.name, ancestorMap, flowMap, jobMap);
        }

        ancestors.addAll(parentAncestors);
      }
    }

    return ancestors;
  }

  /**
   * Helper function to build a map of HDFS paths to the jobs in the workflow that declare that
   * they write to that path.
   *
   * @param workflow The workflow for which to build the write map
   * @return The map of HDFS paths to the jobs in the workflow that declare that they write to that path
   */
  Map<String, Set<Job>> buildWriteMap(Workflow workflow) {
    Map<String, Set<Job>> writeMap = new HashMap<String, Set<Job>>();

    // Note that HDFS paths are case-sensitive, so we don't alter the path casing.
    workflow.jobs.each() { Job job ->
      job.writing.each() { String writePath ->
        Set<Job> writeJobs = writeMap.get(writePath);

        if (writeJobs == null) {
          writeJobs = new HashSet<Job>();
          writeMap.put(writePath, writeJobs);
        }

        writeJobs.add(job);
      }
    }

    return writeMap;
  }
}