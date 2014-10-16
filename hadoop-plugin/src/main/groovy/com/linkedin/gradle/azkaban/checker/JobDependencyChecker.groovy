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
package com.linkedin.gradle.azkaban.checker;

import com.linkedin.gradle.azkaban.AzkabanJob;
import com.linkedin.gradle.azkaban.AzkabanWorkflow;
import com.linkedin.gradle.azkaban.BaseStaticChecker;

import org.gradle.api.Project;

/**
 * The JobDependencyChecker makes the following checks:
 * <ul>
 *   <li>ERROR if there are cyclic job dependencies</li>
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
  void visitAzkabanWorkflow(AzkabanWorkflow workflow) {
    // Build a map of job names to jobs in the workflow
    Map<String, AzkabanJob> jobMap = workflow.buildJobMap();

    // ERROR if there are cyclic job dependencies
    detectJobCycles(workflow, jobMap);

    // WARN if there are jobs that declare that they read paths that other jobs write but are not
    // directly or transitively dependent on those jobs
    detectReadWriteRaces(workflow, jobMap);
  }

  /**
   * Helper function to detect cyclic job dependencies in a workflow. Jobs in a workflow must a
   * directed, acyclic graph.
   * <p>
   * This function performs a depth-first search of the job graph, starting from each job in the
   * workflow. If any job is encountered twice on a depth-first path, there is a cycle.
   *
   * @param workflow The workflow to check for depedency
   * @param jobMap The map of job names to jobs in the workflow
   */
  void detectJobCycles(AzkabanWorkflow workflow, Map<String, AzkabanJob> jobMap) {
    Set<String> jobsChecked = new LinkedHashSet<String>();

    workflow.jobs.each() { AzkabanJob job ->
      if (!jobsChecked.contains(job.name)) {
        Set<String> jobsOnPath = new LinkedHashSet<String>();
        detectJobCycles(workflow, job, jobsChecked, jobsOnPath, jobMap);
      }
    }
  }

  /**
   * Helper function called from the main detectJobCycles method for workflows.
   * <p>
   * This helper function assumes that you have already checked that all declared workflow and job
   * dependency names refer to jobs that belong to the workflow.
   *
   * @param workflow The workflow to check for job cycles
   * @param job The current job being checked
   * @param jobsChecked The set of jobs already checked for cycles
   * @param jobsOnPath The jobs encountered on the path so far to the current job
   * @param jobMap The map of job names to jobs in the workflow
   */
  void detectJobCycles(AzkabanWorkflow workflow, AzkabanJob job, Set<String> jobsChecked, Set<String> jobsOnPath, Map<String, AzkabanJob> jobMap) {
    if (jobsOnPath.contains(job.name)) {
      String jobCycleText = detectJobCyclesText(jobsOnPath, job.name);
      project.logger.lifecycle("JobDependencyChecker ERROR: workflow ${workflow.name} has a dependency cycle: ${jobCycleText}. The workflow dependencies must form directed, acyclic graph.");
      foundError = true;
      return;
    }

    // Add this job to the path before we check its children.
    jobsOnPath.add(job.name);

    job.dependencyNames.each() { String dependencyName ->
      if (!jobsChecked.contains(dependencyName)) {
        AzkabanJob parentJob = jobMap.get(dependencyName);  // Assumes you have already checked that all dependency names refer to jobs that belong to the workflow.
        detectJobCycles(workflow, parentJob, jobsChecked, jobsOnPath, jobMap);
      }
    }

    // Now that we have checked its children, we are done checking this job for cycles.
    jobsChecked.add(job.name);
  }

  /**
   * Helper function to build a message describing a job cycle.
   *
   * @param jobsOnPath The (LinkedHashSet) set jobs that form a cyclic dependency
   * @param jobName The name of the job that is cyclic
   */
  String detectJobCyclesText(Set<String> jobsOnPath, String jobName) {
    List<String> jobsList = new ArrayList<String>(jobsOnPath);
    jobsList.add(jobName);
    return jobsList.join("->");
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
   * @param jobMap The map of job names to jobs in the workflow
   */
  void detectReadWriteRaces(AzkabanWorkflow workflow, Map<String, AzkabanJob> jobMap) {
    Map<AzkabanJob, Set<AzkabanJob>> ancestorMap = buildAncestorMap(workflow, jobMap);
    Map<String, Set<AzkabanJob>> writeMap = buildWriteMap(workflow);

    // For each job, look at the paths declared as read by the job. For each read path, verify that
    // the jobs that write to that path are declared as (immediate or transitive) ancestors of the
    // job. If not, emit a warning.
    workflow.jobs.each() { AzkabanJob job ->
      Set<AzkabanJob> ancestors = ancestorMap.get(job);

      // Note that HDFS paths are case-sensitive, so we don't alter the path casing.
      job.reading.each() { String readPath ->
        if (writeMap.containsKey(readPath)) {
          writeMap.get(readPath).each() { AzkabanJob writeJob ->
            if (job == writeJob) {
              project.logger.lifecycle("JobDependencyChecker WARNING: The job ${job.name} in the workflow ${workflow.name} declares that it both reads and writes the path ${readPath}. Please check that this is correct.");
            }
            else if (!ancestors.contains(writeJob)) {
              project.logger.lifecycle("JobDependencyChecker ERROR: The job ${job.name} in the workflow ${workflow.name} reads the path ${readPath} that is written by the job ${writeJob.name}, but does not have a direct or transitive dependency on this job. This is a potential read-before-write race condition.");
              foundError = true;
            }
          }
        }
      }
    }
  }

  /**
   * Helper function to build a map of jobs in a workflow to the set of (direct or transitive)
   * ancestors for each job.
   *
   * @param workflow The workflow for which to build the ancestor map
   * @param jobMap The map of job names to jobs in the workflow
   * @return The map of jobs to the set of (direct or transitive) ancestors for each job
   */
  Map<AzkabanJob, Set<AzkabanJob>> buildAncestorMap(AzkabanWorkflow workflow, Map<String, AzkabanJob> jobMap) {
    Map<AzkabanJob, Set<AzkabanJob>> ancestorMap = new HashMap<AzkabanJob, Set<AzkabanJob>>();

    workflow.jobs.each() { AzkabanJob job ->
      if (!ancestorMap.containsKey(job)) {
        Set<AzkabanJob> ancestors = buildAncestorSet(job, ancestorMap, jobMap);
        ancestorMap.put(job, ancestors);
      }
    }

    return ancestorMap;
  }

  /**
   * Helper function to build the set of (direct or transitive) ancestors for a job in a workflow.
   *
   * @param job The job for which to build the set of (direct or transitive) ancestors
   * @param ancestorMap The map of jobs to the set of (direct or transitive) ancestors for each job
   * @param jobMap The map of job names to jobs in the workflow
   * @return The set of (direct or transitive) ancestors for the job
   */
  Set<AzkabanJob> buildAncestorSet(AzkabanJob job, Map<AzkabanJob, Set<AzkabanJob>> ancestorMap, Map<String, AzkabanJob> jobMap) {
    Set<AzkabanJob> ancestors = new HashSet<AzkabanJob>();

    job.dependencyNames.each() { String dependencyName ->
      AzkabanJob parentJob = jobMap.get(dependencyName);  // Assumes you have already checked that all dependency names refer to jobs that belong to the workflow.
      Set<AzkabanJob> parentAncestors = ancestorMap.get(parentJob);

      if (parentAncestors == null) {
        parentAncestors = buildAncestorSet(parentJob, ancestorMap);
        ancestorMap.put(parentJob, parentAncestor);
      }

      ancestors.addAll(parentAncestors);
      ancestors.add(parentJob);
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
  Map<String, Set<AzkabanJob>> buildWriteMap(AzkabanWorkflow workflow) {
    Map<String, Set<AzkabanJob>> writeMap = new HashMap<String, Set<AzkabanJob>>();

    // Note that HDFS paths are case-sensitive, so we don't alter the path casing.
    workflow.jobs.each() { AzkabanJob job ->
      job.writing.each() { String writePath ->
        Set<AzkabanJob> writeJobs = writeMap.get(writePath);

        if (writeJobs == null) {
          writeJobs = new HashSet<AzkabanJob>();
          writeMap.put(writePath, writeJobs);
        }

        writeJobs.add(job);
      }
    }

    return writeMap;
  }
}
