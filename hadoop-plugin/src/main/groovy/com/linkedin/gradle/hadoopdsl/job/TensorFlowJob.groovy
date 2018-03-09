/*
 * Copyright 2018 LinkedIn Corp.
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
package com.linkedin.gradle.hadoopdsl.job;

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;

/**
 * Job class for type=TensorFlowJob jobs.
 * <p>
 * In the DSL, a TensorFlowJob can be specified with:
 * <pre>
 *   tensorFlowJob('jobName') {
 *     amMemory 2048
 *     amCores 1
 *     psMemory 2048
 *     psCores 1
 *     workerMemory 8192
 *     workerCores 1
 *     numPs 2
 *     numWorkers 4
 *     archive 'tensorflow-starter-kit-1.4.16-SNAPSHOT-azkaban.zip'
 *     jar 'tensorflow-on-yarn-0.0.1.jar'
 *     taskCommand ''
 *     set workerEnv: [
 *       'ENV1': 'val1',
 *       'ENV2': 'val2'
 *     ]
 *   }
 * </pre>
 */
class TensorFlowJob extends HadoopJavaProcessJob {
  int amMemory;
  int amCores;
  int psMemory;
  int psCores;
  int workerMemory;
  int workerCores;
  int numPs;
  int numWorkers;
  String archive;
  String jar;
  String taskCommand;
  Map<String, Object> workerEnv;

  /**
   * Constructor for a TensorFlowJob.
   *
   * @param jobName The job name
   */
  TensorFlowJob(String jobName) {
    super(jobName);
    workerEnv = new HashMap<>();
    setJobProperty("type", "TensorFlowJob");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  TensorFlowJob clone() {
    return clone(new TensorFlowJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  TensorFlowJob clone(TensorFlowJob cloneJob) {
    cloneJob.amMemory = amMemory;
    cloneJob.amCores = amCores;
    cloneJob.psMemory = psMemory;
    cloneJob.psCores = psCores;
    cloneJob.workerMemory = workerMemory;
    cloneJob.workerCores = workerCores;
    cloneJob.numPs = numPs;
    cloneJob.numWorkers = numWorkers;
    cloneJob.archive = archive;
    cloneJob.jar = jar;
    cloneJob.taskCommand = taskCommand;
    cloneJob.workerEnv.putAll(workerEnv);
    return ((TensorFlowJob)super.clone(cloneJob));
  }

  /**
   * DSL method to set the am memory.
   *
   * @param amMemory amMemory for the TensorFlow job
   */
  @HadoopDslMethod
  void amMemory(int amMemory) {
    this.amMemory = amMemory;
    setJobProperty("am_memory", this.amMemory);
  }

  /**
   * DSL method to set the am cores.
   *
   * @param amCores amCores for the TensorFlow job
   */
  @HadoopDslMethod
  void amCores(int amCores) {
    this.amCores = amCores;
    setJobProperty("am_vcores", this.amCores);
  }

  /**
   * DSL method to set the ps memory.
   *
   * @param psMemory psMemory for the TensorFlow job
   */
  @HadoopDslMethod
  void psMemory(int psMemory) {
    this.psMemory = psMemory;
    setJobProperty("ps_memory", this.psMemory);
  }

  /**
   * DSL method to set the ps cores.
   *
   * @param psCores psCores for the TensorFlow job
   */
  @HadoopDslMethod
  void psCores(int psCores) {
    this.psCores = psCores;
    setJobProperty("ps_vcores", this.psCores);
  }

  /**
   * DSL method to set the worker memory.
   *
   * @param workerMemory workerMemory for the TensorFlow job
   */
  @HadoopDslMethod
  void workerMemory(int workerMemory) {
    this.workerMemory = workerMemory;
    setJobProperty("worker_memory", this.workerMemory);
  }

  /**
   * DSL method to set the worker cores.
   *
   * @param workerCores workerCores for the TensorFlow job
   */
  @HadoopDslMethod
  void workerCores(int workerCores) {
    this.workerCores = workerCores;
    setJobProperty("worker_vcores", this.workerCores);
  }

  /**
   * DSL method to set the num ps.
   *
   * @param numPs numPs for the TensorFlow job
   */
  @HadoopDslMethod
  void numPs(int numPs) {
    this.numPs = numPs;
    setJobProperty("num_ps", this.numPs);
  }

  /**
   * DSL method to set the num workers.
   *
   * @param numWorkers numWorkers for the TensorFlow job
   */
  @HadoopDslMethod
  void numWorkers(int numWorkers) {
    this.numWorkers = numWorkers;
    setJobProperty("num_workers", this.numWorkers);
  }

  /**
   * DSL method to set the archive.
   *
   * @param archive archive for the TensorFlow job
   */
  @HadoopDslMethod
  void archive(String archive) {
    this.archive = archive;
    setJobProperty("archive", this.archive);
  }

  /**
   * DSL method to set the jar.
   *
   * @param jar jar for the TensorFlow job
   */
  @HadoopDslMethod
  void jar(String jar) {
    this.jar = jar;
    setJobProperty("jar", this.jar);
  }

  /**
   * DSL method to set the task command.
   *
   * @param taskCommand Task command for the TensorFlow job
   */
  @HadoopDslMethod
  void taskCommand(String taskCommand) {
    this.taskCommand = taskCommand;
    setJobProperty("task_command", this.taskCommand);
  }

  /**
   * DSL method to set the worker environment.
   *
   * @param workerEnv Worker environment for the TensorFlow job
   */
  @HadoopDslMethod
  @Override
  void set(Map args) {
    super.set(args);
    if (args.containsKey("workerEnv")) {
      Map<String, Object> workerEnv = args.workerEnv;
      workerEnv.each { String name, Object value ->
        setWorkerEnv(name, value);
      }
    }
  }

  @HadoopDslMethod
  void setWorkerEnv(String name, Object value) {
    this.workerEnv.put(name, value);
    setJobProperty("worker_env.${name}", value);
  }
}
