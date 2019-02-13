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

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

/**
 * Job class for type=TonyJob and type=TensorFlowJob (deprecated) jobs.
 * <p>
 * In the DSL, a TonyJob can be specified with:
 * <pre>
 *   tonyJob('jobName') {
 *     def taskParams = [
 *       "--tensorboard",
 *       "--hdfs_input_path /tmp/trainingInput",
 *       "--hdfs_output_path /tmp/trainingOutput",
 *       "--learning_rate 0.25",
 *       "--lambda_l2 0.01",
 *     ].join(' ')
 *     set properties: [
 *       'python_binary_path': 'Python-2.7.11/bin/python',
 *       'python_venv': "my-venv.zip",
 *       'task_params': taskParams,
 *       'tony.ps.memory': '2g',
 *       'tony.ps.vcores': 1,
 *       'tony.worker.memory': '8g',
 *       'tony.worker.vcores': 1,
 *       'tony.worker.gpus': 1,
 *       'tony.ps.instances': 2,
 *       'tony.worker.instances': 4
 *     ]
 *     executes path/to/python/script.py
 *     set workerEnv: [
 *       'ENV1': 'val1',
 *       'ENV2': 'val2'
 *     ]
 *   }
 * </pre>
 *
 * <p>
 * A TensorFlowJob using TonY (deprecated, please use TonyJob) can be specified with:
 * <pre>
 *   tensorFlowJob('jobName', 'tony') {
 *     def taskParams = [ ... ].join(' ')
 *     set properties: [ ... ]
 *     executes path/to/python/script.py
 *     set workerEnv: [ ... ]
 *   }
 * </pre>
 */
class TonyJob extends HadoopJavaProcessJob implements TensorFlowJob {

  private final static Logger logger = Logging.getLogger(TonyJob);

  String executePath;
  String amMemory;
  int amCores;
  int amGpus;
  String psMemory;
  int psCores;
  String workerMemory;
  int workerCores;
  int workerGpus;
  int numPs;
  int numWorkers;
  String archive;
  Map<String, Object> workerEnv;

  /**
   * Constructor for a TonyJob.
   *
   * @param jobName The job name
   */
  TonyJob(String jobName) {
    super(jobName);
    workerEnv = new HashMap<>();
    setJobProperty("type", "TonyJob");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  TonyJob clone() {
    return clone(new TonyJob(name));
  }

  @Override
  TensorFlowJob clone(TensorFlowJob cloneJob) {
    cloneJob.executePath = executePath;
    cloneJob.amMemory = amMemory;
    cloneJob.amCores = amCores;
    cloneJob.amGpus = amGpus;
    cloneJob.psMemory = psMemory;
    cloneJob.psCores = psCores;
    cloneJob.workerMemory = workerMemory;
    cloneJob.workerCores = workerCores;
    cloneJob.workerGpus = workerGpus;
    cloneJob.numPs = numPs;
    cloneJob.numWorkers = numWorkers;
    cloneJob.archive = archive;
    cloneJob.workerEnv.putAll(workerEnv);
    return ((TensorFlowJob)super.clone(cloneJob));
  }

  private void printDeprecatedParamMessage(String deprecatedParam, String config) {
    logger.warn(sprintf("%s is deprecated in favor of setting '%s' in your properties block.", [deprecatedParam, config]))
  }

  @Override
  void executes(String executePath) {
    this.executePath = executePath;
    setJobProperty("executes", this.executePath);
  }

  @Override
  void amMemory(String amMemory) {
    this.amMemory = amMemory;
    printDeprecatedParamMessage("amMemory", "tony.am.memory")
    setJobProperty("tony.am.memory", this.amMemory);
  }

  @Override
  void amCores(int amCores) {
    this.amCores = amCores;
    printDeprecatedParamMessage("amCores", "tony.am.vcores")
    setJobProperty("tony.am.vcores", this.amCores);
  }

  @Override
  void amGpus(int amGpus) {
    this.amGpus = amGpus;
    printDeprecatedParamMessage("amGpus", "tony.am.gpus")
    setJobProperty("tony.am.gpus", this.amGpus);
  }

  @Override
  void psMemory(String psMemory) {
    this.psMemory = psMemory;
    printDeprecatedParamMessage("psMemory", "tony.ps.memory")
    setJobProperty("tony.ps.memory", this.psMemory);
  }

  @Override
  void psCores(int psCores) {
    this.psCores = psCores;
    printDeprecatedParamMessage("psCores", "tony.ps.vcores")
    setJobProperty("tony.ps.vcores", this.psCores);
  }

  @Override
  void workerMemory(String workerMemory) {
    this.workerMemory = workerMemory;
    printDeprecatedParamMessage("workerMemory", "tony.worker.memory")
    setJobProperty("tony.worker.memory", this.workerMemory);
  }

  @Override
  void workerCores(int workerCores) {
    this.workerCores = workerCores;
    printDeprecatedParamMessage("workerCores", "tony.worker.vcores")
    setJobProperty("tony.worker.vcores", this.workerCores);
  }

  @Override
  void workerGpus(int workerGpus) {
    this.workerGpus = workerGpus;
    printDeprecatedParamMessage("workerGpus", "tony.worker.gpus")
    setJobProperty("tony.worker.gpus", this.workerGpus);
  }

  @Override
  void numPs(int numPs) {
    this.numPs = numPs;
    printDeprecatedParamMessage("numPs", "tony.ps.instances")
    setJobProperty("tony.ps.instances", this.numPs);
  }

  @Override
  void numWorkers(int numWorkers) {
    this.numWorkers = numWorkers;
    printDeprecatedParamMessage("numWorkers", "tony.worker.instances")
    setJobProperty("tony.worker.instances", this.numWorkers);
  }

  @Override
  void archive(String archive) {
    this.archive = archive;
    logger.warn("archive param will be removed in the future. It's safe to remove, since it does nothing.")
  }

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
