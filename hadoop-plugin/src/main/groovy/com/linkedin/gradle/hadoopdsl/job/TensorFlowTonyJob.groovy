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
 * In the DSL, a TensorFlowJob using TonY can be specified with:
 * <pre>
 *   tensorFlowJob('jobName', 'tony') {
 *     def taskParams = [
 *       "--tensorboard",
 *       "--hdfs_input_path /tmp/trainingInput",
 *       "--hdfs_output_path /tmp/trainingOutput",
 *       "--learning_rate 0.25",
 *       "--lambda_l2 0.01",
 *     ].join(' ')
 *     set properties: [
 *       'python_binary_path': 'Python-2.7.11/bin/python',
 *       'python_venv': "tensorflow-starter-kit-1.4.16-SNAPSHOT-venv.zip",
 *       'task_params': taskParams,
 *     ]
 *     executes path/to/python/script.py
 *     amMemory 2g
 *     amCores 1
 *     amGpus 1
 *     psMemory 2g
 *     psCores 1
 *     workerMemory 8g
 *     workerCores 1
 *     workerGpus 2
 *     numPs 2
 *     numWorkers 4
 *     archive 'tensorflow-starter-kit-1.4.16-SNAPSHOT-azkaban.zip'
 *     set workerEnv: [
 *       'ENV1': 'val1',
 *       'ENV2': 'val2'
 *     ]
 *   }
 * </pre>
 */
class TensorFlowTonyJob extends HadoopJavaProcessJob implements TensorFlowJob {
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
   * Constructor for a TensorFlowJob.
   *
   * @param jobName The job name
   */
  TensorFlowTonyJob(String jobName) {
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
  TensorFlowTonyJob clone() {
    return clone(new TensorFlowTonyJob(name));
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

  @Override
  void executes(String executePath) {
    this.executePath = executePath;
    setJobProperty("executes", this.executePath);
  }

  @Override
  void amMemory(String amMemory) {
    this.amMemory = parseMemoryToMB(amMemory);
    setJobProperty("am_memory", this.amMemory);
  }

  @Override
  void amCores(int amCores) {
    this.amCores = amCores;
    setJobProperty("am_vcores", this.amCores);
  }

  @Override
  void amGpus(int amGpus) {
    this.amGpus = amGpus;
    setJobProperty("am_gpus", this.amGpus);
  }

  @Override
  void psMemory(String psMemory) {
    this.psMemory = parseMemoryToMB(psMemory);
    setJobProperty("ps_memory", this.psMemory);
  }

  @Override
  void psCores(int psCores) {
    this.psCores = psCores;
    setJobProperty("ps_vcores", this.psCores);
  }

  @Override
  void workerMemory(String workerMemory) {
    this.workerMemory = parseMemoryToMB(workerMemory);
    setJobProperty("worker_memory", this.workerMemory);
  }

  @Override
  void workerCores(int workerCores) {
    this.workerCores = workerCores;
    setJobProperty("worker_vcores", this.workerCores);
  }

  @Override
  void workerGpus(int workerGpus) {
    this.workerGpus = workerGpus;
    setJobProperty("worker_gpus", this.workerGpus);
  }

  @Override
  void numPs(int numPs) {
    this.numPs = numPs;
    setJobProperty("num_ps", this.numPs);
  }

  @Override
  void numWorkers(int numWorkers) {
    this.numWorkers = numWorkers;
    setJobProperty("num_workers", this.numWorkers);
  }

  @Override
  void archive(String archive) {
    this.archive = archive;
    setJobProperty("archive", this.archive);
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

  String parseMemoryToMB(String memString) {
    String parsedMem = memString.toLowerCase();
    if (parsedMem[-1] == "m") {
      parsedMem = parsedMem[0..-2];
    }
    if (parsedMem[-1] == "g") {
      parsedMem = parsedMem[0..-2] + "000";
    }
    return parsedMem;
  }

}
