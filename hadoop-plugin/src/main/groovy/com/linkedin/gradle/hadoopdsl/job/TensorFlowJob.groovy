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
 *       'executes': 'path/to/python/script.py',
 *       'task_params': taskParams,
 *     ]
 *     amMemory 2048
 *     amCores 1
 *     psMemory 2048
 *     psCores 1
 *     workerMemory 8192
 *     workerCores 1
 *     workerGpus 2
 *     numPs 2
 *     numWorkers 4
 *     archive 'tensorflow-starter-kit-1.4.16-SNAPSHOT-azkaban.zip'
 *     jar 'tensorflow-on-yarn-0.0.1.jar'
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
  int workerGpus;
  int numPs;
  int numWorkers;
  String archive;
  String jar;
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
    cloneJob.workerGpus = workerGpus;
    cloneJob.numPs = numPs;
    cloneJob.numWorkers = numWorkers;
    cloneJob.archive = archive;
    cloneJob.jar = jar;
    cloneJob.workerEnv.putAll(workerEnv);
    return ((TensorFlowJob)super.clone(cloneJob));
  }

  /**
   * Sets memory in MB for this TensorFlow job's ApplicationMaster.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param amMemory ApplicationMaster memory, in MB, for the TensorFlow job
   */
  @HadoopDslMethod
  void amMemory(int amMemory) {
    this.amMemory = amMemory;
    setJobProperty("am_memory", this.amMemory);
  }

  /**
   * Sets number of cores for this TensorFlow job's ApplicationMaster.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param amCores Number of ApplicationMaster cores for the TensorFlow job
   */
  @HadoopDslMethod
  void amCores(int amCores) {
    this.amCores = amCores;
    setJobProperty("am_vcores", this.amCores);
  }

  /**
   * Sets memory in MB for each parameter server's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param psMemory Memory in MB for each parameter server
   */
  @HadoopDslMethod
  void psMemory(int psMemory) {
    this.psMemory = psMemory;
    setJobProperty("ps_memory", this.psMemory);
  }

  /**
   * Sets number of cores for each parameter server's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param psCores Cores for each parameter server
   */
  @HadoopDslMethod
  void psCores(int psCores) {
    this.psCores = psCores;
    setJobProperty("ps_vcores", this.psCores);
  }

  /**
   * Sets memory in MB for each TensorFlow worker's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param workerMemory Memory in MB for each TensorFlow worker
   */
  @HadoopDslMethod
  void workerMemory(int workerMemory) {
    this.workerMemory = workerMemory;
    setJobProperty("worker_memory", this.workerMemory);
  }

  /**
   * Sets number of cores for each TensorFlow worker's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param workerCores Cores for each TensorFlow worker
   */
  @HadoopDslMethod
  void workerCores(int workerCores) {
    this.workerCores = workerCores;
    setJobProperty("worker_vcores", this.workerCores);
  }

  /**
   * Sets number of GPUs for each TensorFlow worker's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param workerGpus Number of GPUs for each TensorFlow worker
   */
  @HadoopDslMethod
  void workerGpus(int workerGpus) {
    this.workerGpus = workerGpus;
    setJobProperty("worker_gpus", this.workerGpus);
  }

  /**
   * Sets number of parameter server containers to request from YARN.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param numPs Number of parameter servers for this TensorFlow job
   */
  @HadoopDslMethod
  void numPs(int numPs) {
    this.numPs = numPs;
    setJobProperty("num_ps", this.numPs);
  }

  /**
   * Sets number of TensorFlow workers to request from YARN.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param numWorkers Number of TensorFlow workers for this TensorFlow job
   */
  @HadoopDslMethod
  void numWorkers(int numWorkers) {
    this.numWorkers = numWorkers;
    setJobProperty("num_workers", this.numWorkers);
  }

  /**
   * Sets the name of the archive containing TensorFlow training code, virtual env, etc.
   * to localize to each parameter server/TensorFlow worker. The Azkaban zip should contain
   * an archive with this name.
   *
   * @param archive Name of the archive to in the Azkaban zip to be localized in the YARN application
   */
  @HadoopDslMethod
  void archive(String archive) {
    this.archive = archive;
    setJobProperty("archive", this.archive);
  }

  /**
   * Sets the name of the jar in the Azkaban zip containing the code to run a TensorFlow
   * application on YARN.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param jar Name of the jar in the Azkaban zip containing TensorFlow application code
   */
  @HadoopDslMethod
  void jar(String jar) {
    this.jar = jar;
    setJobProperty("jar", this.jar);
  }

  /**
   * Sets environment variables which will be set for each parameter server/worker.
   *
   * @param workerEnv Environment for each container
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
