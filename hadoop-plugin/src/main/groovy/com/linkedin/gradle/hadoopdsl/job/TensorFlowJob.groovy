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
 * Abstract class for tensorflow job. Subclasses will implement different
 * TensorFlow execution types.
 */
interface TensorFlowJob {

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  TensorFlowJob clone(TensorFlowJob cloneJob);

  /**
   * Sets user's python script entry point.
   *
   * @param executes Path to script to execute.
   */
  @HadoopDslMethod
  void executes(String executePath);

  /**
   * Sets memory in MB for this TensorFlow job's ApplicationMaster.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param amMemoryMB ApplicationMaster memory, in MB, for the TensorFlow job
   */
  @HadoopDslMethod
  void amMemoryMB(int amMemoryMB);

  /**
   * Sets number of cores for this TensorFlow job's ApplicationMaster.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param amCores Number of ApplicationMaster cores for the TensorFlow job
   */
  @HadoopDslMethod
  void amCores(int amCores);

  /**
   * Sets number of GPUs for TensorFlow AM.
   *
   * @param amGpus Number of GPUs for TensorFlow AM
   */
  @HadoopDslMethod
  void amGpus(int amGpus);

  /**
   * Sets memory in MB for each parameter server's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param psMemory Memory in MB for each parameter server
   */
  @HadoopDslMethod
  void psMemoryMB(int psMemoryMB);

  /**
   * Sets number of cores for each parameter server's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param psCores Cores for each parameter server
   */
  @HadoopDslMethod
  void psCores(int psCores);

  /**
   * Sets memory in MB for each TensorFlow worker's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param workerMemory Memory in MB for each TensorFlow worker
   */
  @HadoopDslMethod
  void workerMemoryMB(int workerMemoryMB);

  /**
   * Sets number of cores for each TensorFlow worker's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param workerCores Cores for each TensorFlow worker
   */
  @HadoopDslMethod
  void workerCores(int workerCores);

  /**
   * Sets number of GPUs for each TensorFlow worker's YARN container.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param workerGpus Number of GPUs for each TensorFlow worker
   */
  @HadoopDslMethod
  void workerGpus(int workerGpus);

  /**
   * Sets number of parameter server containers to request from YARN.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param numPs Number of parameter servers for this TensorFlow job
   */
  @HadoopDslMethod
  void numPs(int numPs);

  /**
   * Sets number of TensorFlow workers to request from YARN.
   * Leaving this unset will default to the Hadoop application's default value.
   *
   * @param numWorkers Number of TensorFlow workers for this TensorFlow job
   */
  @HadoopDslMethod
  void numWorkers(int numWorkers);

  /**
   * Sets the name of the archive containing TensorFlow training code, virtual env, etc.
   * to localize to each parameter server/TensorFlow worker. The Azkaban zip should contain
   * an archive with this name.
   *
   * @param archive Name of the archive to in the Azkaban zip to be localized in the YARN application
   */
  @HadoopDslMethod
  void archive(String archive);
}
