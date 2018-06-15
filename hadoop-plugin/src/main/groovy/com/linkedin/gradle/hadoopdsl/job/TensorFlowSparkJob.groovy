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
package com.linkedin.gradle.hadoopdsl.job
/**
 * Job class for type=TensorFlowJob jobs.
 * <p>
 * In the DSL, a TensorFlowJob can be specified with:
 * <pre>
 *   tensorFlowJob('jobName', 'spark') {
 *     executes path/to/python/script.py
 *     appParams params
 *     amMemory '2g'
 *     amCores 1
 *     psMemory '2g'
 *     psCores 1
 *     workerMemory '8g'
 *     workerCores 1
 *     numPs 2
 *     numWorkers 4
 *     set sparkConfs: [
 *       'key1' : 'val1',
 *       'key2' : 'val2'
 *     ]
 *   }
 * </pre>
 */
class TensorFlowSparkJob extends SparkJob implements TensorFlowJob {
  int numPs;
  int numWorkers;

  /**
   * Constructor for a TensorFlowJob.
   *
   * @param jobName The job name
   */
  TensorFlowSparkJob(String jobName) {
    super(jobName);
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  TensorFlowSparkJob clone() {
    return clone(new TensorFlowSparkJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  TensorFlowJob clone(TensorFlowJob cloneJob) {
    //TODO unimplemented
    return null;
  }

  @Override
  void executes(String executePath) {
    super.executes(executePath);
  }

  @Override
  void amMemory(String amMemory) {
    String amMemoryStr = amMemory;
    super.driverMemory(amMemoryStr);
  }

  @Override
  void amCores(int amCores) {
    super.setConf("spark.driver.cores", amCores);
  }

  @Override
  void amGpus(int amGpus) {
    throw new Exception("Requesting AM GPUs via tensorflow on spark is not supported currently.");
  }

  @Override
  void psMemory(String psMemory) {
    // If worker memory has already been set, ignore
    if (this.executorMemory == null) {
      String psMemoryStr = psMemory;
      super.executorMemory(psMemoryStr);
    }
  }

  @Override
  void psCores(int psCores) {
    // If worker cores has already been set, ignore
    if (this.executorCores == null) {
      super.executorCores(psCores);
    }
  }

  @Override
  void workerMemory(String workerMemory) {
    String workerMemoryStr = workerMemory;
    super.executorMemory(workerMemoryStr);
  }

  @Override
  void workerCores(int workerCores) {
    super.executorCores(workerCores);
  }

  @Override
  void workerGpus(int workerGpus) {
    throw new Exception("Requesting worker GPUs via tensorflow on spark is not supported currently.");
  }

  @Override
  void numPs(int numPs) {
    this.numPs = numPs;
    // Set num executors, if num workers has been set
    if (this.numWorkers != 0) {
      super.numExecutors(numPs + this.numWorkers);
    }
  }

  @Override
  void numWorkers(int numWorkers) {
    this.numWorkers = numWorkers;
    // Set num executors, if num ps has been set
    if (this.numPs != 0) {
      super.numExecutors(this.numPs + numWorkers);
    }
  }

  @Override
  void archive(String archive) {
    LOG.warn("Ignoring archive: ${archive} for tensorflow on spark");
  }
}
