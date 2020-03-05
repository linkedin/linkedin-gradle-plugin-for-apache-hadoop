/*
 * Copyright 2020 LinkedIn Corp.
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
 * Job class for type=kubernetesJob jobs.
 * <p>
 * In the DSL, a KubernetesJob can be specified with:
 * <pre>
 *   kubernetesJob('jobName') {
 *     def taskCmd = [
 *       "mpirun",
 *       "some-job",
 *       "--hdfs_input_path /tmp/trainingInput",
 *       "--hdfs_output_path /tmp/trainingOutput",
 *       "--learning_rate 0.25",
 *       "--lambda_l2 0.01",
 *     ].join(' ')
 *     kind 'MPIJob'              // Required
 *     namespace 'some-namespace' // Optional
 *     set properties: [
 *       'k8s.launcher.instances': 1,
 *       'k8s.worker.instances': 1,
 *       'k8s.containers.image': some.image.url,
 *       'k8s.worker.gpus': 4,
 *       'k8s.containers.command': taskCmd
 *      ]
 *   }
 *   </pre>
 */
class KubernetesJob extends Job {
  String kind;
  String namespace;

  /**
   * Constructor for a JavaProcessJob.
   *
   * @param jobName The job name
   */
  KubernetesJob(String jobName) {
    super(jobName);
    setJobProperty("type", "KubernetesJob");
  }

  @Override
  KubernetesJob clone() {
    return clone(new KubernetesJob(name));
  }

  KubernetesJob clone(KubernetesJob cloneJob) {
    cloneJob.kind = kind;
    cloneJob.namespace = namespace;
    return ((KubernetesJob)super.clone(cloneJob));
  }

  @HadoopDslMethod
  void kind(String kind) {
    this.kind = kind;
    setJobProperty("k8s.kind", kind);
  }

  @HadoopDslMethod
  void namespace(String namespace) {
    this.namespace = namespace;
    setJobProperty("k8s.namespace", namespace);
  }
}
