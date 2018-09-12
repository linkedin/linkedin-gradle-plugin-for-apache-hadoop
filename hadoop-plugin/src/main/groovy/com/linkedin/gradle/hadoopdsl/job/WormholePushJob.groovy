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
 * Job class for type=WormholePushJob jobs. This job class is aimed at moving data from a HDFS path
 * to HDFS dropboxes that are accessible from online/nearline services at LinkedIn.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. Please check that these values are appropriate for your application. In the
 * DSL, a WormholePushJob can be specified with:
 * <pre>
 *   wormholePushJob('jobName') {
 *     fromInputPath '/user/input'            // Required
 *     toNamespace 'linkedin'                 // Required
 *     toDatasetGroup 'linkedin-project'      // Required
 *     toDataset 'model'                      // Required
 *     withVersionString '1.2.4-2018.09.04'   // Optional
 *     preserveInput true                     // Optional
 *     toOutputLocation 'LOCAL'               // Optional
 *   }
 * </pre>
 */
class WormholePushJob extends HadoopJavaJob {
  // Required
  String inputPath;
  String namespace;
  String datasetGroup;
  String dataset;

  // Optional
  String versionString;
  Boolean preserveInput;
  String outputLocation;

  /**
   * Constructor for WormholePushJob.
   *
   * @param jobName - The job name
   */
  WormholePushJob(String jobName) {
    super(jobName);
    setJobProperty("type", "WormholePushJob");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  WormholePushJob clone() {
    return clone(new WormholePushJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob - The job being cloned
   * @return The cloned job
   */
  WormholePushJob clone(WormholePushJob cloneJob) {
    cloneJob.inputPath = inputPath;
    cloneJob.namespace = namespace;
    cloneJob.datasetGroup = datasetGroup;
    cloneJob.dataset = dataset;
    cloneJob.versionString = versionString;
    cloneJob.preserveInput = preserveInput;
    cloneJob.outputLocation = outputLocation;
    return ((WormholePushJob)super.clone(cloneJob));
  }

  /**
   * DSL fromInputPath method causes input.path=value to be set in the job file.
   *
   * @param inputPath -  HDFS path of the dataset to be added to Wormhole. This can be a file or
   * a directory. The file system tree rooted at this location will be treated as the dataset.
   */
  @HadoopDslMethod
  void fromInputPath(String inputPath) {
    this.inputPath = inputPath;
    setJobProperty("input.path", inputPath);
  }

  /**
   * DSL toNamespace method causes namespace=value to be set in the job file.
   *
   * @param namespace - The namespace in which to write this dataset. Must already exist.
   */
  @HadoopDslMethod
  void toNamespace(String namespace) {
    this.namespace = namespace;
    setJobProperty("namespace", namespace);
  }

  /**
   * DSL toDatasetGroup method causes dataset.group=value to be set in the job file.
   *
   * @param datasetGroup - The dataset group in which to write this dataset. Must already exist.
   */
  @HadoopDslMethod
  void toDatasetGroup(String datasetGroup) {
    this.datasetGroup = datasetGroup;
    setJobProperty("dataset.group", datasetGroup);
  }

  /**
   * DSL toDataset method causes dataset=value to be set in the job file.
   *
   * @param dataset - The name of the dataset to write to. Must already exist.
   */
  @HadoopDslMethod
  void toDataset(String dataset) {
    this.dataset = dataset;
    setJobProperty("dataset", dataset);
  }

  /**
   * DSL withVersionString method causes version.string=value to be set in the job file.
   *
   * @param versionString - Optional: The version to associate with this push to the dataset. If not
   * specified, the new version will generated via auto-increment.
   */
  @HadoopDslMethod
  void withVersionString(String versionString) {
    this.versionString = versionString;
    setJobProperty("version.string", versionString);
  }

  /**
   * DSL preserveInput method causes preserve.input=value to be set in the job file.
   *
   * @param preserveInput - Optional: If true, copy the input dataset instead of moving it. When set to
   * true, this will preserve the input data. Note that this is more expensive and may require launching
   * a MapReduce job (DistCp).
   * Default: true
   */
  @HadoopDslMethod
  void preserveInput(Boolean preserveInput) {
    this.preserveInput = preserveInput;
    setJobProperty("preserve.input", preserveInput);
  }

  /**
   * DSL toOutputLocation method causes output.location=value to be set in the job file.
   *
   * @param promoteToProd - Optional: The output Wormhole location to push the new version to. This
   * defaults to LOCAL, which will automatically resolve to the local cluster in which this job is
   * being run.
   * Default: LOCAL
   */
  @HadoopDslMethod
  void toOutputLocation(String outputLocation) {
    this.outputLocation = outputLocation;
    setJobProperty("output.location", outputLocation);
  }
}
