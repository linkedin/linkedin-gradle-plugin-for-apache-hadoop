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
 * Job class for type=KabootarJob jobs. This job class is aimed at packaging the trained model
 * artifacts and make them available for LinkedIn Pro-ML model deployment pipeline.
 * <p>
 * The inputs origin and framework are to be chosen from enum <TODO add more info here, @skadam>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. Please check that these values are appropriate for your application. In the
 * DSL, a KabootarJob can be specified with:
 * <pre>
 *   KabootarJob('jobName') {
 *     usesTrainedModelLocation '/user/testmodelregsvc/trained-models' // Required
 *     usesTrainingName 'AyeAyeCaptain'                       // Required
 *     usesWormholeNamespace 'testmodelregsvc'                // Required
 *     usesInitialImport 'initial/import/File'                // Required
 *     usesTrainingID '53fe1ff5-4439-4288-be43-11cb11629552'  // Optional
 *     usesOrigin 'FELLOWSHIP'                                // Optional
 *     usesFramework 'PHOTON_CONNECT'                         // Optional
 *     usesModelSupplementaryDataLocation '/user/testmodelregsvc/trained-models-supplementary-data' // Optional
 *   }
 * </pre>
 */
class KabootarJob extends HadoopJavaJob {
  // Required
  String trainedModelLocation;
  String trainingName;
  String wormholeNamespace;
  String initialImport;

  // Optional
  String trainingID;
  String origin;
  String framework;
  String modelSupplementaryDataLocation;

  /**
   * Constructor for KabootarJob.
   *
   * @param jobName - The job name
   */
  KabootarJob(String jobName) {
    super(jobName);
    setJobProperty("type", "KabootarJob");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  KabootarJob clone() {
    return clone(new KabootarJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob - The job being cloned
   * @return The cloned job
   */
  KabootarJob clone(KabootarJob cloneJob) {
    cloneJob.trainedModelLocation = trainedModelLocation;
    cloneJob.trainingName = trainingName;
    cloneJob.wormholeNamespace = wormholeNamespace;
    cloneJob.initialImport = initialImport;
    cloneJob.trainingID = trainingID;
    cloneJob.origin = origin;
    cloneJob.framework = framework;
    cloneJob.modelSupplementaryDataLocation = modelSupplementaryDataLocation;
    return ((KabootarJob)super.clone(cloneJob));
  }

  /**
   * DSL usesTrainedModelLocation method causes trained.model.location=value to be set in the job file.
   *
   * @param trainedModelLocation -  HDFS path of the trained model.
   * The file system tree rooted at this location will be packaged.
   */
  @HadoopDslMethod
  void usesTrainedModelLocation(String trainedModelLocation) {
    this.trainedModelLocation = trainedModelLocation;
    setJobProperty("trained.model.location", trainedModelLocation);
  }

  /**
   * DSL usesTrainingName method causes training.name=value to be set in the job file.
   *
   * @param trainingName - Human readable name of the training run, which can be used to
   * identify group of training runs.
   */
  @HadoopDslMethod
  void usesTrainingName(String trainingName) {
    this.trainingName = trainingName;
    setJobProperty("training.name", trainingName);
  }

  /**
   * DSL usesWormholeNamespace method causes wormhole.namespace=value to be set in the job file.
   *
   * @param wormholeNamespace - Namespace in wormhole where the user wants packaged models to be copied.
   * This namespace must exist and user should have appropriate permission to write.
   */
  @HadoopDslMethod
  void usesWormholeNamespace(String wormholeNamespace) {
    this.wormholeNamespace = wormholeNamespace;
    setJobProperty("wormhole.namespace", wormholeNamespace);
  }

  /**
   * DSL usesInitialImport method causes initial.import=value to be set in the job file.
   *
   * @param initialImport - The initial model file which will be used by MRE(Model Runtime Environment)
   * to init the models. This initial import path is relative to the 'trained.model.location' path
   */
  @HadoopDslMethod
  void usesInitialImport(String initialImport) {
    this.initialImport = initialImport;
    setJobProperty("initial.import", initialImport);
  }


  /**
   * DSL usesTrainingID method causes training.id=value to be set in the job file.
   *
   * @param trainingID - UUID for the training run, to identify a training run uniquely.
   * If not provided, a UUID will be generated.
   *
   */
  @HadoopDslMethod
  void usesTrainingID(String trainingID) {
    this.trainingID = trainingID;
    setJobProperty("training.id", trainingID);
  }

  /**
   * DSL usesOrigin method causes origin=value to be set in the job file.
   *
   * @param origin - Origin of the training run.
   */
  @HadoopDslMethod
  void usesOrigin(String origin) {
    this.origin = origin;
    setJobProperty("origin", origin);
  }

  /**
   * DSL usesFramework method causes framework=value to be set in the job file.
   *
   * @param framework - Framework used to train the model.
   */
  @HadoopDslMethod
  void usesFramework(String framework) {
    this.framework = framework;
    setJobProperty("framework", framework);
  }

  /**
   * DSL usesModelSupplementaryDataLocation method causes model.supplementary.data.path=value to be set in the job file.
   *
   * @param modelSupplementaryDataLocation - Additional data that needs to be integrated with
   * Model Explorer should be stored in this directory.
   * Make sure a contract is already established with Model Explorer team,
   * for them to be able to parse and store the supplementary data.
   */
  @HadoopDslMethod
  void usesModelSupplementaryDataLocation(String modelSupplementaryDataLocation) {
    this.modelSupplementaryDataLocation = modelSupplementaryDataLocation;
    setJobProperty("model.supplementary.data.path", modelSupplementaryDataLocation);
  }
}
