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
 *   KabootarJob('jobName') {*     usesTrainedModelLocation '/user/testmodelregsvc/trained-models' // Required
 *     usesTrainingName 'AyeAyeCaptain'                       // Required
 *     usesAiProjectGroup 'AIFoundationOther'                 // Required
 *     usesWormholeNamespace 'testmodelregsvc'                // Required
 *     usesInitialImport 'initial/import/File'                // Required
 *     usesTrainingID '53fe1ff5-4439-4288-be43-11cb11629552'  // Optional
 *     usesOrigin 'FELLOWSHIP'                                // Optional
 *     usesFramework 'PHOTON_CONNECT'                         // Optional
 *     usesModelSupplementaryDataLocation '/user/testmodelregsvc/trained-models-supplementary-data' // Optional
 *     usesEnableQuasarModelBundle                            // Optional
 *     usesEnableAutoPublish                                  // Optional
 *     usesAutoPublishModelName                               // Required if usesEnableAutoPublish is true
 *     usesAutoPublishModelDeploymentGroupName                // Required if usesEnableAutoPublish is true
 *     usesAutoPublishVersionUpdateType                       // Optional
 *     usesAutoPublishModelContainsPiiData                    // Required if usesEnableAutoPublish is true
 *     usesAutoPublishModelContainsConfidentialData           // Required if usesEnableAutoPublish is true
 *}* </pre>*/
class KabootarJob extends HadoopJavaJob {
  // Required
  String trainedModelLocation;
  String trainingName;
  String aiProjectGroup;
  String wormholeNamespace;
  String initialImport;

  // Optional
  String trainingID;
  String origin;
  String framework;
  String modelSupplementaryDataLocation;
  Boolean enableQuasarModelBundle;
  Boolean enableAutoPublish;
  String autoPublishModelName; //required if enableAutoPublish is true
  String autoPublishModelDeploymentGroupName; //required if enableAutoPublish is true
  String autoPublishVersionUpdateType;
  Boolean autoPublishModelContainsPiiData; //required if enableAutoPublish is true
  Boolean autoPublishModelContainsConfidentialData; //required if enableAutoPublish is true


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
    cloneJob.aiProjectGroup = aiProjectGroup;
    cloneJob.wormholeNamespace = wormholeNamespace;
    cloneJob.initialImport = initialImport;
    cloneJob.trainingID = trainingID;
    cloneJob.origin = origin;
    cloneJob.framework = framework;
    cloneJob.modelSupplementaryDataLocation = modelSupplementaryDataLocation;
    cloneJob.enableQuasarModelBundle = enableQuasarModelBundle;
    cloneJob.enableAutoPublish = enableAutoPublish;
    cloneJob.autoPublishModelName = autoPublishModelName;
    cloneJob.autoPublishModelDeploymentGroupName = autoPublishModelDeploymentGroupName;
    cloneJob.autoPublishVersionUpdateType = autoPublishVersionUpdateType;
    cloneJob.autoPublishModelContainsPiiData = autoPublishModelContainsPiiData;
    cloneJob.autoPublishModelContainsConfidentialData = autoPublishModelContainsConfidentialData;
    return ((KabootarJob) super.clone(cloneJob));
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
   * DSL usesAiProjectGroup method causes ai.project.group=value to be set in the job file.
   *
   * @param aiProjectGroup - This contains the project group at LinkedIn.
   * The groups are used to connect related AI projects together.
   * Currently, these project groups correspond to AI verticals.
   */
  @HadoopDslMethod
  void usesAiProjectGroup(String aiProjectGroup) {
    this.aiProjectGroup = aiProjectGroup;
    setJobProperty("ai.project.group", aiProjectGroup);
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

  /**
   * DSL usesEnableQuasarModelBundle method causes enable.quasar.model.bundle to be set in the job file.
   *
   * @param enable.quasar.model.bundle - Flag that enables quasar model bundle format for models produced by Kabootar
   */
  @HadoopDslMethod
  void usesEnableQuasarModelBundle(Boolean enableQuasarModelBundle) {
    this.enableQuasarModelBundle = enableQuasarModelBundle;
    setJobProperty("enable.quasar.model.bundle", enableQuasarModelBundle);
  }

  /**
   * DSL usesEnableAutoPublish method causes auto.publish.enabled to be set in the job file.
   *
   * @param enableAutoPublish - Flag that controls whether Kabootar should auto-publish the trained model
   */
  @HadoopDslMethod
  void usesEnableAutoPublish(Boolean enableAutoPublish) {
    this.enableAutoPublish = enableAutoPublish;
    setJobProperty("auto.publish.enabled", enableAutoPublish);
  }

  /**
   * DSL usesAutoPublishModelName method causes auto.publish.model.name to be set in the job file.
   *
   * @param autoPublishModelName - Intended auto-published model name.
   */
  @HadoopDslMethod
  void usesAutoPublishModelName(String autoPublishModelName) {
    this.autoPublishModelName = autoPublishModelName;
    setJobProperty("auto.publish.model.name", autoPublishModelName);
  }

  /**
   * DSL usesAutoPublishModelDeploymentGroupName method causes auto.publish.model.deployment.group.name to be set in the
   * job file.
   *
   * @param autoPublishModelDeploymentGroupName - Intended model deployment group name that the trained model will be
   * auto-published into. This model deployment group must exist and the publisher needs to have DEVELOPMENT_TEAM role
   * for the group.
   */
  @HadoopDslMethod
  void usesAutoPublishModelDeploymentGroupName(String autoPublishModelDeploymentGroupName) {
    this.autoPublishModelDeploymentGroupName = autoPublishModelDeploymentGroupName;
    setJobProperty("auto.publish.model.deployment.group.name", autoPublishModelDeploymentGroupName);
  }

  /**
   * DSL usesAutoPublishVersionUpdateType method causes auto.publish.version.update.type to be set in the job file.
   *
   * @param autoPublishVersionUpdateType - auto-publish model version update type. Can be patch, minor or major.
   */
  @HadoopDslMethod
  void usesAutoPublishVersionUpdateType(String autoPublishVersionUpdateType) {
    this.autoPublishVersionUpdateType = autoPublishVersionUpdateType;
    setJobProperty("auto.publish.version.update.type", autoPublishVersionUpdateType);
  }

  /**
   * DSL usesAutoPublishModelContainsPiiData method causes auto.publish.model.contains.pii.data to be set in the job
   * file.
   *
   * @param autoPublishModelContainsPiiData - Indicate whether the to be auto-published trained model contains PII data.
   */
  @HadoopDslMethod
  void usesAutoPublishModelContainsPiiData(Boolean autoPublishModelContainsPiiData) {
    this.autoPublishModelContainsPiiData = autoPublishModelContainsPiiData;
    setJobProperty("auto.publish.model.contains.pii.data", autoPublishModelContainsPiiData);
  }

  /**
   * DSL usesAutoPublishModelContainsConfidentialData method causes auto.publish.model.contains.confidential.data to
   * be set in the job file.
   *
   * @param autoPublishModelContainsConfidentialData - Indicate whether the to be auto-published trained model contains
   * confidential data.
   */
  @HadoopDslMethod
  void usesAutoPublishModelContainsConfidentialData(Boolean autoPublishModelContainsConfidentialData) {
    this.autoPublishModelContainsConfidentialData = autoPublishModelContainsConfidentialData;
    setJobProperty("auto.publish.model.contains.confidential.data", autoPublishModelContainsConfidentialData);
  }
}
