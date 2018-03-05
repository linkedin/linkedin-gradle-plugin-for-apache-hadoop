/*
 * Copyright 2015 LinkedIn Corp.
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
package com.linkedin.gradle.lihadoopdsl.lijob


import com.linkedin.gradle.hadoopdsl.NamedScope


/**
 * Extend the Hadoop Plugin Job classes with the LinkedIn-specific "autoTunePigLi" job type.
 * <p>
 * In the DSL, a AutoTunePigLiJob can be specified with:
 * <pre>
 *   autoTunePigLiJob('jobName') {*     uses 'myScript.pig'     // Required
 *     caches files: [
 *       'foo.jar' : '/user/bazz/foo.jar'
 *     ]
 *     cachesArchive files: [
 *       'foobar' : '/user/bazz/foobar.zip'
 *     ]
 *     reads files: [
 *       'foo' : '/data/databases/foo'
 *     ]
 *     writes files: [
 *       'bazz' : '/user/bazz/foobar'
 *     ]
 *     set parameters: [
 *       'param1' : 'val1',
 *       'param2' : 'val2',
 *       'enable_tuning': 'true',
 *       'optimizationMetric': 'RESOURCE'
 *     ]
 *     queue 'marathon
 *}* </pre>*/

class AutoTunePigLiJob extends PigLiJob {

  private static final String JOB_CLASS_PROPERTY = "job.class";
  private static final String OPTIMIZATION_VERSION_PROPERTY = "optimizationMetric";
  private static final String ENABLE_TUNING_PROPERTY = "enable_tuning";

  AutoTunePigLiJob(String jobName) {
    /**
     * Constructor for a AutoTunePigLiJob.
     *
     * @param jobName The job name
     */
    super(jobName);
    setJobProperty("type", "hadoopJava");
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  AutoTunePigLiJob clone() {
    return (AutoTunePigLiJob) clone(new AutoTunePigLiJob(name));
  }

  /**
   * Builds the job properties that go into the generated job file, except for the dependencies
   * property, which is built by the other overload of the buildProperties method.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jobProperties correctly.
   *
   * @param parentScope The parent scope in which to lookup the base properties
   * @return The job properties map that holds all the properties that will go into the built job file
   */
  @Override
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> autoTunePigBuildProperties = super.buildProperties(parentScope);
    autoTunePigBuildProperties.put(JOB_CLASS_PROPERTY, "com.linkedin.jobtype.HadoopTuneInPigJob");
    if (!autoTunePigBuildProperties.containsKey(OPTIMIZATION_VERSION_PROPERTY)) {
      autoTunePigBuildProperties.put(OPTIMIZATION_VERSION_PROPERTY, "RESOURCE");
    }
    if (!autoTunePigBuildProperties.containsKey(ENABLE_TUNING_PROPERTY)) {
        autoTunePigBuildProperties.put(ENABLE_TUNING_PROPERTY, "true");
    }
    return autoTunePigBuildProperties;
  }
}
