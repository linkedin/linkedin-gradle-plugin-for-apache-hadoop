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
package com.linkedin.gradle.hadoopdsl.job;

import com.linkedin.gradle.hadoopdsl.HadoopDslMethod;
import com.linkedin.gradle.hadoopdsl.NamedScope;

/**
 * Job class for type=tableau jobs. This job class is aimed at launching a job that
 * refreshes the specified tableau workbook.
 * <p>
 * In the example below, the values are NOT necessarily default values; they are simply meant to
 * illustrate the DSL. Please check that these values are appropriate for your application. In the
 * DSL, a TableauJob can be specified with:
 * <pre>
 *   tableauJob('jobName') {
 *     usesWorkbookName 'workbookName'
 *   }
 * </pre>
 */
class TableauJob extends HadoopJavaJob {
  // Required
  String workbookName;

  /**
   * Constructor for TableauJob.
   *
   * @param jobName The job name
   */
  TableauJob(String jobName) {
    super(jobName);
    setJobProperty("type", "tableau");
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
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> allProperties = super.buildProperties(parentScope);
    return allProperties;
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  TableauJob clone() {
    return clone(new TableauJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  TableauJob clone(TableauJob cloneJob) {
    cloneJob.workbookName = workbookName;
    return ((TableauJob)super.clone(cloneJob));
  }

  /**
   * DSL usesWorkbookName method causes workbook.name=value to be set in the job file.
   *
   * @param workbookName the workbook whose extract is refreshed
   */
  @HadoopDslMethod
  void usesWorkbookName(String workbookName) {
    this.workbookName = workbookName;
    setJobProperty("workbook.name", workbookName);
  }
}