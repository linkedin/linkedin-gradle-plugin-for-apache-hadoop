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
package com.linkedin.gradle.liazkaban;

import com.linkedin.gradle.azkaban.AzkabanDslCompiler;
import com.linkedin.gradle.lihadoopdsl.lijob.LiPigBangBangJob;
import org.gradle.api.Project;

import static com.linkedin.gradle.liazkaban.LiAzkabanCompilerUtils.addBangBangProperties;
import static com.linkedin.gradle.liazkaban.LiAzkabanCompilerUtils.writeGradleForBangBangJob;

class LiAzkabanDslCompiler extends AzkabanDslCompiler {

  /**
   * Constructor for the AzkabanDslCompiler.
   *
   * @param project The Gradle project
   */
  LiAzkabanDslCompiler(Project project) {
    super(project);
  }

  /**
   * Separate visitor for the BangBang type Pig job to build and write Gradle file.
   *
   * @param job The LiPigBangBangJob to build
   */
  void visitJobToBuild(LiPigBangBangJob job) {
    Map<String, String> allProperties = job.buildProperties(this.parentScope);
    if (allProperties.size() == 0) {
      return;
    }

    writeGradleForBangBangJob(job, this.project, this.parentScope, this.parentDirectory);
    List<String> filteredKeys = addBangBangProperties(allProperties);

    String fileName = job.buildFileName(this.parentScope);
    File file = new File(this.parentDirectory, "${fileName}.job");

    file.withWriter { out ->
      out.writeLine("# This file generated from the Hadoop DSL. Do not edit by hand.");
      filteredKeys.each { key ->
        out.writeLine("${key}=${allProperties.get(key)}");
      }
    }

    // Set to read-only to remind people that they should not be editing the job files.
    file.setWritable(false);
  }

}
