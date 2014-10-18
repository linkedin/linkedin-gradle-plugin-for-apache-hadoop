/*
 * Copyright 2014 LinkedIn Corp.
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
package com.linkedin.gradle.azkaban;

import com.linkedin.gradle.hadoopdsl.BaseCompiler;
import com.linkedin.gradle.hadoopdsl.Job;
import com.linkedin.gradle.hadoopdsl.Properties;

import org.gradle.api.Project;

/**
 * Hadoop DSL compiler for Azkaban.
 */
class AzkabanDslCompiler extends BaseCompiler {
  /**
   * Constructor for the AzkabanDslCompiler.
   *
   * @param project The Gradle project
   */
  AzkabanDslCompiler(Project project) {
    super(project);
  }

  /**
   * Cleans up generated files from the build directory.
   *
   * @param buildDirectoryFile Java File object representing the build directory
   */
  @Override
  void cleanBuildDirectory(File buildDirectoryFile) {
    buildDirectoryFile.eachFileRecurse(groovy.io.FileType.FILES) { f ->
      String fileName = f.getName().toLowerCase();
      if (fileName.endsWith(".job") || fileName.endsWith(".properties")) {
        f.delete();
      }
    }
  }

  /**
   * Builds a properties file.
   *
   * @param props The Properties object to build
   */
  @Override
  void visitProperties(Properties props) {
    if (props.properties.keySet().size() == 0) {
      return;
    }

    String fileName = props.buildFileName(props.name, this.parentScope);
    File file = new File(this.buildDirectory, "${fileName}.properties");

    file.withWriter { out ->
      out.writeLine("# This file generated from the Hadoop DSL. Do not edit by hand.");
      properties.each() { key, value ->
        out.writeLine("${key}=${value}");
      }
    }

    // Set to read-only to remind people that they should not be editing the job files.
    file.setWritable(false);
  }

  /**
   * Builds a job file.
   *
   * @param job The job to build
   */
  @Override
  void visitJob(Job job) {
    // Use a LinkedHashMap so that the properties will be enumerated in the order they are added.
    Map<String, String> allProperties = job.buildProperties(new LinkedHashMap<String, String>(), this.parentScope);

    if (allProperties.size() == 0) {
      return;
    }

    String fileName = job.buildFileName(job.name, this.parentScope);
    File file = new File(this.buildDirectory, "${fileName}.job");

    file.withWriter { out ->
      out.writeLine("# This file generated from the Hadoop DSL. Do not edit by hand.");
      allProperties.each() { key, value ->
        out.writeLine("${key}=${value}");
      }
    }

    // Set to read-only to remind people that they should not be editing the job files.
    file.setWritable(false);
  }
}