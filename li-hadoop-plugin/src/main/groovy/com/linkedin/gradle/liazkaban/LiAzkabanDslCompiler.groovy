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
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.libangbang.LiHadoopShellCommand;
import com.linkedin.gradle.libangbang.LiHadoopShellCommandFactory;
import com.linkedin.gradle.lihadoopdsl.lijob.LiBangBangJob;
import com.linkedin.gradle.lihadoopdsl.lijob.LiPigBangBangJob;
import groovy.text.GStringTemplateEngine;
import groovy.text.Template;
import org.gradle.api.Project;

class LiAzkabanDslCompiler extends AzkabanDslCompiler {

  private static final String BANGBANG_TEMPLATE = "LiBangBangJob.template";
  private static final String PIG_JAVA_OPTS = "env.PIG_JAVA_OPTS";
  private LiHadoopShellCommandFactory bangBangCommandFactory;

  /**
   * Constructor for the AzkabanDslCompiler.
   *
   * @param project The Gradle project
   */
  LiAzkabanDslCompiler(Project project) {
    super(project);
    bangBangCommandFactory = new LiHadoopShellCommandFactory();
  }

  /**
   * Separate visitor for the BangBang type pig job to build and write gradle file
   * @param job The LiPigBangBangJob to build
   */
  void visitJobToBuild(LiPigBangBangJob job) {
    writeGradleForJob(job);
    writeJobFile(job);
  }

  /**
   * This file duplicates functionality from the superclass. We have to remove other
   * properties from the job file apart from the command and type
   * @param job The bangbang job
   */
  void writeJobFile(LiBangBangJob job) {
    Map<String, String> allProperties = job.buildProperties(this.parentScope);
    if (allProperties.size() == 0) {
      return;
    }

    Map<String,String> azkabanOptions = new HashMap<String,String>();
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_LINK_WORKFLOW_URL,"\${${LiAzkabanJavaProperties.AZKABAN_LINK_WORKFLOW_URL}}")
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_LINK_ATTEMPT_URL,"\${${LiAzkabanJavaProperties.AZKABAN_LINK_ATTEMPT_URL}}")
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_LINK_JOB_URL,"\${${LiAzkabanJavaProperties.AZKABAN_LINK_JOB_URL}}")
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_LINK_EXECUTION_URL,"\${${LiAzkabanJavaProperties.AZKABAN_LINK_EXECUTION_URL}}")
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_JOB_INNODES,"\${${LiAzkabanJavaProperties.AZKABAN_JOB_INNODES}}")
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_JOB_OUTNODES,"\${${LiAzkabanJavaProperties.AZKABAN_JOB_OUTNODES}}")

    // create jvm string of the form -Dkey1=value1 -Dkey2=value2
    StringBuffer azkabanOpts = new StringBuffer();
    azkabanOptions.each { key,value -> azkabanOpts.append("-D${key}=${value} "); }
    String azkabanJvmString =  azkabanOpts.toString();

    // add the azkabanjvmString to PIG_JAVA_OPTS to add them to job conf.
    if(allProperties.hasProperty(PIG_JAVA_OPTS)) {
      allProperties.put(PIG_JAVA_OPTS, allProperties.get(PIG_JAVA_OPTS) + " " + azkabanJvmString);
    } else {
      allProperties.put(PIG_JAVA_OPTS, azkabanJvmString);
    }
    // since it is a hadoopShell job type, only some of the properties are relevant.
    String fileName = job.buildFileName(this.parentScope);

    File file = new File(this.parentDirectory, "${fileName}.job");
    List<String> sortedKeys = sortPropertiesToBuild(allProperties.keySet());
    List<String> filteredKeys = new ArrayList<String>();
    Set<String> irrelevantProperties = ['pig.script','pig.home',/param.*/,'use.user.pig.jar',/hadoop-inject.*/];

    for(String key: sortedKeys) {
      boolean isRelevant = true;
      for(String property: irrelevantProperties) {
        if(key.matches(property)) {
          isRelevant = false;
          break;
        }
      }
      if(isRelevant) {
        filteredKeys.add(key);
      }
    }

    file.withWriter { out ->
      out.writeLine("# This file generated from the Hadoop DSL. Do not edit by hand.");
      filteredKeys.each { key ->
          out.writeLine("${key}=${allProperties.get(key)}");
      }
    }
    // Set to read-only to remind people that they should not be editing the job files.
    file.setWritable(false);
  }

  /**
   * Takes a LiBangBangJob and writes the gradle file for bangbang
   * @param job The BangBang Job
   */
  void writeGradleForJob(LiBangBangJob job) {
    String fileName = job.buildFileName(this.parentScope);
    File file = new File(this.parentDirectory, "${fileName}.gradle");
    if (job.isOverwritten()) {
      project.logger.lifecycle("Writing the ${fileName}.gradle to ${file.getAbsolutePath()}");
      file.write(getBangBangGradleText(job));
    }
  }

  /**
   * Extracts the information to write from the bangbang template
   * @param job The job for which information should be extracted
   * @return The Text for the gradle file
   */
  String getBangBangGradleText(LiBangBangJob job) {
    LiHadoopShellCommand command = bangBangCommandFactory.getCommand(job, job.buildProperties(this.parentScope));

    URL templateURL = Thread.currentThread().getContextClassLoader().getResource(BANGBANG_TEMPLATE);

    // create bindings for the template
    Map<String, Object> bindings = new HashMap<String, Object>();
    bindings.put("dependency", job.getDependency());
    bindings.put("executable", command.getExecutable());
    bindings.put("argList", command.getArguments())
    bindings.put("environmentMap", command.getEnvironment());

    // Get text from the template
    GStringTemplateEngine templateEngine = new GStringTemplateEngine();
    Template template = templateEngine.createTemplate(templateURL.text);
    String templateText = template.make(bindings).toString();
    return templateText;
  }
}
