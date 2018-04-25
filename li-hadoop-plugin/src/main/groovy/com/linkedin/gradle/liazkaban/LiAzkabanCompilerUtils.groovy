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
package com.linkedin.gradle.liazkaban

import com.linkedin.gradle.azkaban.AzkabanCompilerUtils;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.liazkaban.LiAzkabanJavaProperties;
import com.linkedin.gradle.libangbang.LiHadoopShellCommand;
import com.linkedin.gradle.libangbang.LiHadoopShellCommandFactory;
import com.linkedin.gradle.lihadoopdsl.lijob.LiBangBangJob;
import groovy.text.GStringTemplateEngine;
import groovy.text.Template
import org.gradle.api.Project;

/**
 * Helpful utils for the LiAzkaban Compilers.
 */
class LiAzkabanCompilerUtils extends AzkabanCompilerUtils {

  private static final String BANGBANG_TEMPLATE = "LiBangBangJob.template";
  private static final String PIG_JAVA_OPTS = "env.PIG_JAVA_OPTS";
  private static final LiHadoopShellCommandFactory bangBangCommandFactory =
          new LiHadoopShellCommandFactory();

  /**
   * This file duplicates functionality from the superclass. We have to remove other properties
   * from the job file apart from the command and type.
   *
   * @param allProperties The original map of properties from the job
   * @return List The filtered list of keys that need to be looked up in allProperties to get the
   *              resulting BangBang properties.
   */
  static List<String> addBangBangProperties(Map<String, String> allProperties) {
    Map<String,String> azkabanOptions = new HashMap<String,String>();
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_LINK_WORKFLOW_URL, "\${${LiAzkabanJavaProperties.AZKABAN_LINK_WORKFLOW_URL}}");
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_LINK_ATTEMPT_URL, "\${${LiAzkabanJavaProperties.AZKABAN_LINK_ATTEMPT_URL}}");
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_LINK_JOB_URL,"\${${LiAzkabanJavaProperties.AZKABAN_LINK_JOB_URL}}");
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_LINK_EXECUTION_URL, "\${${LiAzkabanJavaProperties.AZKABAN_LINK_EXECUTION_URL}}");
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_JOB_INNODES,"\${${LiAzkabanJavaProperties.AZKABAN_JOB_INNODES}}");
    azkabanOptions.put(LiAzkabanJavaProperties.AZKABAN_JOB_OUTNODES,"\${${LiAzkabanJavaProperties.AZKABAN_JOB_OUTNODES}}");

    // Create JVM string of the form -Dkey1=value1 -Dkey2=value2
    StringBuffer azkabanOpts = new StringBuffer();
    azkabanOptions.each { key, value -> azkabanOpts.append("-D${key}=${value} "); }
    String azkabanJvmString = azkabanOpts.toString();

    // Add the Azkaban JVM String to PIG_JAVA_OPTS to add them to job conf.
    if (allProperties.hasProperty(PIG_JAVA_OPTS)) {
      allProperties.put(PIG_JAVA_OPTS, allProperties.get(PIG_JAVA_OPTS) + " " + azkabanJvmString);
    } else {
      allProperties.put(PIG_JAVA_OPTS, azkabanJvmString);
    }

    // since it is a hadoopShell job type, only some of the properties are relevant.
    List<String> sortedKeys = sortPropertiesToBuild(allProperties.keySet());
    List<String> filteredKeys = new ArrayList<String>();
    Set<String> irrelevantProperties = ['pig.script','pig.home',/param.*/,'use.user.pig.jar',/hadoop-inject.*/];

    for (String key: sortedKeys) {
      boolean isRelevant = true;
      for (String property: irrelevantProperties) {
        if (key.matches(property)) {
          isRelevant = false;
          break;
        }
      }
      if (isRelevant) {
        filteredKeys.add(key);
      }
    }

    return filteredKeys;
  }

  /**
   * Takes a LiBangBangJob and writes the Gradle file for bangbang.
   *
   * @param job The LiBangBangJob Job to build
   * @param project The Gradle project
   * @param parentScope The parent scope of the LiBangBangJob
   * @param parentDirectory The string referring to the parent directory to write the gradle file to
   */
  static void writeGradleForBangBangJob(LiBangBangJob job, Project project, NamedScope parentScope,
          String parentDirectory) {
    String fileName = job.buildFileName(parentScope);
    File file = new File(parentDirectory, "${fileName}.gradle");
    if (job.isOverwritten()) {
      file.write(getBangBangGradleText(job, parentScope));
      project.logger.lifecycle("Writing the ${fileName}.gradle to ${file.getAbsolutePath()}");
    }
  }

  /**
   * Extracts the information to write from the bangbang template.
   *
   * @param job The job for which information should be extracted
   * @return The text for the gradle file
   */
  static String getBangBangGradleText(LiBangBangJob job, NamedScope parentScope) {
    LiHadoopShellCommand command = bangBangCommandFactory.getCommand(job, job.buildProperties(parentScope));
    URL templateURL = Thread.currentThread().getContextClassLoader().getResource(BANGBANG_TEMPLATE);

    // Create bindings for the template
    Map<String, Object> bindings = new HashMap<String, Object>();
    bindings.put("dependency", job.getDependency());
    bindings.put("executable", command.getExecutable());
    bindings.put("argList", command.getArguments());
    bindings.put("environmentMap", command.getEnvironment());

    // Get text from the template
    GStringTemplateEngine templateEngine = new GStringTemplateEngine();
    Template template = templateEngine.createTemplate(templateURL.text);
    String templateText = template.make(bindings).toString();
    return templateText;
  }
}
