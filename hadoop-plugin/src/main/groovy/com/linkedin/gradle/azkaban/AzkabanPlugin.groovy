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
package com.linkedin.gradle.azkaban

import com.linkedin.gradle.hadoopdsl.HadoopDslChecker;
import com.linkedin.gradle.hadoopdsl.HadoopDslFactory;
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin;
import groovy.json.JsonBuilder;
import groovy.json.JsonSlurper;
import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

import static com.linkedin.gradle.azkaban.AzkabanConstants.*;

/**
 * AzkabanPlugin implements features for Azkaban, including building the Hadoop DSL for Azkaban.
 */
class AzkabanPlugin implements Plugin<Project> {

  /**
   * Applies the AzkabanPlugin. This adds the Gradle task that builds the Hadoop DSL for Azkaban.
   * Plugin users should have their build tasks depend on this task.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    // Enable users to skip the plugin
    if (project.hasProperty("disableAzkabanPlugin")) {
      println("AzkabanPlugin disabled");
      return;
    }

    project.tasks.create("buildAzkabanFlows") {
      description = "Builds the Hadoop DSL for Azkaban. Have your build task depend on this task.";
      group = "Hadoop Plugin";

      doLast {
        HadoopDslPlugin plugin = project.extensions.hadoopDslPlugin;
        HadoopDslFactory factory = project.extensions.hadoopDslFactory;

        // Run the static checker on the DSL
        HadoopDslChecker checker = factory.makeChecker(project);
        checker.check(plugin);

        if (checker.failedCheck()) {
          throw new Exception("Hadoop DSL static checker FAILED");
        } else {
          logger.lifecycle("Hadoop DSL static checker PASSED");
        }

        AzkabanDslCompiler compiler = makeCompiler(project);
        compiler.compile(plugin);
      }
    }

    project.task("azkabanUpload", type: AzkabanUploadTask) { task ->
      description = "Uploads package to azkaban.";
      group = "Hadoop Plugin";

      doFirst {
        azkProject = readAzkabanProperties(project);
        String zipTask = azkProject.azkabanZipTask;
        if (zipTask == null) {
          throw new GradleException("\nPlease set the property 'azkabanZipTask' in the .azkabanPlugin.json file");
        }
        def zipTaskCont = project.getProject().tasks[zipTask];
        if (zipTaskCont == null) {
          throw new GradleException("\nTask " + zipTask + " doesn't exist. Please specify the right zip task after configuring it in your build file.");
        }
        archivePath = zipTaskCont.archivePath;
      }
    }

    project.tasks.create("writeAzkabanProperties") {
      description = "Creates a .azkabanPlugin.json file in the project directory with default properties.";
      group = "Hadoop Plugin";

      doLast {
        def azkabanPluginFilePath = "${project.getProjectDir()}/.azkabanPlugin.json";
        if (!new File(azkabanPluginFilePath).exists()) {
          String azkData = new JsonBuilder(new AzkabanProject()).toPrettyString();
          new File(azkabanPluginFilePath).write(azkData);
        }
      }
    }

  }

  /**
   * Factory method to build the Hadoop DSL compiler for Azkaban. Subclasses can override this
   * method to provide their own compiler.
   *
   * @param project The Gradle project
   * @return The AzkabanDslCompiler
   */
  AzkabanDslCompiler makeCompiler(Project project) {
    return new AzkabanDslCompiler(project);
  }

  /**
   * Load the properties defined in .azkabanPlugin.json file.
   * @return
   */
  AzkabanProject readAzkabanProperties(Project project) {
    def reader = null;
    def azkProject = null;
    try {
      reader = new BufferedReader(new FileReader("${project.getProjectDir()}/.azkabanPlugin.json"));
      def parsedData = new JsonSlurper().parse(reader);
      azkProject = new AzkabanProject();
      azkProject.azkabanUrl = parsedData[AZK_URL];
      azkProject.azkabanZipTask = parsedData[AZK_ZIP_TASK];
      azkProject.azkabanUsername = parsedData[AZK_USER_NAME];
      azkProject.azkabanProjName = parsedData[AZK_PROJ_NAME];
      azkProject.azkabanValidatorAutoFix = parsedData[AZK_VAL_AUTO_FIX];
    } catch (IOException ex) {
      throw new IOException(ex.toString() + "\n\nPlease run \"ligradle writeAzkabanProperties\" to create a default .azkabanPlugin.json file in your project directory which you can then edit.\n");
    } catch (Exception ex) {
      throw new Exception("\nError parsing .azkabanPlugin.json.\n" + ex.toString());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    azkProject;
  }

}