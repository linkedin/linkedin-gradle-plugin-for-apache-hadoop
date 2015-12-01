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
package com.linkedin.gradle.lizip;

import com.linkedin.gradle.zip.HadoopZipExtension;

import org.apache.tools.ant.filters.ReplaceTokens;

import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.bundling.Zip;

/**
 * LinkedIn-specific customizations to the HadoopZipExtension class. In particular, this class
 * enables users to declare that they should generate a CRT deployment zip that contains their .crt
 * file. This is particular to LinkedIn and required for LinkedIn's one-click deployment system.
 */
class LiHadoopZipExtension extends HadoopZipExtension {
  boolean declaresCRT = false;
  boolean declaresMain = false;

  /**
   * Constructor for the LiHadoopZipExtension.
   *
   * @param project The Gradle project
   */
  LiHadoopZipExtension(Project project) {
    super(project);
  }

  /**
   * <pre>
   *   hadoopZip {
   *     CRT { }
   *   }
   * </pre>
   */
  void CRT(Closure closure) {
    if (declaresMain) {
      throw new Exception("You cannot declare both a main zip and a CRT zip. Refer to go/HadoopCRT for more information.");
    }
    declaresCRT = true;
    createDeploymentZipTask(this.project);
  }

  /**
   * <pre>
   *   hadoopZip {
   *     main {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *   }
   * </pre>
   * The DSL inside the {@code main\{} } block is the same DSL used for Copy tasks.
   */
  @Override
  void main(Closure closure) {
    if (declaresCRT) {
      throw new Exception("You cannot declare both a main zip and a CRT zip. Refer to go/HadoopCRT for more information.");
    }
    declaresMain = true;
    super.main(closure);
  }

  /**
   * <pre>
   *   hadoopZip {
   *     zip("magic") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *     zip("canasta") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *   }
   * </pre>
   * The DSL inside the {@code zip(zipName)\{} } block is the same DSL used for Copy tasks.
   */
  @Override
  void zip(String zipName, Closure closure) {
    if ("CRT".equals(zipName)) {
      throw new Exception("You cannot declare a zip called CRT. Use the CRT method to declare that your project uses CRT. Refer to go/HadoopCRT for more information.");
    }
    super.zip(zipName, closure);
  }

  /** 
   * Creates a zip archive that contains the .crt file and is setup correctly to work with CRT. CRT
   * has a couple of weird bugs. To be able to deploy from CRT, there must be:
   *
   * 1. An "artifact": line in the artifact-spec.json for this project. This is required by mint
   *    deploy and crt deploy.
   *
   * 2. An artifact in Artifactory with no classifier in the file name, i.e. the entry must be of
   *    the form: ${project.name}-${project.version}.zip.
   *
   * The Tools Python code explicitly tries to form this name as a part of the URL it downloads,
   * since it assumes you want to deploy the project artifact, rather than something with a
   * classifier in the name. Thus if you "artifact" line in the artifact-spec.json for your project
   * is ${project.name}-${project.version}-azkaban.zip, it won't work!
   *
   * To work around these problems, we'll give the CRT zip the special "defaultArtifact"
   * classifier. When the Tools Python code constructs the artifact-spec, it will explicitly use an
   * artifact with this classifier in the "artifact" line. This accomplishes #1 above.
   *
   * Then we'll explicitly set the archiveName property on the zip so that it has the right form.
   * This accomplishes #2 above. These two together produce the artifacts and an artifact-spec.json
   * that is compatible with CRT.
   *
   * @param project The Gradle project
   * @return The zip task
   */
  Task createDeploymentZipTask(Project project) {
    return project.tasks.create(name: "CRTHadoopZip", type: Zip) { task ->
      archiveName = "${project.name}-${project.version}.zip";
      classifier = "defaultArtifact";;
      description = "Creates a Hadoop CRT deployment zip archive";
      group = "Hadoop Plugin";

      // This task is a dependency of buildHadoopZips and depends on the startHadoopZips
      project.tasks["buildHadoopZips"].dependsOn task;
      dependsOn "startHadoopZips";

      // Check that the .crt file exists
      if (!new File("${project.projectDir}/.crt").exists()) {
        throw new GradleException("Could not find a .crt file at ${project.projectDir}. See go/HadoopCRT for more information.");
      }

      // The deployment zip consists of just the .crt file
      from("${project.projectDir}/.crt") {
        filter(ReplaceTokens, tokens: [version: project.version.toString()]);
      }

      // Add the task to project artifacts
      project.artifacts.add("archives", task);

      // When everything is done, print out a message
      doLast {
        project.logger.lifecycle("Prepared Hadoop zip archive at: ${archivePath}");
      }
    }
  }
}
