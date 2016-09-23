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
package com.linkedin.gradle.hadoopValidator;

import com.linkedin.gradle.hadoopValidator.PigValidator.PigValidatorPlugin;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.UnknownTaskException;

/**
 * HadoopValidatorPlugin is the class that provides a Gradle Plugin which checks for Hadoop ecosystem applications
 * like Apache Pig.
 */
class HadoopValidatorPlugin implements Plugin<Project> {
  Project project;
  Properties properties;

  /**
   * Applies the Hadoop Validator Plugin, which in turn applies the PigValidator plugin.
   *
   * @param project The Gradle Project
   */
  @Override
  void apply(Project project) {
    this.project = project;

    if (project.hasProperty("disableHadoopValidatorPlugin")) {
      return;
    }
    readValidatorProperties();

    PigValidatorPlugin plugin = getPigValidatorPlugin().newInstance();
    plugin.properties = this.properties;
    plugin.apply(project);
    try {
      project.tasks.create(name: "hadoopValidate", group: "Hadoop Plugin",
          description: "Applies several checks - Syntax checking, dependency validation, data validation") {
        System.setProperty("java.security.krb5.conf", "krb5.conf");
      }
      .dependsOn(project.getTasks().getByName('pigValidate'));
    } catch (UnknownTaskException e) {
      println e.getMessage();
    }
  }

  /**
   * Factory method to return the PigValidatorPlugin class. Subclasses can override this method to return
   * their own PigValidatorPlugin class.
   *
   * @return Class that implements the PigValidatorPlugin
   */
  Class<? extends PigValidatorPlugin> getPigValidatorPlugin() {
    return PigValidatorPlugin.class;
  }

  /**
   * Reads the properties file containing information to configure the plugin.
   */
  void readValidatorProperties() {
    File file = new File("${project.projectDir}/.hadoopValidatorProperties");
    if (!file.exists()) {
      writeValidatorProperties();
    }
    properties = new Properties();
    file.withInputStream { inputStream -> properties.load(inputStream); }
  }

  /**
   * Set Hadoop Validator Properties
   *
   * @param properties Hadoop Validator properties
   */
  void setValidatorProperties(Properties properties) {
    properties.setProperty(ValidatorConstants.NAME_NODE, "");
    properties.setProperty(ValidatorConstants.REPOSITORY_URL, "");
  }

  /**
   * Sets and writes the Hadoop Validator properties to .hadoopValidatorProperties
   */
  void writeValidatorProperties() {
    File file = new File("${project.projectDir}/.hadoopValidatorProperties");
    try {
      file.withWriter { writer ->
        Properties properties = new Properties();
        setValidatorProperties(properties);
        properties.store(writer, null);
      }

      // Make the file readable only by the user. The Java File API makes this a little awkward to express.
      file.setReadable(false, false);
      file.setReadable(true, true);
    }
    catch (IOException ex) {
      project.logger.error("Unable to store session ID to file: " + file.toString() + "\n" + ex.toString());
    }
  }
}
