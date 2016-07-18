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
package com.linkedin.gradle.hadoopValidator.PigValidator


import com.linkedin.gradle.hadoopValidator.ValidatorPlugin
import com.linkedin.gradle.hadoopdsl.job.PigJob
import com.linkedin.gradle.hadoopdsl.NamedScope
import com.linkedin.gradle.pig.PigTaskHelper
import com.linkedin.gradle.zip.HadoopZipExtension
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.CopySpec
import org.gradle.api.file.FileTree
import org.gradle.api.internal.file.copy.CopySpecInternal


/**
 * PigValidatorPlugin is the class that provides a Gradle Plugin which checks script files of Apache Pig,
 * for Syntax errors, Data access errors and dependency errors.
 */
class PigValidatorPlugin implements ValidatorPlugin {
  Project project
  Map<PigJob, NamedScope> jobScopeMap
  Properties properties
  String libPath

  /**
   * Applies the Pig Validator Plugin, which provides task for checking syntax,data access dependencies and other jar dependencies.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {

    this.project = project

    createSyntaxChecker()
    createDataValidator()
    createDependencyValidator()

    project.tasks.create(name: "PigValidate", group: "Hadoop Plugin",
        description: "Applies Syntax checking, data validation, dependency validation for pig scripts")
        .dependsOn(project.getTasks().getByName('pigSyntax'))
        .dependsOn(project.getTasks().getByName('pigData'))
        .dependsOn(project.getTasks().getByName('pigDependency'))

    project.getTasks().getByName('pigData').dependsOn('pigSyntax')
    project.getTasks().getByName('pigDependency').dependsOn('pigSyntax')

  }


  /**
   * Creates and returns syntax checker task named 'pigSyntax'
   */
  Task createSyntaxChecker() {
    return createValidator("pigSyntax",\
    "checks syntax for all configured pig job scripts in the project",\
    getSyntaxValidatorClass()).doFirst {
      jobScopeMap = PigTaskHelper.findConfiguredPigJobs(project);
      if (jobScopeMap.isEmpty()) {
        throw  new GradleException("The project ${project.name} does not have any Pig jobs configured with the Hadoop DSL.");
      }
      checkJobScopeMap()
      jobMap = jobScopeMap
    }
  }

  /**
   * Creates and returns data validator task named 'pigData'
   */
  Task createDataValidator() {
    return createValidator("pigData",\
    "checks for existence of files loaded by all configured pig job scripts in the project",\
    getDataValidatorClass()) << {
      if(error){
        project.logger.error("Data validator found following errors:")
        err_paths.each{tuple ->
          if(tuple.size()==3)
            project.logger.error("path ${tuple[1]} in file ${tuple[0]} at line<${tuple[2]}> doesnot exist")
          else
            project.logger.error("path ${tuple[1]} in file ${tuple[0]} at line<${tuple[2]}> ${tuple[3]}")
        }
        throw new GradleException("Data Validator found errors")
      }
    }
  }

  /**
   * Creates and returns dependency validator task named 'pigData'
   */
  Task createDependencyValidator() {
    return createValidator('pigDependency',\
     "checks for existence of dependencies(jars) loaded by all configured pig job scripts in the project", \
      getDependencyValidatorClass()) .doFirst {
      createListZipFiles()
      zipContents = createListZipFiles()
      libpath = libPath
    } << {
      if(error){
        project.logger.error("Dependency validator found following errors:")
        err_paths.each{tuple ->
            project.logger.error("In file ${tuple[0]}, ${tuple[1]}")
        }
        throw new GradleException("Dependency Validator found errors")
      }
    }
  }


  private  <T extends DefaultTask & PigValidator> Task createValidator(String name,String description, Class<T> validator){
    return this.project.tasks.create(name: name,group: "Hadoop Plugin", type: validator ,description: description).doFirst {
      jobMap = this.jobScopeMap
      properties = this.properties
    }
  }

  /**
   * Checks the jobScopeMap field of the class to find valid pig jobs
   */
  void checkJobScopeMap(){
    jobScopeMap.each{PigJob pigJob, NamedScope parentScope ->
      if (pigJob.script == null) {
        throw new GradleException("Pig job with name ${pigJob.name} does not have a script set");
      }

      File file = new File(pigJob.script);
      if (!file.exists()) {
        throw new GradleException("Script ${pigJob.script} for Pig job with name ${pigJob.name} does not exist");
      }
    }
  }

  /**
   * Calls the getCopySpec method on each hadoopZip copyspec configured using hadoopDSL.
   * The actual population is done by getCopySpecFiles
   * @return the list of source files of all configured CopySpecs in the project
   */
  ArrayList<File> createListZipFiles() {
    ArrayList<File> zipContents = new ArrayList<File>()
    HadoopZipExtension hadoopZipExtension = project.extensions.getByName("hadoopZip") //createZipExtension();
    CopySpec base = hadoopZipExtension.getBaseCopySpec()
    libPath = hadoopZipExtension.libPath

    zipContents.addAll(getCopySpecFiles(base))

    Map<String, CopySpec> zipMap = hadoopZipExtension.getZipMap()

    zipMap.each { String zipName, CopySpec copySpec ->
      zipContents.addAll(getCopySpecFiles(copySpec))
    }
    return zipContents
  }

  /**
   * Populates object field zipContents with the files which will go into the hadoop Zip
   *
   * @param cpy A hadoopZip CopySpec object
   * @return the list of source files of the input CopySpec
   */
  ArrayList<File> getCopySpecFiles(CopySpec cpy){
    ArrayList<File> zipContents = new ArrayList<File>()
    FileTree fileTree = (cpy as CopySpecInternal).buildRootResolver().getAllSource()
    fileTree.each {
      zipContents.add(it)
    }
    return zipContents
  }

  boolean isSet(def param) {
    if (param.equals(null)) {
      return false
    } else if (param.isEmpty()) {
      return false
    }
    return true
  }


  /**
   * Factory method to return the PigSyntaxValidator Task class. Subclasses can override this method to return
   * their own PigSyntaxValidator Task class.
   *
   * @return Class that implements the PigSyntaxValidator
   */
  Class<? extends PigSyntaxValidator> getSyntaxValidatorClass(){
    return PigSyntaxValidator.class
  }

  /**
   * Factory method to return the PigDataValidator Task class. Subclasses can override this method to return
   * their own PigDataValidator Task class.
   *
   * @return Class that implements the PigDataValidator Task
   */
  Class<? extends PigDataValidator> getDataValidatorClass(){
    return PigDataValidator.class
  }

  /**
   * Factory method to return the PigDependencyValidator Task class. Subclasses can override this method to return
   * their own PigDependencyValidator Task class.
   *
   * @return Class that implements the PigDependencyValidator Task
   */
  Class<? extends PigDependencyValidator> getDependencyValidatorClass(){
    return PigDependencyValidator.class
  }
}
