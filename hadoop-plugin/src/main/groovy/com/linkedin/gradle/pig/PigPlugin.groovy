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
package com.linkedin.gradle.pig;

import com.linkedin.gradle.hadoopdsl.PigJob;

import org.gradle.api.GradleException
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.Exec;
import org.gradle.api.tasks.Sync;

/**
 * PigPlugin implements features for Apache Pig.
 */
class PigPlugin implements Plugin<Project> {
  PigExtension pigExtension;
  Project project;

  /**
   * Applies the Pig Plugin, which sets up tasks for Apache Pig.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    this.pigExtension = makePigExtension(project);
    this.project = project;

    project.extensions.add("pig", pigExtension);
    readPigProperties();

    if (pigExtension.generateTasks) {
      addBuildCacheTask();
      addPigScriptTasks();
      addShowPigJobsTask();
      addExecPigJobsTask();
    }
  }

  /**
   * Factory method to return the Pig extension. Can be overridden by subclasses.
   *
   * @param project The Gradle project
   * @return The PigExtension object to use for the PigPlugin
   */
  PigExtension makePigExtension(Project project) {
    return new PigExtension(project);
  }

  /**
   * Reads the properties file containing information to configure the plugin.
   */
  void readPigProperties() {
    File file = new File("${project.projectDir}/.pigProperties");
    if (file.exists()) {
      Properties properties = new Properties();
      file.withInputStream { inputStream ->
        properties.load(inputStream);
      }
      // Now read the properties into the extension and validate them.
      pigExtension.readFromProperties(properties);
      pigExtension.validateProperties();
    }
  }

  /**
   * Adds a task to set up the cache directory that will be rsync'd to the host that will run Pig.
   */
  void addBuildCacheTask() {
    project.tasks.create(name: "buildPigCache", type: Sync) {
      description = "Build the cache directory to run Pig scripts by Gradle tasks. This task will be run automatically for you.";
      group = "Hadoop Plugin";

      if (pigExtension.dependencyConf != null) {
        from project.configurations[pigExtension.dependencyConf];
      }

      FileTree pigFileTree = project.fileTree([
        dir: "${project.projectDir}",
        includes: ["src/**/*", "resources/**"],
        exclude: "src/test"
      ]);

      from pigFileTree;
      into "${pigExtension.pigCacheDir}/${project.name}";
      includeEmptyDirs = false;

      doFirst {
        if (pigExtension.dependencyConf == null) {
          logger.lifecycle("Pig Plugin dependencyConf property is not set. No configuration dependencies will be added to the Pig cache directory");
          logger.lifecycle("To runtime jars or other resources when you run a Pig script with the Pig Plugin, set this property in your .pigProperties file");
        }
      }
    }
  }

  /**
   * For each Pig script, adds a task to run the script on a host. With these tasks, you cannot
   * pass any Java or Pig parameters to the script.
   */
  void addPigScriptTasks() {
    FileTree pigFileTree = project.fileTree([
      dir: "${project.projectDir}",
      include: "src/**/*.pig"
    ]);

    String projectDir = "${pigExtension.pigCacheDir}/${project.name}";
    Set<String> taskNames = new HashSet<String>();

    pigFileTree.each { File file ->
      String fileName = file.getName();
      String filePath = file.getAbsolutePath();
      String relaPath = filePath.replace("${project.projectDir}/", "");
      String taskName = PigTaskHelper.buildUniqueTaskName(fileName, taskNames);
      taskNames.add(taskName);

      project.tasks.create(name: "run_${taskName}", type: Exec) {
        commandLine "sh", "${projectDir}/run_${taskName}.sh"
        dependsOn project.tasks["buildPigCache"]
        description = "Run the Pig script ${relaPath} with no Pig parameters or JVM properties";
        group = "Hadoop Plugin";

        doFirst {
          writePigExecScript(filePath, taskName, null, null);
        }
      }
    }
  }

  /**
   * Adds tasks to display the Pig jobs specified by the user in the Hadoop DSL and a task that
   * can execute these jobs.
   */
  void addShowPigJobsTask() {
    project.tasks.create("showPigJobs") {
      description = "Lists Pig jobs configured in the Hadoop DSL that can be run with the runPigJob task";
      group = "Hadoop Plugin";

      doLast {
        Map<String, PigJob> pigJobs = PigTaskHelper.findConfiguredPigJobs(project);
        if (pigJobs.isEmpty()) {
          logger.lifecycle("The project ${project.name} does not have any Pig jobs configured with the Hadoop DSL.");
          return;
        }

        logger.lifecycle("The following Pig jobs in the project ${project.name} are configured in the Hadoop DSL and can be run with gradle runPigJob -Pjob=<job name>:");

        pigJobs.each { String jobName, PigJob job ->
          Map<String, String> allProperties = job.buildProperties(new LinkedHashMap<String, String>());
          logger.lifecycle("\n----------");
          logger.lifecycle("Job name: ${jobName}");
          allProperties.each() { key, value ->
            logger.lifecycle("${key}=${value}");
          }
        }
      }
    }
  }

  /**
   * Adds a task to run a Pig job configured in the Hadoop DSL. This enables the user to pass
   * parameters to the script.
   */
  void addExecPigJobsTask() {
    project.tasks.create(name: "runPigJob", type: Exec) {
      dependsOn project.tasks["buildPigCache"]
      description = "Runs a Pig job configured in the Hadoop DSL with gradle runPigJob -Pjob=<job name>. Uses the Pig parameters and JVM properties from the DSL.";
      group = "Hadoop Plugin";

      doFirst {
        if (!project.job) {
          throw new GradleException("You must use -Pjob=<job name> to specify the job name with runPigJob");
        }

        Map<String, PigJob> pigJobs = PigTaskHelper.findConfiguredPigJobs(project);
        PigJob pigJob = pigJobs.get(project.job);

        if (pigJob == null) {
          throw new GradleException("Could not find Pig job with name ${project.job} configured in the project ${project.name}. Please check the job name and run the task from within the module directory in which your jobs are configured.");
        }

        if (pigJob.script == null) {
          throw new GradleException("Pig job with name ${pigJob.name} does not have a script set");
        }

        File file = new File(pigJob.script);
        if (!file.exists()) {
          throw new GradleException("Script ${pigJob.script} for Pig job with name ${pigJob.name} does not exist");
        }

        String filePath = file.getAbsolutePath();
        writePigExecScript(filePath, project.job, pigJob.parameters, pigJob.jvmProperties);

        String projectDir = "${pigExtension.pigCacheDir}/${project.name}";
        commandLine "bash", "${projectDir}/run_${project.job}.sh"
      }
    }
  }

  /**
   * Writes out the shell script that will run Pig for the given script and parameters.
   *
   * @param filePath The absolute path to the Pig script file
   * @param taskName The name of the corresponding Gradle task for this execution of the script
   * @param parameters The Pig parameters
   * @param jvmProperties The JVM properties
   */
  void writePigExecScript(String filePath, String taskName, Map<String, String> parameters, Map<String, String> jvmProperties) {
    String relFilePath = filePath.replace("${project.projectDir}/", "");
    String projectDir = "${pigExtension.pigCacheDir}/${project.name}";
    String pigCommand = pigExtension.pigCommand;

    if (pigExtension.remoteHostName) {
      String remoteHostName = pigExtension.remoteHostName;
      String remoteCacheDir = pigExtension.remoteCacheDir;
      String remoteProjDir = "${remoteCacheDir}/${project.name}";

      new File("${projectDir}/run_${taskName}.sh").withWriter { out ->
        out.writeLine("#!/usr/bin/env bash");
        out.writeLine("echo ====================");
        out.writeLine("echo Running the script ${projectDir}/run_${taskName}.sh");
        out.writeLine("echo Creating directory ${remoteCacheDir} on host ${remoteHostName}");
        out.writeLine(buildRemoteMkdirCmd());
        out.writeLine("echo Syncing local directory ${projectDir} to ${remoteCacheDir} on host ${remoteHostName}");
        out.writeLine(buildRemoteRsyncCmd());
        out.writeLine("echo Executing ${pigCommand} on host ${remoteHostName}");
        out.writeLine(buildRemotePigCmd(relFilePath, parameters, jvmProperties));
      }
    }
    else {
      new File("${projectDir}/run_${taskName}.sh").withWriter { out ->
        out.writeLine("#!/usr/bin/env bash");
        out.writeLine("echo ====================");
        out.writeLine("echo Running the script ${projectDir}/run_${taskName}.sh");
        out.writeLine("echo Executing ${pigCommand} on the local host");
        out.writeLine(buildLocalPigCmd(relFilePath, parameters, jvmProperties));
      }
    }
  }

  /**
   * Method to build the command to create the cache directory on the remote host. Subclasses can
   * override this method to provide their own command.
   *
   * @return Command that creates the cache directory on the remote host
   */
  String buildRemoteMkdirCmd() {
    String remoteHostName = pigExtension.remoteHostName;
    String remoteSshOpts = pigExtension.remoteSshOpts;
    String remoteCacheDir = pigExtension.remoteCacheDir;
    return "ssh ${remoteSshOpts} ${remoteHostName} mkdir -p ${remoteCacheDir}";
  }

  /**
   * Method to build the command to rsync the project directory to the remote host's cache
   * directory. Subclasses can override this method to provide their own command.
   *
   * @return Command that rsyncs the project directory to the remote host's cache directory.
   */
  String buildRemoteRsyncCmd() {
    String projectDir = "${pigExtension.pigCacheDir}/${project.name}";
    String remoteHostName = pigExtension.remoteHostName;
    String remoteSshOpts = pigExtension.remoteSshOpts;
    String remoteCacheDir = pigExtension.remoteCacheDir;
    return "rsync -av ${projectDir} -e \"ssh ${remoteSshOpts}\" ${remoteHostName}:${remoteCacheDir}";
  }

  /**
   * Method to build the command that invokes Pig on the remote host. Subclasses can override
   * this method to provide their own command.
   *
   * @param relFilePath The relative path to the Pig script file
   * @param parameters The Pig parameters
   * @param jvmProperties The JVM properties
   * @return Command that invokes Pig on the remote host
   */
  String buildRemotePigCmd(String relFilePath, Map<String, String> parameters, Map<String, String> jvmProperties) {
    String pigCommand = pigExtension.pigCommand;
    String pigOptions = pigExtension.pigOptions ?: "";
    String pigParams = parameters == null ? "" : PigTaskHelper.buildPigParameters(parameters);
    String jvmParams = jvmProperties == null ? "" : PigTaskHelper.buildJvmParameters(jvmProperties);
    String remoteHostName = pigExtension.remoteHostName;
    String remoteSshOpts = pigExtension.remoteSshOpts;
    String remoteCacheDir = pigExtension.remoteCacheDir;
    String remoteProjDir = "${remoteCacheDir}/${project.name}";
    return "ssh ${remoteSshOpts} -tt ${remoteHostName} 'cd ${remoteProjDir}; ${pigCommand} -Dpig.additional.jars=${remoteProjDir}/*.jar ${jvmParams} ${pigOptions} -f ${relFilePath} ${pigParams}'";
  }

  /**
   * Method to build the command that invokes Pig on the local host. Subclasses can override
   * this method to provide their own command.
   *
   * @param relFilePath The relative path to the Pig script file
   * @param parameters The Pig parameters
   * @param jvmProperties The JVM properties
   * @return Command that invokes Pig on the local host
   */
  String buildLocalPigCmd(String relFilePath, Map<String, String> parameters, Map<String, String> jvmProperties) {
    String projectDir = "${pigExtension.pigCacheDir}/${project.name}";
    String pigCommand = pigExtension.pigCommand;
    String pigOptions = pigExtension.pigOptions ?: "";
    String pigParams = parameters == null ? "" : PigTaskHelper.buildPigParameters(parameters);
    String jvmParams = jvmProperties == null ? "" : PigTaskHelper.buildJvmParameters(jvmProperties);
    return "cd ${projectDir}; ${pigCommand} -Dpig.additional.jars=${projectDir}/*.jar ${jvmParams} ${pigOptions} -f ${relFilePath} ${pigParams}";
  }
}