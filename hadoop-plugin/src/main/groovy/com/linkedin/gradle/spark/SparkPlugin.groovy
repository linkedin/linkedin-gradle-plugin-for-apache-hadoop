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
package com.linkedin.gradle.spark;

import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;

import org.gradle.api.GradleException;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.Exec;

/**
 * SparkPlugin implements features for Apache Spark.
 */
class SparkPlugin implements Plugin<Project> {
  SparkExtension sparkExtension;
  Project project;

  /**
   * Applies the Spark Plugin, which sets up tasks for Apache Spark.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    // Enable users to skip the plugin
    if (project.hasProperty("disableSparkPlugin")) {
      println("SparkPlugin disabled");
      return;
    }

    this.sparkExtension = makeSparkExtension(project);
    this.project = project;

    project.extensions.add("spark", sparkExtension);
    readSparkProperties();

    // Add spark tasks
    addShowSparkJobsTask();
    addExecSparkJobsTask();
  }

  /**
   * Factory method to return the Spark extension. Can be overridden by subclasses.
   *
   * @param project The Gradle project
   * @return The SparkExtension object to use for the SparkPlugin
   */
  SparkExtension makeSparkExtension(Project project) {
    return new SparkExtension(project);
  }

  /**
   * Reads the properties file containing information to configure the plugin.
   */
  void readSparkProperties() {
    File file = new File("${project.projectDir}/.sparkProperties");
    if (file.exists()) {
      Properties properties = new Properties();
      file.withInputStream { inputStream ->
        properties.load(inputStream);
      }
      // Now read the properties into the extension and validate them.
      sparkExtension.readFromProperties(properties);
      sparkExtension.validateProperties();
    }
  }

  /**
   * Adds tasks to display the Spark jobs specified by the user in the Hadoop DSL and a task that
   * can execute these jobs.
   */
  void addShowSparkJobsTask() {
    project.tasks.create("showSparkJobs") {
      description = "Lists Spark jobs configured in the Hadoop DSL that can be run with the runSparkJob task";
      group = "Hadoop Plugin";

      doLast {
        Map<SparkJob, NamedScope> jobScopeMap = SparkTaskHelper.findConfiguredSparkJobs(project);
        if (jobScopeMap.isEmpty()) {
          logger.lifecycle("The project ${project.name} does not have any Spark jobs configured with the Hadoop DSL.");
          return;
        }

        logger.lifecycle("The following Spark jobs in the project ${project.name} are configured in the Hadoop DSL and can be run with gradle runSparkJob -PjobName=<job name>:");

        jobScopeMap.each { SparkJob job, NamedScope parentScope ->
          Map<String, String> allProperties = job.buildProperties(parentScope);
          String jobName = job.getQualifiedName(parentScope);

          logger.lifecycle("\n----------");
          logger.lifecycle("Job name: ${jobName}");

          allProperties.each { key, value ->
            logger.lifecycle("${key}=${value}");
          }
        }
      }
    }
  }

  /**
   * Adds a task to run a Spark job configured in the Hadoop DSL.
   */
  void addExecSparkJobsTask() {
    project.tasks.create(name: "runSparkJob", type: Exec) {
      description = "Runs a Spark job configured in the Hadoop DSL with gradle runSparkJob -PjobName=<job name> -PzipTaskName=<zip task name>. Uses the Spark parameters and JVM properties from the DSL."
      group = "Hadoop Plugin";

      doFirst {
        if (!project.hasProperty("jobName")) {
          throw new GradleException("You must use -PjobName=<job name> to specify the job name with runSparkJob");
        }

        if (!project.hasProperty("zipTaskName")) {
          throw new GradleException("You must use -PzipTaskName=<zip task name> to specify the zipTask")
        }

        // write the zipTaskName
        def zipTaskName = project.zipTaskName;

        // create the cache directory on local system.
        makeCacheDir(zipTaskName);

        Map<SparkJob, NamedScope> jobScopeMap = SparkTaskHelper.findConfiguredSparkJobs(project);
        SparkJob sparkJob = null;
        NamedScope parentScope = null;

        for (Map.Entry<SparkJob, NamedScope> entry : jobScopeMap.entrySet()) {
          SparkJob job = entry.key;
          NamedScope scope = entry.value;
          if (job.getQualifiedName(scope).equals(project.jobName)) {
            sparkJob = job;
            parentScope = scope;
            break;
          }
        }

        if (sparkJob == null) {
          throw new GradleException("Could not find Spark job with name ${project.jobName} configured in the project ${project.name}. Please check the job name and run the task from within the module directory in which your jobs are configured.");
        }

        if (sparkJob.appClass == null) {
          throw new GradleException("Spark job with name ${sparkJob.name} does not have 'uses' set");
        }

        if (sparkJob.executionTarget == null) {
          throw new GradleException("Spark job with name ${sparkJob.name} does not have 'executes' set");
        }

        def scriptName = project.jobName;
        writeSparkExecJar(scriptName, sparkJob.executionTarget, sparkJob.appClass, sparkJob.sparkConfs, sparkJob.flags, sparkJob.appParams, sparkJob.jobProperties);

        String projectDir = "${sparkExtension.sparkCacheDir}/${project.name}";
        commandLine "bash", "${projectDir}/run_${project.jobName}.sh"
      }
    }
  }

  /**
   * Writes out the shell script that will run Spark for the given script and parameters.
   *
   * @param filePath The absolute path to the Spark script file
   * @param scriptName The name of the corresponding Gradle task for this execution of the script
   * @param parameters The Spark parameters
   * @param jvmProperties The JVM properties
   * @param confProperties The Hadoop Configuration properties
   */
  void writeSparkExecJar(String scriptName, String executionTarget, String appClass, Map<String, Object> confs, Set<String> flags, List<String> appParams, Map<String, Object> properties) {
    String projectDir = "${sparkExtension.sparkCacheDir}/${project.name}";
    String sparkCommand = sparkExtension.sparkCommand;

    if (sparkExtension.remoteHostName) {
      String remoteHostName = sparkExtension.remoteHostName;
      String remoteCacheDir = sparkExtension.remoteCacheDir;
      String remoteProjDir = "${remoteCacheDir}/${project.name}";

      new File("${projectDir}/run_${scriptName}.sh").withWriter { out ->
        out.writeLine("#!/usr/bin/env bash");
        out.writeLine("echo ====================");
        out.writeLine("echo Running the script ${projectDir}/run_${scriptName}.sh");
        out.writeLine("echo Creating directory ${remoteCacheDir} on host ${remoteHostName}");
        out.writeLine(buildRemoteMkdirCmd());
        out.writeLine("echo Syncing local directory ${projectDir} to ${remoteCacheDir} on host ${remoteHostName}");
        out.writeLine(buildRemoteRsyncCmd());
        out.writeLine("echo Executing ${sparkCommand} on host ${remoteHostName}");
        out.writeLine(buildRemoteSparkCmd(executionTarget, appClass, confs, flags, appParams, properties));
      }
    } else {
      new File("${projectDir}/run_${scriptName}.sh").withWriter { out ->
        out.writeLine("#!/usr/bin/env bash");
        out.writeLine("echo ====================");
        out.writeLine("echo Running the script ${projectDir}/run_${scriptName}.sh");
        out.writeLine("echo Executing ${sparkCommand} on the local host");
        out.writeLine(buildLocalSparkCmd(executionTarget, appClass, confs, flags, appParams, properties));
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
    String remoteHostName = sparkExtension.remoteHostName;
    String remoteSshOpts = sparkExtension.remoteSshOpts;
    String remoteCacheDir = sparkExtension.remoteCacheDir;
    return "ssh ${remoteSshOpts} ${remoteHostName} mkdir -p ${remoteCacheDir}";
  }

  /**
   * Method to build the command to rsync the project directory to the remote host's cache
   * directory. Subclasses can override this method to provide their own command.
   *
   * @return Command that rsyncs the project directory to the remote host's cache directory.
   */
  String buildRemoteRsyncCmd() {
    String projectDir = "${sparkExtension.sparkCacheDir}/${project.name}";
    String remoteHostName = sparkExtension.remoteHostName;
    String remoteSshOpts = sparkExtension.remoteSshOpts;
    String remoteCacheDir = sparkExtension.remoteCacheDir;
    return "rsync -av ${projectDir} -e \"ssh ${remoteSshOpts}\" ${remoteHostName}:${remoteCacheDir}";
  }

  /**
   * Method to build the command that invokes Spark on the remote host. Subclasses can override
   * this method to provide their own command.
   *
   * @param relFilePath The relative path to the Spark script file
   * @param parameters The Spark parameters
   * @param jvmProperties The JVM properties
   * @param confProperties The Hadoop Configuration properties
   * @return Command that invokes Spark on the remote host
   */
  String buildRemoteSparkCmd(String executionTarget, String appClass, Map<String, Object> confs, Set<String> flags, List<String> appParams, Map<String, Object> properties) {
    String sparkCommand = sparkExtension.sparkCommand;
    String sparkOptions = SparkSubmitHelper.buildSparkOptions(properties);
    String sparkConfs = SparkSubmitHelper.buildSparkConfs(confs);
    String sparkFlags = SparkSubmitHelper.buildSparkFlags(flags);
    String sparkAppParameters = SparkSubmitHelper.buildSparkAppParams(appParams);
    String sparkAppClass = SparkSubmitHelper.buildSparkClass(appClass);
    String remoteHostName = sparkExtension.remoteHostName;
    String remoteSshOpts = sparkExtension.remoteSshOpts;
    String remoteCacheDir = sparkExtension.remoteCacheDir;
    String remoteProjDir = "${remoteCacheDir}/${project.name}";
    return "ssh ${remoteSshOpts} -tt ${remoteHostName} 'cd ${remoteProjDir}; ${sparkCommand} ${sparkOptions} ${sparkConfs} ${sparkFlags} ${sparkAppClass} ${executionTarget} ${sparkAppParameters}'";
  }

  /**
   * Method to build command that should invoke spark on the local system
   *
   * @param executionTarget The application jar to run
   * @param appClass The application's main class
   * @param confs The spark configurations to be passed to spark-submit
   * @param flags The spark flags which should be passed to spark-submit
   * @param appParams application parameters
   * @param properties other spark options such as "master, jars, etc"
   * @return Command that invokes spark on local system.
   */
  String buildLocalSparkCmd(String executionTarget, String appClass, Map<String, Object> confs, Set<String> flags, List<String> appParams, Map<String, Object> properties) {
    String sparkCommand = sparkExtension.sparkCommand;
    String sparkOptions = SparkSubmitHelper.buildSparkOptions(properties);
    String sparkConfs = SparkSubmitHelper.buildSparkConfs(confs);
    String sparkFlags = SparkSubmitHelper.buildSparkFlags(flags);
    String sparkAppParameters = SparkSubmitHelper.buildSparkAppParams(appParams);
    String sparkAppClass = SparkSubmitHelper.buildSparkClass(appClass);
    String projectDir = "${sparkExtension.sparkCacheDir}/${project.name}";
    return "cd ${projectDir}; ${sparkCommand} $sparkOptions $sparkConfs $sparkFlags $sparkAppClass $executionTarget $sparkAppParameters";
  }

  /**
   * Builds the Cache directory on the local system specified by the sparkExtension.sparkCacheDir.
   */
  void makeCacheDir(String zipTaskName) {
    String zipFile = project.getProject().tasks[zipTaskName].archivePath.getAbsolutePath();
    String outputDir = "${sparkExtension.sparkCacheDir}/${project.name}";
    new AntBuilder().unzip(src: zipFile, dest: outputDir);
  }
}
