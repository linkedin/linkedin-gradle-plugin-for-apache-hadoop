package com.linkedin.gradle.hadoop;

import org.gradle.api.DefaultTask
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.Exec;
import org.gradle.api.tasks.Sync;

class PigTasks {

  static void generatePigTasks(Project project) {
    readPigProperties(project);

    if (project.extensions.pig.generateTasks) {
      addPigCacheTask(project);
      addPigScriptTasks(project);
    }
  }

  static void readPigProperties(Project project) {
    File file = new File("${project.projectDir}/.pigProperties");
    if (file.exists()) {
      Properties properties = new Properties();
      file.withInputStream { inputStream ->
        properties.load(inputStream);
      }

      // Now read the properties into the extension and validate them.
      project.extensions.pig.readFromProperties(properties);
      project.extensions.pig.validateProperties();
    }
  }

  // Adds a task to set up the cache directory that will be rsync'd to the host that will run Pig.
  static void addPigCacheTask(Project project) {
    project.tasks.create(name: "buildPigCache", type: Sync) {
      description = "Build the cache directory to run Pig scripts by Gradle tasks";
      group = "Hadoop Plugin";

      FileTree pigFileTree = project.fileTree([
        dir: "${project.projectDir}",
        include: "src/**/*.pig"
      ]);

      from pigFileTree;
      from project.configurations[project.extensions.pig.dependencyConf];
      into "${project.extensions.pig.pigCacheDir}/${project.name}";
      includeEmptyDirs = false;
    }
  }

  // For each Pig script, adds a task to run the script on a host.
  static void addPigScriptTasks(Project project) {
    Set<String> taskNames = new HashSet<String>();

    FileTree pigFileTree = project.fileTree([
      dir: "${project.projectDir}",
      include: "src/**/*.pig"
    ]);

    pigFileTree.each { File file ->
      String fileName = file.getName();
      String filePath = file.getAbsolutePath();

      String taskName = buildUniqueTaskName(fileName, taskNames);
      taskNames.add(taskName);
      addPigScriptTask(project, filePath, taskName, null);
    }
  }

  static void addPigScriptTask(Project project, String filePath, String taskName, Map<String, String> parameters) {
    String relaPath = filePath.replace("${project.projectDir}/", "");

    project.tasks.create(name: "run_${taskName}", type: Exec) {
      dependsOn project.tasks["buildPigCache"]
      description = "Run the Pig script ${relaPath}";
      group = "Hadoop Plugin";

      String projectDir = "${project.extensions.pig.pigCacheDir}/${project.name}";
      commandLine "sh", "${projectDir}/run_${taskName}.sh"

      doFirst {
        String pigCommand = project.extensions.pig.pigCommand;
        String pigOptions = project.extensions.pig.pigOptions ?: "";

        if (project.extensions.pig.remoteHostName) {
          String remoteHostName = project.extensions.pig.remoteHostName;
          String remoteShellCmd = project.extensions.pig.remoteShellCmd;
          String remoteCacheDir = project.extensions.pig.remoteCacheDir;
          String remoteProjDir = "${remoteCacheDir}/${project.name}";

          new File("${projectDir}/run_${taskName}.sh").withWriter { out ->
            out.writeLine("#!/bin/sh");
            out.writeLine("echo ==================");
            out.writeLine("echo Running the script ${projectDir}/run_${taskName}.sh");
            out.writeLine("echo Creating directory ${remoteCacheDir} on host ${remoteHostName}");
            out.writeLine("${remoteShellCmd} ${remoteHostName} mkdir -p ${remoteCacheDir}");
            out.writeLine("echo Syncing local directory ${projectDir} to ${remoteHostName}:${remoteCacheDir}");
            out.writeLine("rsync -av ${projectDir} -e \"${remoteShellCmd}\" ${remoteHostName}:${remoteCacheDir}");
            out.writeLine("echo Executing ${pigCommand} on host ${remoteHostName}");
            out.writeLine("${remoteShellCmd} ${remoteHostName} ${pigCommand} -Dpig.additional.jars=${remoteProjDir}/*.jar ${pigOptions} -f ${remoteProjDir}/${relaPath}");
          }
        }
        else {
          new File("${projectDir}/run_${taskName}.sh").withWriter { out ->
            out.writeLine("#!/bin/sh");
            out.writeLine("echo ==================");
            out.writeLine("echo Running the script ${projectDir}/run_${taskName}.sh");
            out.writeLine("echo Executing ${pigCommand} on the local host");
            out.writeLine("${pigCommand} -Dpig.additional.jars=${projectDir}/*.jar ${pigOptions} -f ${projectDir}/${relaPath}");
          }
        }
      }
    }
  }

  // Uniquify the task names that correspond to running Pig scripts on a host, since there may be
  // more than one Pig script with the same name recursively under ${project.projectDir}/src.
  static String buildUniqueTaskName(String fileName, Set<String> taskNames) {
    if (!taskNames.contains(fileName)) {
      return fileName;
    }

    char index = '1';
    StringBuilder sb = new StringBuilder(fileName + index);
    int length = sb.length();

    while (!taskNames.contains(sb.toString())) {
      sb.setCharAt(length, ++index);
    }

    return sb.toString();
  }
}