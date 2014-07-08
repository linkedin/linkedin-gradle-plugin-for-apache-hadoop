package com.linkedin.gradle.hadoop;

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.Exec;
import org.gradle.api.tasks.Sync;

class PigTasks {

  static void generatePigTasks(Project project) {
    readPigProperties(project);

    if (project.extensions.pig.generateTasks) {
      addBuildCacheTask(project);
      addPigScriptTasks(project);
      addShowPigJobsTask(project);
      addRunPigJobTask(project);
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
  static void addBuildCacheTask(Project project) {
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

  static void addPigScriptTask(Project project, String filePath, String taskName, Map<String, String> parameters) {
    String relaPath = filePath.replace("${project.projectDir}/", "");
    String projectDir = "${project.extensions.pig.pigCacheDir}/${project.name}";

    project.tasks.create(name: "run_${taskName}", type: Exec) {
      commandLine "sh", "${projectDir}/run_${taskName}.sh"
      dependsOn project.tasks["buildPigCache"]
      description = "Run the Pig script ${relaPath}";
      group = "Hadoop Plugin";

      doFirst {
        writePigExecScript(project, filePath, taskName, parameters);
      }
    }
  }

  static void writePigExecScript(Project project, String filePath, String taskName, Map<String, String> parameters) {
    String relaPath = filePath.replace("${project.projectDir}/", "");
    String projectDir = "${project.extensions.pig.pigCacheDir}/${project.name}";

    String pigCommand = project.extensions.pig.pigCommand;
    String pigOptions = project.extensions.pig.pigOptions ?: "";
    String pigParams = buildPigParameters(parameters);

    if (project.extensions.pig.remoteHostName) {
      String remoteHostName = project.extensions.pig.remoteHostName;
      String remoteShellCmd = project.extensions.pig.remoteShellCmd;
      String remoteCacheDir = project.extensions.pig.remoteCacheDir;
      String remoteProjDir = "${remoteCacheDir}/${project.name}";

      new File("${projectDir}/run_${taskName}.sh").withWriter { out ->
        out.writeLine("#!/bin/sh");
        out.writeLine("echo ====================");
        out.writeLine("echo Running the script ${projectDir}/run_${taskName}.sh");
        out.writeLine("echo Creating directory ${remoteCacheDir} on host ${remoteHostName}");
        out.writeLine("${remoteShellCmd} ${remoteHostName} mkdir -p ${remoteCacheDir}");
        out.writeLine("echo Syncing local directory ${projectDir} to ${remoteHostName}:${remoteCacheDir}");
        out.writeLine("rsync -av ${projectDir} -e \"${remoteShellCmd}\" ${remoteHostName}:${remoteCacheDir}");
        out.writeLine("echo Executing ${pigCommand} on host ${remoteHostName}");
        out.writeLine("${remoteShellCmd} ${remoteHostName} ${pigCommand} -Dpig.additional.jars=${remoteProjDir}/*.jar ${pigOptions} -f ${remoteProjDir}/${relaPath} ${pigParams}");
      }
    }
    else {
      new File("${projectDir}/run_${taskName}.sh").withWriter { out ->
        out.writeLine("#!/bin/sh");
        out.writeLine("echo ====================");
        out.writeLine("echo Running the script ${projectDir}/run_${taskName}.sh");
        out.writeLine("echo Executing ${pigCommand} on the local host");
        out.writeLine("${pigCommand} -Dpig.additional.jars=${projectDir}/*.jar ${pigOptions} -f ${projectDir}/${relaPath} ${pigParams}");
      }
    }
  }

  static String buildPigParameters(Map<String, String> parameters) {
    StringBuilder sb = new StringBuilder();

    if (parameters.size() > 0) {
      sb.append("-param");
    }

    parameters.each { String key, String val ->
      sb.append(" ${key}=${val}");
    }

    return sb.toString();
  }

  // Adds tasks to display the Pig jobs specified by the user in the Azkaban DSL and a task that
  // can execute these jobs.
  static void addShowPigJobsTask(Project project) {
    project.tasks.create("showPigJobs") {
      description = "Lists Pig jobs configured in the Azkaban DSL that can be run with the runPigJob task";
      group = "Hadoop Plugin";

      doLast {
        Map<String, PigJob> pigJobs = findPigJobs(project);
        println("The following Pig jobs configured in the AzkabanDSL can be run with gradle runPigJob -PjobName=<job name>");

        pigJobs.keySet().each { String jobName ->
          println(jobName);
        }
      }
    }
  }

  static Map<String, PigJob> findPigJobs(Project project) {
    Map<String, PigJob> pigJobs = new LinkedHashMap<String, PigJob>();
    findPigJobs(project.extensions.globalScope, "", pigJobs);
    return pigJobs;
  }

  static void findPigJobs(NamedScope scope, String prefix, Map<String, PigJob> pigJobs) {
    scope.thisLevel.each { String name, Object val ->
      if (val instanceof PigJob) {
        PigJob pigJob = (PigJob)val;
        pigJobs.put(prefix + pigJob.name, pigJob);
      }
      else if (val instanceof AzkabanExtension) {
        AzkabanExtension azkaban = (AzkabanExtension)val;
        findPigJobs(azkaban.azkabanScope, "${prefix}${azkaban.azkabanScope.levelName}.", pigJobs);
      }
      else if (val instanceof AzkabanWorkflow) {
        AzkabanWorkflow workflow = (AzkabanWorkflow)val;
        findPigJobs(workflow.workflowScope, "${prefix}${workflow.workflowScope.levelName}.", pigJobs);
      }
    }
  }

  static void addRunPigJobTask(Project project) {
    project.tasks.create(name: "runPigJob", type: Exec) {
      dependsOn project.tasks["buildPigCache"]
      description = "Runs a Pig job configured in the Azkaban DSL with gradle runPigJob -PjobName=<job name>";
      group = "Hadoop Plugin";

      doFirst {
        if (!project.jobName) {
          throw new GradleException("You must use -PjobName=<job name> to specify the job name with runPigJob");
        }

        Map<String, PigJob> pigJobs = findPigJobs(project);
        PigJob pigJob = pigJobs.get(project.jobName);

        if (pigJob == null) {
          throw new GradleException("Could not find Pig job with name ${project.jobName}");
        }

        if (pigJob.script == null) {
          throw new GradleException("Pig job with name ${project.jobName} does not have a script set");
        }

        File file = new File(pigJob.script);
        if (!file.exists()) {
          throw new GradleException("Script ${pigJob.script} for Pig job with name ${project.jobName} does not exist");
        }

        String filePath = file.getAbsolutePath();
        String taskName = project.jobName;
        writePigExecScript(project, filePath, taskName, pigJob.parameters);

        String projectDir = "${project.extensions.pig.pigCacheDir}/${project.name}";
        commandLine "sh", "${projectDir}/run_${taskName}.sh"
      }
    }
  }
}