package com.linkedin.gradle.hadoop;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.Exec;

class PigTasks {
  static void generatePigTasks(Project project) {
    // Add a task that sets up the cache directory we will copy to the host
    // that will execute our Pig scripts.
    project.tasks.create(name: "buildPigCache", type: Copy) {
      dependsOn project.tasks["jar"]
      from project.configurations['runtime']
      into "${project.buildDir}/pigCache"
    }

    File sourceDir = new File("${project.projectDir}/src");
    Collection<File> files = FileUtils.listFiles(sourceDir, FileFileFilter.FILE, DirectoryFileFilter.DIRECTORY);

    for (File file : files) {
      String fileName = file.getName();

      // For each Pig script, add a task to the project that will run the script.
      // TODO uniquify task names
      if (fileName.toLowerCase().endsWith(".pig")) {

        project.tasks.create(name: "run_${fileName}", type: Exec) {
          dependsOn project.tasks["buildPigCache"]
          description = "Run this Pig script";
          group = "Hadoop Plugin";

          String pigHost = "eat1-magicgw01.grid.linkedin.com";
          String ssh = "/usr/bin/ssh -K ${pigHost}";
          String pigCache = "./.pigCache";
          String mkdir = "${ssh} \"mkdir -p ${pigCache}\"";
          String rsync = "rsync -av ${project.buildDir}/pigCache -e \"ssh -K\" ${pigHost}:${pigCache}";
          String pig = "magic-pig ${fileName}"

          // println "Will execute Pig script ${fileName} on host ${pigHost}";
          // println "Running mkdir command: ${mkdir}"
          commandLine mkdir

          // println "Running rsync command: ${rsync}"
          // println "Running Pig: ${pig}"
        }
      }
    }
  }
}