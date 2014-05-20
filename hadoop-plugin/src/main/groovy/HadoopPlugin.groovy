import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFileFilter;
import org.apache.commons.io.filefilter.DirectoryFileFilter;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.Exec;

/**
 * HadoopPlugin is the class that implements our Gradle Plugin.
 */
class HadoopPlugin implements Plugin<Project> {

  Project project;

  AzkabanWorkflow workflow(String name, Closure configure) {
    println "HadoopPlugin workflow: " + name
    AzkabanWorkflow flow = new AzkabanWorkflow(name, project);
    project.configure(flow, configure);
    // workflows.add(flow);
    return flow;
  }

  void apply(Project project) {
    // Add the extension object that exposes the DSL to users.
    AzkabanExtension azkabanExtension = new AzkabanExtension(project);
    project.extensions.add("azkaban", azkabanExtension);
    project.extensions.add("workflow", this.&workflow);

    // Add the Gradle task that checks and evaluates the DSL. Plugin users
    // should have their build tasks depend on this task.
    project.tasks.add("buildAzkabanFlow") << {
      AzkabanLintChecker checker = new AzkabanLintChecker();
      if (!checker.checkAzkabanExtension(project.extensions.azkaban)) {
        throw new Exception("AzkabanLintChecker FAILED");
      }

      System.out.println("AzkabanLintChecker PASSED");
      project.extensions.azkaban.build();
    }

    // Add a task that sets up the cache directory we will copy to the host
    // that will execute our Pig scripts.
    project.tasks.add(name: "buildPigCache", type: Copy) {
      dependsOn project.tasks["jar"]
      from project.configurations['runtime']
      into "${project.buildDir}/pigCache"
    }

    // Add a task for each Pig script that runs the script. This task will
    // depend on the previous task that sets up the pigCache directory.
    generatePigTasks(project);
  }

  void generatePigTasks(Project project) {
    File sourceDir = new File("./src");
    Collection<File> files = FileUtils.listFiles(sourceDir, FileFileFilter.FILE, DirectoryFileFilter.DIRECTORY);

    for (File file : files) {
      String fileName = file.getName();

      // For each Pig script, add a task to the project that will run the script.
      // TODO uniquify task names
      if (fileName.toLowerCase().endsWith(".pig")) {

        project.tasks.add(name: "run_${fileName}", type: Exec) {
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
