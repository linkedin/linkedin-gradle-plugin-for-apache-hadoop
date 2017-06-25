package com.linkedin.gradle.hadoopdsl

import org.gradle.api.DefaultTask
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.file.FileTree
import org.gradle.api.tasks.TaskAction

/**
 * <p>
 * Task that sets up Hadoop DSL automatic builds. This task creates subtasks that rebuild the
 * Hadoop DSL for each definition set file found in a specified path. In each subtask, the
 * definition set file is applied, followed by the user profile and the user's workflow scripts.
 * The output build path for the compiled Hadoop DSL files is adjusted so that on each build, the
 * output will be written to a different subdirectory of the original build path.
 * <p>
 * This task can be specified with:
 * <pre>
 *   autoAzkabanFlows {
 *     defaultBuildPath = "azkaban"  // Optional - defaults to "azkaban". Default location to build the Hadoop DSL if it is not already specified.
 *     showSetup = false  // Optional - defaults to false. Displays information about the automatic setup.
 *     definitions = 'src/main/definitions'  // Optional - defaults to 'src/main/definitions'. Path to the Hadoop DSL definition sets.
 *     profiles = 'src/main/profiles'  // Optional - defaults to 'src/main/profiles'. Set to null or pass -PskipProfile=true to disable profiles. Path to the Hadoop DSL user profile scripts.
 *     workflows = 'src/main/gradle'  // Optional - defaults to 'src/main/gradle'. Path to the Hadoop DSL workflow scripts.
 *   }.addAutoBuildTasks()  // Invoke this method to add the sub-tasks that build the Hadoop DSL for each definition set
 * </pre>
 */
class HadoopDslAutoBuild extends DefaultTask {
  HadoopDslCompiler dslCompiler    // Will be set by class that creates this task
  HadoopDslExtension dslExtension  // Will be set in Hadoop Plugin setupTaskDependencies method
  HadoopDslPlugin dslPlugin        // Will be set in Hadoop Plugin setupTaskDependencies method

  boolean alreadySetup = false
  boolean showSetup = false

  String defaultBuildPath = "azkaban"
  String definitions = "src/main/definitions"
  String profiles = "src/main/profiles"
  String workflows = "src/main/gradle"

  List<File> definitionSetFiles
  List<File> profileFiles
  List<File> workflowFiles

  /**
   * Constructor for the HadoopDslAutoBuild task.
   */
  HadoopDslAutoBuild() { }

  /**
   * Task action for the HadoopDslAutoBuild task.
   */
  @TaskAction
  void autoBuild() {
    if (!alreadySetup) {
      throw new Exception("You must call `autoAzkabanFlows { ... }.addAutoBuildTasks()` before you can execute the task")
    }
  }

  /**
   * Uses the settings configured by the user to automatically setup the per-definition Hadoop DSL
   * build tasks for the project.
   *
   * @return The generated subtasks
   */
  List<Task> addAutoBuildTasks() {
    if (alreadySetup) {
      throw new Exception("HadoopDslBuild addBuildTasks was called more than once")
    }

    alreadySetup = true

    if (dslCompiler == null) {
      throw new Exception("No Hadoop DSL compiler has been configured for the HadoopDslBuild class")
    }

    // Now add the auto build tasks
    definitionSetFiles = getMatchingFiles(definitions, '**/*.gradle').sort()
    profileFiles = getMatchingFiles(profiles, '**/*.gradle').sort()
    workflowFiles = getMatchingFiles(workflows, '**/*.gradle').sort()

    // If the user has enabled verbose mode, display information about the files we found
    if (showSetup) {
      project.logger.lifecycle("\n[Hadoop DSL Auto] Setting up Hadoop DSL auto build tasks")

      showFilesFound(definitionSetFiles,
          "\n[Hadoop DSL Auto] Found the following Hadoop DSL definition files:",
          "\n[Hadoop DSL Auto] No Hadoop DSL definition files found in ${definitions}")

      showFilesFound(profileFiles,
          "\n[Hadoop DSL Auto] Found the following Hadoop DSL profile files:",
          "\n[Hadoop DSL Auto] No Hadoop DSL profile files found in ${profiles}")

      showFilesFound(workflowFiles,
          "\n[Hadoop DSL Auto] Found the following Hadoop DSL workflow files:",
          "\n[Hadoop DSL Auto] No Hadoop DSL workflow files found in ${workflows}")
    }

    // For each definition set, we will apply the definition set, the user profile script, and all
    // of the user's workflow scripts.
    Task previousTask = null
    List<Task> subTasks = new ArrayList<Task>()

    for (File definitionSetFile : definitionSetFiles) {
      String definitionSetName = definitionSetFile.getName().replace(".gradle", "")
      String definitionSetPath = definitionSetFile.getAbsolutePath().replace("${project.projectDir}/", "")

      List<String> workflowPaths = workflowFiles.collect { workflowFile ->
        String filePath = workflowFile.getAbsolutePath()
        return filePath.replace("${project.projectDir}/", "")  // Relative path to the script
      }

      previousTask = createAutoHadoopDslTask(definitionSetPath, workflowPaths, previousTask)
      subTasks.add(previousTask)
    }

    return subTasks
  }

  Task createAutoHadoopDslTask(String definitionSetPath, List<String> workflowPaths, Task previousTask) {
    String definitionSetName = new File(definitionSetPath).getName().replace(".gradle", "")

    if (showSetup) {
      String applyText = workflowPaths.collect { workflowPath -> "\tapply from: '${workflowPath}'" }.join("\n")
      String hadoopDslText =
            """\tapply from: '${definitionSetPath}'
              |\tapplyUserProfile()
              |
              |\t// Now apply the workflow files
              |${applyText}
              |""".stripMargin()
       project.logger.lifecycle("\n[Hadoop DSL Auto] Generating the Hadoop DSL task 'autoAzkabanFlows_${definitionSetName}':")
       project.logger.lifecycle(hadoopDslText)
    }

    return project.tasks.create("autoAzkabanFlows_${definitionSetName}") { task ->
      description = "Automatically builds the Hadoop DSL for the definitions at ${definitionSetPath}"
      group = "Hadoop Plugin - Hadoop DSL Auto"
      this.dependsOn task

      // These tasks must be executed in serial
      if (previousTask != null) {
        task.dependsOn previousTask
      }

      doLast {
        // First, clear the state of the Hadoop DSL
        dslPlugin.clearHadoopDslState()

        // Apply the definitions
        project.apply([from: definitionSetPath])

        // Override the definitions with the user profile
        dslPlugin.applyUserProfile([profilePath: profiles])

        // Apply the user's workflow scripts
        for (String workflowPath : workflowPaths) {
          project.apply([from: workflowPath])
        }

        // If the user didn't set a default Hadoop DSL build directory, set one for them
        if (dslExtension.buildDirectory == null) {
          dslExtension.buildPath(defaultBuildPath)
        }

        // Change the user's build path to be specific to this set of definitions
        dslExtension.buildPath(new File(dslExtension.buildDirectory, definitionSetName).getPath())

        // Build the Hadoop DSL for this definition set
        dslPlugin.buildHadoopDsl(dslCompiler)
      }
    } 
  }

  /**
   * Helper function to find files in the given path that match the specified pattern.
   *
   * @param path The path under which to look for files
   * @param pattern The file name pattern to match
   * @return The files in the path that match the pattern
   */
  List<File> getMatchingFiles(String path, String pattern) {
    FileTree fileTree = project.fileTree([
        dir: path,
        include: pattern
    ])

    return fileTree.toList()
  }

  /**
   * Helper method to display files found to the user.
   *
   * @param files List of files that were found
   * @param foundMsg Message to display if the list of files is non-empty
   * @param notFoundMsg Message to display if the list of files is empty
   */
  void showFilesFound(List<File> files, String foundMsg, String notFoundMsg) {
    if (files.size() > 0) {
      project.logger.lifecycle(foundMsg)

      for (File file : files) {
        String filePath = file.getAbsolutePath()
        String relativePath = filePath.replace("${project.projectDir}/", "")
        project.logger.lifecycle("\t${relativePath}")
      }
    } else {
      project.logger.lifecycle(notFoundMsg)
    }
  }
}
