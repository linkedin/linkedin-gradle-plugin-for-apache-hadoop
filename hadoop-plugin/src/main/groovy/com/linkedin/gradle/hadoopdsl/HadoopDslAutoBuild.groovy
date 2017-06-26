package com.linkedin.gradle.hadoopdsl

import org.gradle.api.Project
import org.gradle.api.file.FileTree

/**
 * Gradle extension to configure Hadoop DSL auto builds. Hadoop DSL auto builds will examine a
 * specified path for Hadoop DSL definition set files. For each file found, the auto build will
 * apply the definition set file, the user's profile script (if desired), and the the user's
 * Hadoop DSL workflow scripts and move the resulting Hadoop DSL state into a Hadoop DSL namespace
 * with the definition set file name.
 * <p>
 * This extension can be specified with:
 * <pre>
 *   hadoopDslBuild {
 *     showSetup = false  // Optional - defaults to false. Displays information about the automatic setup.
 *     definitions = 'src/main/definitions'  // Optional - defaults to 'src/main/definitions'. Path to the Hadoop DSL definition sets.
 *     profiles = 'src/main/profiles'  // Optional - defaults to 'src/main/profiles'. Set to null or pass -PskipProfile=true to disable profiles. Path to the Hadoop DSL user profile scripts.
 *     workflows = 'src/main/gradle'  // Optional - defaults to 'src/main/gradle'. Path to the Hadoop DSL workflow scripts.
 *   }.autoSetup()  // Invoke this method to configure the Hadoop DSL for each definition set file
 * </pre>
 */
class HadoopDslAutoBuild {
  HadoopDslExtension dslExtension
  HadoopDslPlugin dslPlugin
  Project project

  boolean alreadySetup = false
  boolean showSetup = false

  String definitions = "src/main/definitions"
  String profiles = "src/main/profiles"
  String workflows = "src/main/gradle"

  List<File> definitionSetFiles
  List<File> profileFiles
  List<File> workflowFiles

  /**
   * Constructor for the HadoopDslAutoBuild extension.
   *
   * @param dslExtension The HadoopDslExtension
   * @param dslPlugin The HadoopDslPlugin
   */
  HadoopDslAutoBuild(HadoopDslExtension dslExtension, HadoopDslPlugin dslPlugin) {
    this.dslExtension = dslExtension
    this.dslPlugin = dslPlugin
    this.project = dslPlugin.project
  }

  /**
   * Configures the Hadoop DSL for each definition set file found in the definitions path.
   *
   * @return Reference to this HadoopDslAutoBuild instance
   */
  HadoopDslAutoBuild autoSetup() {
    if (alreadySetup) {
      throw new Exception("You cannot call `hadoopDslBuild { ... }.autoConfigure()` more than once")
    }

    alreadySetup = true

    // Now read the Hadoop DSL files for the automatic build
    definitionSetFiles = getMatchingFiles(definitions, '*.gradle').sort()
    profileFiles = getMatchingFiles(profiles, '*.gradle').sort()
    workflowFiles = getMatchingFiles(workflows, '*.gradle').sort()

    // If the user has enabled verbose mode, display information about the files we found
    if (showSetup) {
      project.logger.lifecycle("\n[Hadoop DSL Auto] Setting up Hadoop DSL automatic build configuration")

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
    List<Namespace> savedNamespaces = new ArrayList<Namespace>()

    // Before configuring the first definition set, clear the state of the Hadoop DSL so that every
    // definition set will be applied from a clean state.
    dslPlugin.clearHadoopDslState()

    for (File definitionSetFile : definitionSetFiles) {
      String definitionSetName = definitionSetFile.getName().replace(".gradle", "")
      String definitionSetPath = definitionSetFile.getAbsolutePath().replace("${project.projectDir}/", "")

      List<String> workflowPaths = workflowFiles.collect { workflowFile ->
        String filePath = workflowFile.getAbsolutePath()
        return filePath.replace("${project.projectDir}/", "")  // Relative path to the script
      }

      // Save the state of the Hadoop DSL into a temporary namespace
      savedNamespaces.add(autoSetupForDefinition(definitionSetPath, workflowPaths))

      // Clear the state of the Hadoop DSL before processing the next set of definitions
      dslPlugin.clearHadoopDslState()
    }

    // Restore the state of the Hadoop DSL by cloning the saved namespaces into the Hadoop block.
    // Mark the cloned namespace as hidden so that its name will not appear as part of the compiled
    // Hadoop DSL file names.
    for (Namespace savedNamespace : savedNamespaces) {
      Namespace hiddenNamespace = savedNamespace.clone(dslExtension.scope)
      hiddenNamespace.scope.hidden = true
      dslExtension.configureNamespace(hiddenNamespace, null)
    }
  }

  /**
   * Helper method to automatically configure the Hadoop DSL for a particular definition. This
   * method applies the given definition set file, the user's profile (if desired), and the user's
   * workflow scripts. Then it saves the state of the Hadoop DSL into a temporary element and clears
   * the Hadoop DSL state for for the next definition.
   *
   * @param definitionSetPath Relative path to the definition set file
   * @param workflowPaths Collection of relative paths to the user's worflow scripts
   * @return Temporary namespace with the saved Hadoop DSL state
   */
  Namespace autoSetupForDefinition(String definitionSetPath, List<String> workflowPaths) {
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
       project.logger.lifecycle("\n[Hadoop DSL Auto] Configuring Hadoop DSL for definition ${definitionSetName}':")
       project.logger.lifecycle(hadoopDslText)
    }

    // Apply the definitions
    project.apply([from: definitionSetPath])

    // Override the definitions with the user profile
    dslPlugin.applyUserProfile([profilePath: profiles])

    // Apply the user's workflow scripts
    for (String workflowPath : workflowPaths) {
      project.apply([from: workflowPath])
    }

    // Save the state of the Hadoop DSL into a temporary namespace
    Namespace savedNamespace = dslPlugin.factory.makeNamespace(definitionSetName, dslPlugin.project, null)
    dslExtension.clone(savedNamespace)

    // Return the temporary namespace so that it can be restored later
    return savedNamespace
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
