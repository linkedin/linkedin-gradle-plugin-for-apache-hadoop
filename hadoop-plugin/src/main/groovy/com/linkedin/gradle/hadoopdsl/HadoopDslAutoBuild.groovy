package com.linkedin.gradle.hadoopdsl

import org.gradle.api.Project
import org.gradle.api.file.FileCollection
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
 *     showSetup = false                     // Optional - defaults to false. Displays information about the automatic setup.
 *
 *     definitions = 'src/main/definitions'  // Optional - defaults to 'src/main/definitions'. Path to the Hadoop DSL definition sets.
 *     profiles = 'src/main/profiles'        // Optional - defaults to 'src/main/profiles'. Set to null or pass -PskipProfile=true to disable profiles. Path to the Hadoop DSL user profile scripts.
 *     workflows = 'src/main/gradle'         // Optional - defaults to 'src/main/gradle'. Path to the Hadoop DSL workflow scripts.
 *
 *     // Properties to specify the definition and workflow files to apply (and the order in which
 *     // to apply them). These properties override the definitions and workflow paths set above.
 *     definitionFiles = files(['src/main/otherDefs/defs1.gradle', 'src/main/otherDefs/defs2.gradle']
 *     workflowFiles = files(['src/main/otherFlows/flows1.gradle', 'src/main/otherFlows/defs2.gradle']
 *
 *     // Property to specify what workflow files should be applied first before any other workflow
 *     // files. Use this to apply helper scripts that should be applied before anything else.
 *     workflowFilesFirst = files(['src/main/gradle/common.gradle']
 *
 *     // Properties to customize the user profile to apply. These can also be customized with command line options.
 *     profileName = 'ackermann'             // Optional - defaults to null (in which case your user name is used). Name of the user profile to apply. Pass -PprofileName=<name> on the command line to override.
 *     skipProfile = false                   // Optional - defaults to false. Specifies whether or not to skip applying the user profile. Pass -PskipProfile=true on the command line to override.
 *   }.autoSetup()  // Invoke this method to configure the Hadoop DSL for each definition set file
 * </pre>
 */
class HadoopDslAutoBuild {
  boolean alreadySetup = false
  HadoopDslExtension dslExtension
  HadoopDslPlugin dslPlugin
  Project project

  // Properties for users to specify whether or not to print information about the automatic build
  boolean showSetup = false

  // Properties for users to specify the paths in which to find the definition files, the user
  // profile files and the Hadoop DSL workflow files.
  String definitions = "src/main/definitions"
  String profiles = "src/main/profiles"
  String workflows = "src/main/gradle"

  // Properties for users to manually specify the definition and workflow files to apply (and the
  // order in which to apply them). If these properties are non-empty, they will override any files
  // found in the definitions and workflows paths (specified above).
  FileCollection definitionFiles
  FileCollection workflowFiles

  // Property for users to manually specify what workflow files should be applied first (and the
  // order in which to apply them) before the other workflow files. Users can use this to apply
  // helper scripts that should be applied before anything else.
  FileCollection firstWorkflowFiles

  // Properties for users to manually specify the user profile to apply
  String profileName = null
  boolean skipProfile = false

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
      throw new Exception("You cannot call `hadoopDslBuild { ... }.autoSetup()` more than once")
    }

    alreadySetup = true

    // Now read the Hadoop DSL files for the automatic build. The user can override the definition
    // and workflow files by setting the associated FileCollection properties directly.
    List<File> definitionFilesList = definitionFiles?.toList() ?: getMatchingFiles(definitions, '*.gradle').sort()
    List<File> profileFilesList = getMatchingFiles(profiles, '*.gradle').sort()
    List<File> workflowFilesList = workflowFiles?.toList() ?: getMatchingFiles(workflows, '*.gradle').sort()
    List<File> firstWorkflowFilesList = firstWorkflowFiles?.toList() ?: []

    // If the user has enabled verbose mode, display information about the files we found
    if (showSetup) {
      project.logger.lifecycle("\n[Hadoop DSL Auto] Setting up Hadoop DSL automatic build configuration")

      showFilesFound(definitionFilesList,
          "\n[Hadoop DSL Auto] Found the following Hadoop DSL definition files:",
          "\n[Hadoop DSL Auto] No Hadoop DSL definition files found in ${definitions}")

      showFilesFound(profileFilesList,
          "\n[Hadoop DSL Auto] Found the following Hadoop DSL profile files:",
          "\n[Hadoop DSL Auto] No Hadoop DSL profile files found in ${profiles}")

      showFilesFound(workflowFilesList,
          "\n[Hadoop DSL Auto] Found the following Hadoop DSL workflow files:",
          "\n[Hadoop DSL Auto] No Hadoop DSL workflow files found in ${workflows}")

      showFilesFound(firstWorkflowFilesList,
          "\n[Hadoop DSL Auto] Requested to apply the following workflow files first:",
          "\n[Hadoop DSL Auto] Not requested to apply any workflow files first")
    }

    // Collect the relative paths to the definition and workflow files
    LinkedHashSet<String> definitionPathSet = new LinkedHashSet<>()
    LinkedHashSet<String> workflowPathSet = new LinkedHashSet<>()

    // Add the definition files
    definitionPathSet.addAll(definitionFilesList.collect { definitionFile ->
      return definitionFile.getAbsolutePath().replace("${project.projectDir}/", "")
    })

    // Add any workflows the user requested to apply first
    workflowPathSet.addAll(firstWorkflowFilesList.collect { workflowFile ->
      return workflowFile.getAbsolutePath().replace("${project.projectDir}/", "")
    })

    // Then add all the remaining workflow files
    workflowPathSet.addAll(workflowFilesList.collect { workflowFile ->
      return workflowFile.getAbsolutePath().replace("${project.projectDir}/", "")
    })

    List<String> definitionFilesToApply = definitionPathSet.toList()
    List<String> workflowFilesToApply = workflowPathSet.toList()

    // Before configuring the first definition set, clear the state of the Hadoop DSL so that every
    // definition set will be applied from a clean state.
    dslPlugin.clearHadoopDslState()

    // If we did not find any no definition set files, we will apply just the user profile and the
    // workflow files directly (and not save it into a temporary namespace).
    if (definitionFilesToApply.isEmpty()) {
      autoSetupForWorkflows(workflowFilesToApply)
      return this
    }

    // For each definition set, we will apply the definition set, the user profile script, and all
    // of the user's workflow scripts.
    List<Namespace> savedNamespaces = new ArrayList<Namespace>()

    for (String definitionSetPath : definitionFilesToApply) {
      // Save the state of the Hadoop DSL into a temporary namespace
      savedNamespaces.add(autoSetupForDefinition(definitionSetPath, workflowFilesToApply))

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

    return this
  }

  /**
   * Helper method to automatically configure the Hadoop DSL for a particular definition. This
   * method applies the given definition set file, the user's profile (if desired), and the user's
   * workflow scripts. Then it saves the state of the Hadoop DSL into a temporary element and
   * clears the Hadoop DSL state for for the next definition.
   *
   * @param definitionSetPath Relative path to the definition set file
   * @param workflowPaths Collection of relative paths to the user's workflow files
   * @return Temporary namespace with the saved Hadoop DSL state
   */
  Namespace autoSetupForDefinition(String definitionSetPath, List<String> workflowPaths) {
    String definitionSetName = new File(definitionSetPath).getName().replace(".gradle", "")
    def profileArgs = [profilePath: profiles]

    if (profileName != null) {
      profileArgs += [profileName: profileName]
    }

    if (skipProfile) {
      profileArgs += [skipProfile: true]
    }

    if (showSetup) {
      String applyText = workflowPaths.collect { workflowPath -> "\tapply from: '${workflowPath}'" }.join("\n")
      applyText = applyText ?: "\tNo workflow files found"
      String hadoopDslText =
            """\tapply from: '${definitionSetPath}'
              |\tapplyUserProfile ${profileArgs}
              |
              |\t// Now apply the workflow files
              |${applyText}
              |""".stripMargin()
       project.logger.lifecycle("\n[Hadoop DSL Auto] Configuring Hadoop DSL for definition ${definitionSetName}:")
       project.logger.lifecycle(hadoopDslText)
    }

    // Apply the definitions
    project.apply([from: definitionSetPath])

    // Override the definitions with the user profile
    dslPlugin.applyUserProfile(profileArgs)

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
   * Helper method to automatically configure the Hadoop DSL workflow files when no definition
   * files were found. This method applies the user's profile (if desired) and the user's workflow
   * scripts only.
   *
   * @param workflowPaths Collection of relative paths to the user's workflow files
   */
  void autoSetupForWorkflows(List<String> workflowPaths) {
    def profileArgs = [profilePath: profiles]

    if (profileName != null) {
      profileArgs += [profileName: profileName]
    }

    if (skipProfile) {
      profileArgs += [skipProfile: true]
    }

    if (showSetup) {
      String applyText = workflowPaths.collect { workflowPath -> "\tapply from: '${workflowPath}'" }.join("\n")
      applyText = applyText ?: "\tNo workflow files found"
      String hadoopDslText =
          """\tapplyUserProfile ${profileArgs}
            |
            |\t// Now apply the workflow files
            |${applyText}
            |""".stripMargin()
      project.logger.lifecycle("\n[Hadoop DSL Auto] No definition files found. Applying the user profile and workflow files only.")
      project.logger.lifecycle(hadoopDslText)
    }

    // Override the definitions with the user profile
    dslPlugin.applyUserProfile(profileArgs)

    // Apply the user's workflow scripts
    for (String workflowPath : workflowPaths) {
      project.apply([from: workflowPath])
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
