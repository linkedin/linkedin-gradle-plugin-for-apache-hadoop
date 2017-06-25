package com.linkedin.gradle.zip

import com.linkedin.gradle.hadoopdsl.HadoopDslAutoBuild
import com.linkedin.gradle.hadoopdsl.HadoopDslExtension
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin

import org.gradle.api.Project

/**
 * Extension for specifying "automatic" mode for building the Hadoop zip artifacts for the project.
 * <p>
 * Automatic mode can be specified with:
 * <pre>
 *   hadoopZip {
 *     automatic {
 *       automaticMode = true            // Optional - defaults to true
 *       showSetup = false               // Optional - defaults to false
 *       definitions = 'src/main/definitions'  // Optional - defaults to 'src/main/definitions'
 *       profiles = 'src/main/profiles'  // Optional - defaults to 'src/main/profiles'. Set to null or pass -PskipProfile=true to disable profiles.
 *       workflows = 'src/main/gradle'   // Optional - defaults to 'src/main/gradle'
 *     }
 *   }
 * </pre>
 */
class AutomaticBuild {
  Project project

  HadoopDslExtension dslExtension
  HadoopDslPlugin dslPlugin
  HadoopZipExtension zipExtension

  boolean alreadySetup = false
  boolean automaticMode = true
  boolean showSetup = false

  String definitions = "src/main/definitions"
  String profiles = "src/main/profiles"
  String workflows = "src/main/gradle"

  /**
   * Constructor for the AutomaticBuild extension.
   *
   * @param project The Gradle project
   */
  AutomaticBuild(Project project, HadoopZipExtension zipExtension) {
    this.project = project
    this.dslExtension = (HadoopDslExtension)project.extensions.findByName("hadoop")
    this.dslPlugin = (HadoopDslPlugin)project.extensions.findByName("hadoopDslPlugin")
    this.zipExtension = zipExtension
  }

  /**
   * Automatically configures all the Hadoop-related build settings for the project, including all
   * the setup for Hadoop DSL automatic builds and the hadoopZip { ... } block.
   *
   * @return Reference to this AutomaticBuild
   */
  AutomaticBuild setup() {
    if (!automaticMode) {
      throw new Exception("AutomaticBuild setup was called, but automatic mode is set to false")
    }

    if (alreadySetup) {
      throw new Exception("AutomaticBuild setup cannot be called more than once")
    }

    alreadySetup = true

    if (showSetup) {
      project.logger.lifecycle("\n[Hadoop Plugin Auto Build] Automatic mode is enabled")
    }

    // Setup the Hadoop Zip build task dependencies
    project.tasks["startHadoopZips"].dependsOn("autoAzkabanFlows")
    project.tasks["build"].dependsOn("buildHadoopZips")

    if (showSetup) {
      project.logger.lifecycle("\n[Hadoop Plugin Auto Build] Added task dependencies:")
      project.logger.lifecycle("\tstartHadoopZips.dependsOn autoAzkabanFlows")
      project.logger.lifecycle("\tbuild.dependsOn buildHadoopZips")
    }

    // Now setup the Hadoop DSL automatic build
    setupHadoopDslBuild()

    // Default the library path to the "lib" folder inside the Hadoop zip
    zipExtension.libPath = "lib"

    if (showSetup) {
      project.logger.lifecycle("\n" + "[Hadoop Plugin Auto Build] Setup hadoopZip block:")
      project.logger.lifecycle(zipExtension.toPrettyString())
    }

    return this
  }

  /**
   * Helper function to setup the Hadoop DSL auto build tasks for the project.
   */
  void setupHadoopDslBuild() {
    HadoopDslAutoBuild hadoopDslBuild = project.tasks["autoAzkabanFlows"]
    hadoopDslBuild.definitions = definitions
    hadoopDslBuild.profiles = profiles
    hadoopDslBuild.workflows = workflows
    hadoopDslBuild.showSetup = showSetup
    hadoopDslBuild.addAutoBuildTasks()
  }
}
