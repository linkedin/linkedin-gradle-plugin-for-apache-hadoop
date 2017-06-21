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
package com.linkedin.gradle.hadoop;

import com.linkedin.gradle.azkaban.AzkabanPlugin;
import com.linkedin.gradle.dependency.DependencyPlugin;
import com.linkedin.gradle.hadoopdsl.HadoopDslPlugin;
import com.linkedin.gradle.oozie.OoziePlugin;
import com.linkedin.gradle.pig.PigPlugin;
import com.linkedin.gradle.scm.ScmPlugin;
import com.linkedin.gradle.spark.SparkPlugin;
import com.linkedin.gradle.test.TestPlugin;
import com.linkedin.gradle.validator.hadoop.HadoopValidatorPlugin;
import com.linkedin.gradle.zip.HadoopZipPlugin;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.plugins.ide.eclipse.EclipsePlugin;
import org.gradle.plugins.ide.idea.IdeaPlugin;
import org.gradle.plugins.ide.idea.model.IdeaModule;

/**
 * HadoopPlugin is the class that implements our Gradle Plugin.
 */
class HadoopPlugin implements Plugin<Project> {
  /**
   * Applies the Hadoop Plugin, which in turn applies the Hadoop DSL, Azkaban, Apache Pig and SCM
   * plugins.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    addHadoopConfigurations(project);
    project.getPlugins().apply(getAzkabanPluginClass());
    project.getPlugins().apply(getDependencyPluginClass());
    project.getPlugins().apply(getHadoopDslPluginClass());
    project.getPlugins().apply(getHadoopValidatorPluginClass());
    project.getPlugins().apply(getHadoopZipPluginClass());
    project.getPlugins().apply(getOoziePluginClass());
    project.getPlugins().apply(getPigPluginClass());
    project.getPlugins().apply(getScmPluginClass());
    project.getPlugins().apply(getSparkPluginClass());
    project.getPlugins().apply(getTestPluginClass());
    setupTaskDependencies(project);
  }

  /**
   * Create hadoop configurations - hadoopRuntime and clusterProvided
   * @param project The Gradle project
   */
  void addHadoopConfigurations(Project project) {
    addHadoopDslConfiguration(project);
    addHadoopRuntimeConfiguration(project);
    addClusterProvidedConfiguration(project);
  }

  /**
   * Prepare the "clusterProvided" Hadoop configuration for the project.
   *
   * @param project The Gradle project
   * @return The clusterProvided configuration
   */
  Configuration addClusterProvidedConfiguration(Project project) {
    // Create clusterProvided configuration and set it as a private configuration
    Configuration clusterProvided = project.getConfigurations().create("clusterProvided");
    clusterProvided.setVisible(false);

    // We get default, testCompile, compile and runtime configurations from Java plugin
    project.getPlugins().withType(JavaPlugin) {

      // Remove the clusterProvided dependencies from default configuration
      clusterProvided.dependencies.all {
        dep -> project.getConfigurations().getByName('default').exclude group: dep.group, module: dep.name
      }

      // testCompile should extend from clusterProvided so that all the clusterProvided
      // dependencies including transitive dependencies are on the classpath to facilitate testing
      project.getConfigurations().getByName("testCompile").extendsFrom(clusterProvided);

      // Add clusterProvided jars on the classpath, this will not add transitive dependencies of each jar
      project.sourceSets.main.compileClasspath += clusterProvided
      if (project.tasks['javadoc'].hasProperty('classpath')) {
        project.tasks['javadoc'].classpath += clusterProvided
      }

      // Idea doesn't understand the above classpath modification. Adding it explicitly. Java
      // plugin is also required for getting non empty scopes.
      project.getPlugins().withType(IdeaPlugin) {
        IdeaModule ideaModule = project.idea.module;

        // Additional checks for map keys
        if (ideaModule.scopes.containsKey("PROVIDED") && ideaModule.scopes.PROVIDED.containsKey("plus")) {
          project.idea.module.scopes.PROVIDED.plus += [clusterProvided]
        }
      }
    }

    // Eclipse understands the classpath, but add it, to be on the safe side
    project.getPlugins().withType(EclipsePlugin) {
      project.eclipse.classpath.plusConfigurations += [clusterProvided]
    }

    return clusterProvided;
  }

  /**
   * Creates a Gradle dependency configuration for the Hadoop DSL. This dependency configuration is
   * only used to hold the jar for the Hadoop DSL for Intellij IDEA syntax auto-completion.
   *
   * @param project The Gradle project
   */
  protected Configuration addHadoopDslConfiguration(Project project) {
    Configuration hadoopDslConfiguration = project.getConfigurations().create("hadoopDsl");

    // This configuration should only hold the Hadoop DSL jars and thus should be non-transitive
    hadoopDslConfiguration.transitive = false;

    // Tell IntelliJ Idea about our custom dependency configuration. Note that the Java plugin is
    // also required for getting non-empty scopes.
    project.getPlugins().withType(IdeaPlugin) {
      IdeaModule ideaModule = project.idea.module;

      if (ideaModule.scopes.containsKey("PROVIDED") && ideaModule.scopes.PROVIDED.containsKey("plus")) {
        ideaModule.scopes.PROVIDED.plus += [hadoopDslConfiguration];
      }
    }

    return hadoopDslConfiguration;
  }

  /**
   * Prepare the "hadoopRuntime" Hadoop configuration for the project.
   *
   * @param project The Gradle project
   * @return The hadoopRuntime configuration
   */
  Configuration addHadoopRuntimeConfiguration(Project project) {
    Configuration hadoopRuntime = project.getConfigurations().create("hadoopRuntime");

    // For Java projects, the Hadoop configuration should contain the runtime jars by default.
    project.getPlugins().withType(JavaPlugin) {
      hadoopRuntime.extendsFrom(project.getConfigurations().getByName("runtime"));
    }

    // Tell IntelliJ Idea about our custom dependency configuration. Note that the Java plugin is
    // also required for getting non-empty scopes. This enables users to search for classes in the
    // hadoopRuntime configuration with Ctrl+N <className>.
    project.getPlugins().withType(IdeaPlugin) {
      IdeaModule ideaModule = project.idea.module;

      if (ideaModule.scopes.containsKey("RUNTIME") && ideaModule.scopes.RUNTIME.containsKey("plus")) {
        ideaModule.scopes.RUNTIME.plus += [hadoopRuntime];
      }
    }

    return hadoopRuntime;
  }

  /**
   * Factory method to return the AzkabanPlugin class. Subclasses can override this method to
   * return their own AzkabanPlugin class.
   *
   * @return Class that implements the AzkabanPlugin
   */
  Class<? extends AzkabanPlugin> getAzkabanPluginClass() {
    return AzkabanPlugin.class;
  }

  /**
   * Factory method to return the DependencyPlugin class. Subclasses can override this method to
   * return their own DepndencyPlugin class.
   * @return Class that implements the DependencyPlugin
   */
  Class<? extends DependencyPlugin> getDependencyPluginClass() {
    return DependencyPlugin.class;
  }

  /**
   * Factory method to return the HadoopDslPlugin class. Subclasses can override this method to
   * return their own HadoopDslPlugin class.
   *
   * @return Class that implements the HadoopDslPlugin
   */
  Class<? extends HadoopDslPlugin> getHadoopDslPluginClass() {
    return HadoopDslPlugin.class;
  }

  /**
   * Factory method to return the HadoopValidatorPlugin class. Subclasses can override this method
   * to return their own HadoopValidatorPlugin class.
   *
   * @return Class that implements the HadoopValidatorPlugin
   */
  Class<? extends HadoopValidatorPlugin> getHadoopValidatorPluginClass() {
    return HadoopValidatorPlugin.class;
  }

  /**
   * Factory method to return the HadoopZipPlugin class. Subclasses can override this method to
   * return their own HadoopZipPlugin class.
   *
   * @return Class that implements the HadoopZipPlugin
   */
  Class<? extends HadoopZipPlugin> getHadoopZipPluginClass() {
    return HadoopZipPlugin.class;
  }

  /**
   * Factory method to return the OoziePlugin class. Subclasses can override this method to return
   * their own OoziePlugin class.
   *
   * @return Class that implements the OoziePlugin
   */
  Class<? extends OoziePlugin> getOoziePluginClass() {
    return OoziePlugin.class;
  }

  /**
   * Factory method to return the PigPlugin class. Subclasses can override this method to return
   * their own PigPlugin class.
   *
   * @return Class that implements the PigPlugin
   */
  Class<? extends PigPlugin> getPigPluginClass() {
    return PigPlugin.class;
  }

  /**
   * Factory method to return the ScmPlugin class. Subclasses can override this method to return
   * their own ScmPlugin class.
   *
   * @return Class that implements the ScmPlugin
   */
  Class<? extends ScmPlugin> getScmPluginClass() {
    return ScmPlugin.class;
  }

  /**
   * Factory method to return the SparkPlugin class. Subclasses can override this method to return
   * their own SparkPlugin class.
   *
   * @return Class that implements the SparkPlugin
   */
  Class<? extends SparkPlugin> getSparkPluginClass() {
    return SparkPlugin.class;
  }

  /**
   * Factory method to return the TestPlugin class. Subclasses can override this method to return their
   * own TestPlugin class.
   *
   * @return  Class that implements the TestPlugin
   */
  Class<? extends TestPlugin> getTestPluginClass() {
    return TestPlugin.class;
  }
  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupTaskDependencies(Project project) {
    setupAzkabanPluginTaskDependencies(project);
    setupDependencyPluginTaskDependencies(project);
    setupHadoopDslPluginTaskDependencies(project);
    setupHadoopValidatorPluginTaskDependencies(project);
    setupHadoopZipPluginTaskDependencies(project);
    setupOoziePluginTaskDependencies(project);
    setupPigPluginTaskDependencies(project);
    setupScmPluginTaskDependencies(project);
    setupSparkPluginTaskDependencies(project);
    setupTestPluginTaskDependencies(project);
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupAzkabanPluginTaskDependencies(Project project) {
    // Before uploading to Azkaban we should build the Hadoop zips.
    Task azkabanUploadTask = project.tasks.findByName("azkabanUpload");
    Task buildHadoopZipsTask = project.tasks.findByName("buildHadoopZips");

    if (azkabanUploadTask != null && buildHadoopZipsTask != null) {
      azkabanUploadTask.dependsOn(buildHadoopZipsTask);
    }
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupDependencyPluginTaskDependencies(Project project) {
    // Empty - enables subclasses to override
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupHadoopDslPluginTaskDependencies(Project project) {
    // Empty - enables subclasses to override
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupHadoopValidatorPluginTaskDependencies(Project project) {
    // Empty - enables subclasses to override
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupHadoopZipPluginTaskDependencies(Project project) {
    // By default, each Hadoop zip should include the sources zip and the .scmMetadata file.
    Task scmMetadataTask = project.tasks.findByName("buildScmMetadata");
    Task sourceTask = project.getRootProject().tasks.findByName("buildSourceZip");
    Task startHadoopZips = project.tasks.findByName("startHadoopZips");

    // We should finish writing the metadata file and sources zip before building any of the zips.
    if (startHadoopZips != null && scmMetadataTask != null) {
      startHadoopZips.dependsOn(scmMetadataTask);
    }

    if (startHadoopZips != null && sourceTask != null) {
      startHadoopZips.dependsOn(sourceTask);
    }

    if (project.extensions.hadoopZip != null && scmMetadataTask != null) {
      project.extensions.hadoopZip.additionalPaths.add(scmMetadataTask.metadataPath);
    }

    if (project.extensions.hadoopZip != null && sourceTask != null) {
      project.extensions.hadoopZip.additionalPaths.add(sourceTask.archivePath.getAbsolutePath());
    }
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupOoziePluginTaskDependencies(Project project) {
    // Before uploading to HDFS we should build the Hadoop zips.
    Task buildHadoopZipsTask = project.tasks.findByName("buildHadoopZips");
    Task oozieUploadTask = project.tasks.findByName("oozieUpload");

    if (buildHadoopZipsTask != null && oozieUploadTask != null) {
      oozieUploadTask.dependsOn(buildHadoopZipsTask);
    }
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupPigPluginTaskDependencies(Project project) {
    // Empty - enables subclasses to override
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupScmPluginTaskDependencies(Project project) {
    // The ScmPlugin buildSourceZip task must run after the AzkabanPlugin buildAzkabanFlows task
    // that builds the Hadoop DSL, since that task creates and deletes files.
    Task azFlowTask = project.tasks.findByName("buildAzkabanFlows");
    Task sourceTask = project.getRootProject().tasks.findByName("buildSourceZip");

    if (azFlowTask != null && sourceTask != null) {
      sourceTask.mustRunAfter(azFlowTask);
    }

    // The ScmPlugin buildSourceZip task must run after the OoziePlugin buildOozieFlows task that
    // builds the Hadoop DSL, since that task creates and deletes files.
    Task ozFlowTask = project.tasks.findByName("buildOozieFlows");

    if (ozFlowTask != null && sourceTask != null) {
      sourceTask.mustRunAfter(ozFlowTask);
    }
  }

  /**
   * Helper method to setup dependencies between tasks across plugins. Subclasses can override this
   * method to customize their own task dependencies.
   *
   * @param project The Gradle project
   */
  void setupSparkPluginTaskDependencies(Project project) {
    Task buildHadoopZipsTask = project.tasks.findByName("buildHadoopZips");
    Task runSparkJobTask = project.tasks.findByName("runSparkJob");

    if (buildHadoopZipsTask != null && runSparkJobTask != null) {
      runSparkJobTask.dependsOn(buildHadoopZipsTask);
    }

    // Running local dependency check task after startHadoopZips ensures the dependencies are declared
    Task localDependencyTask = project.tasks.findByName("disallowLocalDependencies");
    Task hadoopZips = project.tasks.findByName("startHadoopZips");

    if (localDependencyTask != null && hadoopZips != null) {
      hadoopZips.dependsOn localDependencyTask;
    }
  }

  /**
   * Helper method to setup the dependencies between the buildFlowsForTesting task and the buildSourceZip
   * task in the root project. Subclasses can override this method to customize their own task dependencies
   * @param project
   */
  void setupTestPluginTaskDependencies(Project project) {
    Task buildFlowsForTestingTask = project.tasks.findByName("buildFlowsForTesting");
    Task buildSourceZipTask = project.getRootProject().tasks.findByName("buildSourceZip");

    if(buildFlowsForTestingTask!=null && buildSourceZipTask!=null) {
      buildFlowsForTestingTask.mustRunAfter buildSourceZipTask
    }
  }
}
