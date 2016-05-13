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

import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.internal.artifacts.dependencies.DefaultExternalModuleDependency;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.plugins.ide.eclipse.EclipsePlugin;
import org.gradle.plugins.ide.idea.IdeaPlugin;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the HadoopPlugin class.
 */
class HadoopPluginTest {
  /**
   * Simple unit test to make sure applying the plugin succeeds and that the hadoopRuntime
   * configuration was created.
   */
  @Test
  void testApplyHadoopPlugin() {
    HadoopPlugin hadoopPlugin = new HadoopPlugin();
    Project project = ProjectBuilder.builder().build();
    hadoopPlugin.apply(project);

    // Check that hadoop configurations were created successfully.
    Assert.assertNotNull(project.configurations["hadoopRuntime"]);
    Assert.assertNotNull(project.configurations["clusterProvided"]);
  }

  /**
   * Test to check if any dependency added to clusterProvided is present in testCompile and not present in
   * compile, runtime, hadoopRuntime, default.
   */
  @Test
  void testClusterProvidedConfiguration() {
    // Create plugins
    HadoopPlugin hadoopPlugin = new HadoopPlugin();
    JavaPlugin javaPlugin = new JavaPlugin();

    // Create project to test
    Project project = ProjectBuilder.builder().build();

    // Apply Java and Hadoop plugin
    project.apply plugin: javaPlugin.class
    project.apply plugin: hadoopPlugin.class

    // Check if java plugin has been applied
    Assert.assertTrue(project.pluginManager.hasPlugin('java'));

    // Check if all the configurations are present.
    Assert.assertNotNull(project.configurations["clusterProvided"]);
    Assert.assertNotNull(project.configurations["compile"]);
    Assert.assertNotNull(project.configurations["default"]);
    Assert.assertNotNull(project.configurations["hadoopRuntime"]);
    Assert.assertNotNull(project.configurations["runtime"]);
    Assert.assertNotNull(project.configurations["testCompile"]);

    Configuration clusterProvided = project.getConfigurations().getByName("clusterProvided");

    // Create sample dependencies
    Dependency[] testDependencies = new Dependency[3];
    testDependencies[0] = new DefaultExternalModuleDependency("org.sample.group", "dummyName1", "1.0")
    testDependencies[1] = new DefaultExternalModuleDependency("org.sample.group", "dummyName2", "1.0")
    testDependencies[2] = new DefaultExternalModuleDependency("org.sample.group", "dummyName3", "1.0")

    // Add all the dependencies to clusterProvided
    clusterProvided.dependencies.addAll(testDependencies);

    // For each dependency, assert if it is present in only testCompile and not in any other configuration.
    testDependencies.each {
      dependency ->
        Assert.assertFalse(project.configurations["compile"].allDependencies.contains(dependency))
        Assert.assertFalse(project.configurations["default"].allDependencies.contains(dependency))
        Assert.assertFalse(project.configurations["hadoopRuntime"].allDependencies.contains(dependency))
        Assert.assertFalse(project.configurations["runtime"].allDependencies.contains(dependency))
        Assert.assertTrue(project.configurations["testCompile"].allDependencies.contains(dependency))
    }
  }

  /**
   * Checks IDEs support for clusterProvided configuration. We'll apply idea and eclipse plugins and
   * assert that clusterProvided is added to their classpath.
   */
  @Test
  void testClusterProvidedIDESupport() {
    HadoopPlugin hadoopPlugin = new HadoopPlugin();
    IdeaPlugin ideaPlugin = new IdeaPlugin();
    EclipsePlugin eclipsePlugin = new EclipsePlugin();

    Project projectWithoutJavaPlugin = ProjectBuilder.builder().build();

    projectWithoutJavaPlugin.apply plugin: hadoopPlugin.class
    projectWithoutJavaPlugin.apply plugin: ideaPlugin.class
    projectWithoutJavaPlugin.apply plugin: eclipsePlugin.class

    Assert.assertTrue(projectWithoutJavaPlugin.idea.module.scopes.isEmpty());
    Assert.assertTrue(projectWithoutJavaPlugin.eclipse.classpath.plusConfigurations.contains(projectWithoutJavaPlugin.configurations["clusterProvided"]));

    Project projectWithJavaPlugin = ProjectBuilder.builder().build();
    JavaPlugin javaPlugin = new JavaPlugin();

    projectWithJavaPlugin.apply plugin: hadoopPlugin.class
    projectWithJavaPlugin.apply plugin: javaPlugin.class
    projectWithJavaPlugin.apply plugin: ideaPlugin.class
    projectWithJavaPlugin.apply plugin: eclipsePlugin.class

    Assert.assertTrue(projectWithJavaPlugin.idea.module.scopes.PROVIDED.plus.contains(projectWithJavaPlugin.configurations["clusterProvided"]));
    Assert.assertTrue(projectWithJavaPlugin.eclipse.classpath.plusConfigurations.contains(projectWithJavaPlugin.configurations["clusterProvided"]));
  }
}
