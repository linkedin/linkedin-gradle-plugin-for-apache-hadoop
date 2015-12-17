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
package com.linkedin.gradle.dependency;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.FileCollectionDependency;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.internal.artifacts.dependencies.DefaultExternalModuleDependency;
import org.gradle.api.internal.artifacts.dependencies.DefaultSelfResolvingDependency;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

class DependencyPluginTest {
  static final String SAMPLE_DEPENDENCY_FILE = "sample-dependency-pattern.json";
  Plugin plugin;
  Project project;

  @Before
  void setup() {
    project = ProjectBuilder.builder().build();
    project.apply plugin: 'distribution';
    plugin = new DummyDependencyPlugin();
  }

  @Test
  void testGetDependencyPatterns() {
    String dummyJsonFile = new File(System.getProperty("java.io.tmpdir"), SAMPLE_DEPENDENCY_FILE).getAbsolutePath();
    String dependencyJsonString = "{\n" +
      "  \"dependencyPatterns\": [\n" +
      "    {\n" +
      "      \"severity\":\"ERROR\",\n" +
      "      \"message\": \"Incompatible dependencies\",\n" +
      "      \"versionPattern\": \".*\",\n" +
      "      \"namePattern\": \".*\",\n" +
      "      \"groupPattern\": \"org\\\\.dummy\\\\..*\"\n" +
      "    }\n" +
      "  ]\n" +
      "}"
    new File(dummyJsonFile).withWriter { writer ->
      writer.write(dependencyJsonString);
    }

    plugin.apply(project);
    Task checkDependencyTask = project.tasks["checkDependencies"];
    checkDependencyTask.setDummyDependencyJsonFile(dummyJsonFile);

    DependencyPattern actualDependencyPatterns = checkDependencyTask.getDependencyPatterns(project).get(0);
    DependencyPattern expected = new DependencyPattern("org\\.dummy\\..*",".*",".*", SEVERITY.ERROR, "Incompatible dependencies");
    Assert.assertEquals(expected, actualDependencyPatterns);
    new File(dummyJsonFile).delete();
  }

  @Test
  void testMatches() {
    plugin.apply(project);
    DummyCheckDependencyTask dependencyTask = project.tasks["checkDependencies"];

    Dependency[] testDependencies = new Dependency[3];
    testDependencies[0] = new DefaultExternalModuleDependency("org.dummy.group", "dummyName", "1.1", "testConfiguration");
    testDependencies[1] = new DefaultExternalModuleDependency("org.dummy.group", "dummyName", "2.0", "testConfiguration");
    testDependencies[2] = new DefaultExternalModuleDependency("org.sample.group", "sampleName", "dummyVersion", "testConfiguration");

    DependencyPattern[] testDependencyPatterns = new DependencyPattern[4];
    testDependencyPatterns[0]  = new DependencyPattern(".*", ".*", ".*", SEVERITY.WARN, "should match everything");
    Assert.assertTrue(dependencyTask.dependencyMatchesPattern(project, testDependencies[0], testDependencyPatterns[0]));
    Assert.assertTrue(dependencyTask.dependencyMatchesPattern(project, testDependencies[1], testDependencyPatterns[0]));
    Assert.assertTrue(dependencyTask.dependencyMatchesPattern(project, testDependencies[2], testDependencyPatterns[0]));

    testDependencyPatterns[1] = new DependencyPattern("org\\.dummy\\..*", ".*", ".*", SEVERITY.WARN, "should match only testDependencies[0] and testDependencies[1]");
    Assert.assertTrue(dependencyTask.dependencyMatchesPattern(project, testDependencies[0], testDependencyPatterns[1]));
    Assert.assertTrue(dependencyTask.dependencyMatchesPattern(project, testDependencies[1], testDependencyPatterns[1]));
    Assert.assertTrue(!dependencyTask.dependencyMatchesPattern(project, testDependencies[2], testDependencyPatterns[1]));

    testDependencyPatterns[2] = new DependencyPattern("org\\.dummy\\..*", ".*", "1\\.1", SEVERITY.WARN, "should match only testDependencies[0] ");
    Assert.assertTrue(dependencyTask.dependencyMatchesPattern(project, testDependencies[0], testDependencyPatterns[2]));
    Assert.assertTrue(!dependencyTask.dependencyMatchesPattern(project, testDependencies[1], testDependencyPatterns[2]));
    Assert.assertTrue(!dependencyTask.dependencyMatchesPattern(project, testDependencies[2], testDependencyPatterns[2]));

    testDependencyPatterns[3] = new DependencyPattern(".*", "sample.*", ".*", SEVERITY.WARN, "should match only testDependency[2] ");
    Assert.assertTrue(!dependencyTask.dependencyMatchesPattern(project, testDependencies[0], testDependencyPatterns[3]));
    Assert.assertTrue(!dependencyTask.dependencyMatchesPattern(project, testDependencies[1], testDependencyPatterns[3]));
    Assert.assertTrue(dependencyTask.dependencyMatchesPattern(project, testDependencies[2], testDependencyPatterns[3]));
  }

  @Test
  void testNull() {
    plugin.apply(project);
    DummyCheckDependencyTask dependencyTask = project.tasks["checkDependencies"];

    Dependency[] testDependencies = new Dependency[3];
    testDependencies[0] = new DefaultExternalModuleDependency(null,"dummyName", null, "testConfiguration");
    testDependencies[1] = new DefaultExternalModuleDependency(null, "dummyName", "2.0", "testConfiguration");
    testDependencies[2] = new DefaultExternalModuleDependency("org.sample.group", "dummyName", null, "testConfiguration");

    DependencyPattern testDependencyPattern = new DependencyPattern(".*", ".*", ".*", SEVERITY.WARN, "should match everything");
    Assert.assertTrue(!dependencyTask.dependencyMatchesPattern(project, testDependencies[0], testDependencyPattern))
    Assert.assertTrue(!dependencyTask.dependencyMatchesPattern(project, testDependencies[1], testDependencyPattern))
    Assert.assertTrue(!dependencyTask.dependencyMatchesPattern(project, testDependencies[2], testDependencyPattern))
  }

  @Test
  void testDisallowLocal() {
    plugin.apply(project);
    DisallowLocalDependencyTask disallowLocalDepTask = project.tasks["disallowLocalDependencies"];
    Configuration conf = project.getConfigurations().create("compile");

    disallowLocalDepTask.findLocalDependencies();
    Assert.assertFalse(disallowLocalDepTask.containsLocalDependency());

    conf.getDependencies().add(new DefaultExternalModuleDependency("groupName", "moduleName", "version"));
    disallowLocalDepTask.findLocalDependencies();
    Assert.assertFalse(disallowLocalDepTask.containsLocalDependency());

    FileCollectionDependency buildDep = new DefaultSelfResolvingDependency(project.files(project.buildDir.getAbsolutePath() + "/buildDependency"));
    conf.getDependencies().add(buildDep);
    disallowLocalDepTask.findLocalDependencies();
    Assert.assertFalse(disallowLocalDepTask.containsLocalDependency());

    FileCollectionDependency dep = new DefaultSelfResolvingDependency(project.files("fileDependency"));
    conf.getDependencies().add(dep);
    disallowLocalDepTask.findLocalDependencies();
    Assert.assertTrue(disallowLocalDepTask.containsLocalDependency());
  }

  /**
   * DummyDependencyPlugin class for unit tests.
   */
  class DummyDependencyPlugin extends DependencyPlugin {
    /**
     * Method to enable or disable dependency check. We enable the dependency check for LinkedIn.
     *
     * @return true Since we enable the dependency check for LinkedIn
     */
    @Override
    boolean isDependencyCheckEnabled() {
      return true;
    }

    /**
     * Returns the dummy CheckDependency task.
     *
     * @return The dummy CheckDependency task
     */
    @Override
    Class<? extends CheckDependencyTask> getCheckDependencyTask() {
      return DummyCheckDependencyTask.class;
    }
  }
}
