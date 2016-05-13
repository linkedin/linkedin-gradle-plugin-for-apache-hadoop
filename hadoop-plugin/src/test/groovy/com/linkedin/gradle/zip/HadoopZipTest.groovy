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
package com.linkedin.gradle.zip;

import com.linkedin.gradle.tests.HelperFunctions;

import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

class HadoopZipTest {
  Configuration hadoopRuntime;
  HadoopZipPluginTest plugin;
  Project project;

  @Before
  void setup() {
    plugin = new HadoopZipPluginTest();
    project = ProjectBuilder.builder().build();
    project.apply plugin: 'distribution';
    hadoopRuntime = project.getConfigurations().create("hadoopRuntime");

    /**
     * Create the project structure and files for testing:
     * AzRoot
     *   \_conf
     *       \_jobs
     *   \_custom-lib
     *   \_resources
     *   \_sample
     *   \_src
     *       \_main
     *       \_test
     */
    String folderPath = project.getProjectDir().getAbsolutePath();
    project.mkdir("${folderPath}/AzRoot");
    project.mkdir("${folderPath}/AzRoot/conf");
    project.mkdir("${folderPath}/AzRoot/conf/jobs");
    project.mkdir("${folderPath}/AzRoot/custom-lib");
    project.mkdir("${folderPath}/AzRoot/resources");
    project.mkdir("${folderPath}/AzRoot/sample");
    project.mkdir("${folderPath}/AzRoot/src");
    project.mkdir("${folderPath}/AzRoot/src/main");
    project.mkdir("${folderPath}/AzRoot/src/test");

    HelperFunctions.createFilesForTesting("${folderPath}/AzRoot/conf/jobs", "pig", 5);
    HelperFunctions.createFilesForTesting("${folderPath}/AzRoot/custom-lib", "jar", 5);
    HelperFunctions.createFilesForTesting("${folderPath}/AzRoot/resources", "avro", 5);
    HelperFunctions.createFilesForTesting("${folderPath}/AzRoot/sample", "txt", 5);
    HelperFunctions.createFilesForTesting("${folderPath}/AzRoot/src/main", "java", 5);
    HelperFunctions.createFilesForTesting("${folderPath}/AzRoot/src/test", "testjava", 5);
  }

  /**
   * Basic test using the regular zip method to add some things to the zip.
   */
  @Test
  void testHadoopZipExtension() {
    plugin.apply(project);

    project.extensions.hadoopZip.zip("magic", {
      from("AzRoot/conf/jobs/") { }
      from("AzRoot/resources/") { into "resources" }
      from("AzRoot/src") {
        exclude "test"
        include "main/**/**"
        into "src"
      }
    });

    Set<String> expected = new HashSet<String>();
    expected.add("resources/sample0.avro");
    expected.add("resources/sample1.avro");
    expected.add("resources/sample2.avro");
    expected.add("resources/sample3.avro");
    expected.add("resources/sample4.avro");
    expected.add("sample0.pig");
    expected.add("sample1.pig");
    expected.add("sample2.pig");
    expected.add("sample3.pig");
    expected.add("sample4.pig");
    expected.add("src/main/sample0.java");
    expected.add("src/main/sample1.java");
    expected.add("src/main/sample2.java");
    expected.add("src/main/sample3.java");
    expected.add("src/main/sample4.java");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project, "magicHadoopZip", expected));
  }

  /**
   * Basic test that just adds jars from the hadoopRuntime configuration.
   */
  @Test
  void testHadoopConfiguration() {
    plugin.apply(project);

    hadoopRuntime.getDependencies().add(project.getDependencies().create(project.fileTree("AzRoot/custom-lib")));
    project.extensions.hadoopZip.main({});

    Set<String> expected = new HashSet<String>();
    expected.add("lib/sample0.jar");
    expected.add("lib/sample1.jar");
    expected.add("lib/sample2.jar");
    expected.add("lib/sample3.jar");
    expected.add("lib/sample4.jar");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project, "mainHadoopZip", expected));
  }

  /**
   * Test for using the base method and then adding to it when declaring a zip.
   */
  @Test
  void testBaseCopySpec() {
    plugin.apply(project);

    // Add sources and resources using the base method
    project.extensions.hadoopZip.base({
      from("AzRoot/resources/") { into "resources" }
      from("AzRoot/src") {
        exclude "test"
        include "main/**/**"
        into "src"
      }
    });

    // Add jobs using zip method
    project.extensions.hadoopZip.zip("magic", {
      from("AzRoot/conf/jobs/") { }
    });

    Set<String> expected = new HashSet<String>();
    expected.add("resources/sample0.avro");
    expected.add("resources/sample1.avro");
    expected.add("resources/sample2.avro");
    expected.add("resources/sample3.avro");
    expected.add("resources/sample4.avro");
    expected.add("sample0.pig");
    expected.add("sample1.pig");
    expected.add("sample2.pig");
    expected.add("sample3.pig");
    expected.add("sample4.pig");
    expected.add("src/main/sample0.java");
    expected.add("src/main/sample1.java");
    expected.add("src/main/sample2.java");
    expected.add("src/main/sample3.java");
    expected.add("src/main/sample4.java");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project, "magicHadoopZip", expected));
  }

  /**
   * Test that specifies excludes in the zip that apply to the base.
   */
  @Test
  void testBaseUnion() {
    plugin.apply(project);

    // Create a new folder called groovy under src and add 5 groovy files.
    project.mkdir(project.getProjectDir().absolutePath + "/AzRoot/src/groovy");
    HelperFunctions.createFilesForTesting(project.getProjectDir().absolutePath + "/AzRoot/src/groovy", "groovy", 5);

    // Add jobs to the zip, but exclude some java files and a test folder
    project.extensions.hadoopZip.zip("magic", {
      from("AzRoot/conf/jobs/") { }
      exclude "main/*.java"
      exclude "sample0.avro"
      exclude "sample1.avro"
      exclude "test"
    });

    // Add sources and resources to the base. Test declaring the base after declaring the zip.
    project.extensions.hadoopZip.base({
      from("AzRoot/resources/") { into "resources" }
      from("AzRoot/src") { into "src" }
    });

    Set<String> expected = new HashSet<String>();
    expected.add("resources/sample2.avro");
    expected.add("resources/sample3.avro");
    expected.add("resources/sample4.avro");
    expected.add("sample0.pig");
    expected.add("sample1.pig");
    expected.add("sample2.pig");
    expected.add("sample3.pig");
    expected.add("sample4.pig");
    expected.add("src/groovy/sample0.groovy");
    expected.add("src/groovy/sample1.groovy");
    expected.add("src/groovy/sample2.groovy");
    expected.add("src/groovy/sample3.groovy");
    expected.add("src/groovy/sample4.groovy");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project, "magicHadoopZip", expected));
  }

  /**
   * Test that overwrites the base declarations with conflicting declarations in the zip (which
   * should take precedence over the base declarations).
   */
  @Test
  void testBaseOverwrite() {
    plugin.apply(project);

    // Create a new folder called groovy under src and add 5 groovy files.
    project.mkdir(project.getProjectDir().absolutePath + "/AzRoot/src/groovy");
    HelperFunctions.createFilesForTesting(project.getProjectDir().absolutePath + "/AzRoot/src/groovy", "groovy", 5);

    // Add jobs to the zip and overwrite the base declarations. Test declaring the base after
    // declaring the zip.
    project.extensions.hadoopZip.zip("magic", {
      from("AzRoot/conf/jobs/") { }
      // Overwrite base spec and include everything from src including test
      from("AzRoot/src") {
        into "src"
      }
      // Overwrite base spec and exclude only sample2.avro
      from("AzRoot/resources/") {
        exclude "sample2.avro"
        into "resources"
      }
      // Exclude only java files
      exclude "main/*.java"
    });

    // Add sources and resources to the base
    project.extensions.hadoopZip.base({
      from("AzRoot/src") {
        exclude "test"
        into "src"
      }
      from("AzRoot/resources/") {
        exclude "sample0.avro"
        exclude "sample1.avro"
        exclude "sample2.avro"
        into "resources"
      }
    });

    Set<String> expected = new HashSet<String>();
    expected.add("resources/sample0.avro");
    expected.add("resources/sample1.avro");
    expected.add("resources/sample3.avro");
    expected.add("resources/sample4.avro");
    expected.add("sample0.pig");
    expected.add("sample1.pig");
    expected.add("sample2.pig");
    expected.add("sample3.pig");
    expected.add("sample4.pig");
    expected.add("src/groovy/sample0.groovy");
    expected.add("src/groovy/sample1.groovy");
    expected.add("src/groovy/sample2.groovy");
    expected.add("src/groovy/sample3.groovy");
    expected.add("src/groovy/sample4.groovy");
    expected.add("src/test/sample0.testjava");
    expected.add("src/test/sample1.testjava");
    expected.add("src/test/sample2.testjava");
    expected.add("src/test/sample3.testjava");
    expected.add("src/test/sample4.testjava");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project, "magicHadoopZip", expected));
  }

  class HadoopZipPluginTest extends HadoopZipPlugin {
    @Override
    HadoopZipExtension createZipExtension(Project project) {
      HadoopZipExtension extension = super.createZipExtension(project);
      extension.libPath = "lib";
      return extension;
    }
  }
}
