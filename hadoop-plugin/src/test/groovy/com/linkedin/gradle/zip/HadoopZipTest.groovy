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

import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.CopySpec;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

class HadoopZipTest {
  Configuration hadoopRuntime;
  HadoopZipPluginTest plugin;
  Project project;

  Closure baseClosure;
  Closure closure;
  String zipName;

  @Before
  public void setup() {
    project = ProjectBuilder.builder().build();
    project.apply plugin: 'distribution';
    hadoopRuntime = project.getConfigurations().create("hadoopRuntime");
    plugin = new HadoopZipPluginTest();

    baseClosure = {};
    closure = {};
    zipName = "magic";

    /**
     * Create the project structure:
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
    def folder = project.getProjectDir();
    project.mkdir(folder.absolutePath + "/AzRoot");
    project.mkdir(folder.absolutePath + "/AzRoot/conf");
    project.mkdir(folder.absolutePath + "/AzRoot/conf/jobs");
    project.mkdir(folder.absolutePath + "/AzRoot/custom-lib");
    project.mkdir(folder.absolutePath + "/AzRoot/resources");
    project.mkdir(folder.absolutePath + "/AzRoot/sample");
    project.mkdir(folder.absolutePath + "/AzRoot/src");
    project.mkdir(folder.absolutePath + "/AzRoot/src/main");
    project.mkdir(folder.absolutePath + "/AzRoot/src/test");

    // Create files for testing
    createFilesForTesting(folder.absolutePath + "/AzRoot/conf/jobs","pig", 5);
    createFilesForTesting(folder.absolutePath + "/AzRoot/custom-lib","jar", 5);
    createFilesForTesting(folder.absolutePath + "/AzRoot/resources","avro", 5);
    createFilesForTesting(folder.absolutePath + "/AzRoot/sample","txt", 5);
    createFilesForTesting(folder.absolutePath + "/AzRoot/src/main","java", 5);
    createFilesForTesting(folder.absolutePath + "/AzRoot/src/test","testjava", 5);
  }

  /**
   * Helper function to check whether the contents of the zip contain the expected files.
   *
   * @param expected The set of file names expected in the zip
   */
  private void checkExpectedZipFiles(Set<String> expected) {
    plugin.apply(project);
    project.evaluate();

    def zipTask = project.tasks.findByName("${zipName}HadoopZip");
    zipTask.execute();

    Set<String> actual = new HashSet<String>();

    project.zipTree(((Zip)zipTask).archivePath).getFiles().each { file ->
      String pathName = file.path;
      int testIndex = pathName.indexOf("test-magic.zip");
      int rootIndex = pathName.substring(testIndex).indexOf("/") + testIndex;
      actual.add(pathName.substring(rootIndex + 1));
    }

    Assert.assertEquals(expected, actual);
  }

  /**
   * Helper function to create files to add to the zips for testing.
   *
   * @param dir The directory in which to add the files
   * @param ext The file extension
   * @param number The number of files to create
   */
  private void createFilesForTesting(String dir, String ext, int number) {
    number.times {
      new File("${dir}/sample${it}.${ext}").withWriter { writer ->
        writer.print("blah");
      }
    }
  }

  @Test
  public void testHadoopZipExtension() {
    closure = {
      from("AzRoot/src") {
        into "src"
        exclude "test"
        include "main/**/**"
      }
      from("AzRoot/resources/") { into "resources" }
      from("AzRoot/conf/jobs/") { }
    }

    Set<String> expected = new HashSet<String>();
    expected.add("resources/sample0.avro");
    expected.add("resources/sample1.avro");
    expected.add("resources/sample2.avro");
    expected.add("resources/sample3.avro");
    expected.add("resources/sample4.avro");
    expected.add("src/main/sample0.java");
    expected.add("src/main/sample1.java");
    expected.add("src/main/sample2.java");
    expected.add("src/main/sample3.java");
    expected.add("src/main/sample4.java");
    expected.add("sample0.pig");
    expected.add("sample1.pig");
    expected.add("sample2.pig");
    expected.add("sample3.pig");
    expected.add("sample4.pig");
    checkExpectedZipFiles(expected);
  }

  @Test
  public void testHadoopConfiguration() {
    hadoopRuntime.getDependencies().add(project.getDependencies().create(project.fileTree(new File("AzRoot", "custom-lib"))));

    Set<String> expected = new HashSet<String>();
    expected.add("lib/sample0.jar");
    expected.add("lib/sample1.jar");
    expected.add("lib/sample2.jar");
    expected.add("lib/sample3.jar");
    expected.add("lib/sample4.jar");
    checkExpectedZipFiles(expected);
  }

  @Test
  public void testBaseCopySpec() {
    // Add sources and resources using baseClosure
    baseClosure = {
      from("AzRoot/src") {
        into "src"
        exclude "test"
        include "main/**/**"
      }
      from("AzRoot/resources/") { into "resources" }
    }

    // Add jobs using zip specific closure.
    closure = {
      from("AzRoot/conf/jobs/") { }
    }

    Set<String> expected = new HashSet<String>();
    expected.add("resources/sample0.avro");
    expected.add("resources/sample1.avro");
    expected.add("resources/sample2.avro");
    expected.add("resources/sample3.avro");
    expected.add("resources/sample4.avro");
    expected.add("src/main/sample0.java");
    expected.add("src/main/sample1.java");
    expected.add("src/main/sample2.java");
    expected.add("src/main/sample3.java");
    expected.add("src/main/sample4.java");
    expected.add("sample0.pig");
    expected.add("sample1.pig");
    expected.add("sample2.pig");
    expected.add("sample3.pig");
    expected.add("sample4.pig");
    checkExpectedZipFiles(expected);
  }

  @Test
  public void testBaseUnion() {
    // Create a new folder called groovy under src and add 5 groovy files.
    project.mkdir(project.getProjectDir().absolutePath + "/AzRoot/src/groovy");
    createFilesForTesting(project.getProjectDir().absolutePath + "/AzRoot/src/groovy", "groovy", 5);

    // Add sources and resources using baseClosure
    baseClosure = {
      // Add source in the base but exclude java files and test folder in the specific zip spec
      from("AzRoot/src") { into "src" }
      // Add resources in base
      from("AzRoot/resources/") { into "resources" }
    }

    // Add jobs using zip specific closure.
    closure = {
      from("AzRoot/conf/jobs/") { }
      // Exclude test directory and all java files and two avro files.
      exclude "test"
      exclude "main/*.java"
      exclude "sample0.avro"
      exclude "sample1.avro"
    }

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
    checkExpectedZipFiles(expected);
  }

  @Test
  public void testBaseOverwrite() {
    // Create a new folder called groovy under src and add 5 groovy files.
    project.mkdir(project.getProjectDir().absolutePath + "/AzRoot/src/groovy");
    createFilesForTesting(project.getProjectDir().absolutePath + "/AzRoot/src/groovy","groovy",5);

    // dd sources and resources using baseClosure
    baseClosure = {
      // Add source in the base and exclude test folder
      from("AzRoot/src") {
        into "src"
        exclude "test"
      }
      // Add resources in base and exclude sample0.avro, sample1.avro and sample2.avro
      from("AzRoot/resources/") {
        into "resources"
        exclude "sample0.avro"
        exclude "sample1.avro"
        exclude "sample2.avro"
      }
    }

    // Add jobs using zip specific closure.
    closure = {
      from("AzRoot/conf/jobs/") { }

      // Overwrite base spec and include everything from src including test
      from("AzRoot/src") { into "src" }

      // Overwrite base spec and exclude only sample2.avro
      from("AzRoot/resources/") {
        into "resources"
        exclude "sample2.avro"
      }

      // Exclude only java files
      exclude "main/*.java"
    }

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
    checkExpectedZipFiles(expected);
  }

  class HadoopZipExtensionTest extends HadoopZipExtension {
    public HadoopZipExtensionTest(Project project) {
      super(project);
    }

    @Override
    public CopySpec getBaseCopySpec() {
      return project.copySpec(baseClosure);
    }

    @Override
    public CopySpec getZipCopySpec(String zipName) {
      return project.copySpec(closure);
    }

    @Override
    public Map<String, CopySpec> getZipMap() {
      Map<String, CopySpec> map = new HashMap<String, CopySpec>();
      map.put(zipName, project.copySpec(closure));
      return map;
    }
  }

  class HadoopZipPluginTest extends HadoopZipPlugin {
    @Override
    HadoopZipExtension createZipExtension(Project project) {
      HadoopZipExtensionTest extension = new HadoopZipExtensionTest(project);
      extension.libPath = "lib";
      project.extensions.add("hadoopZip", extension);
      return extension;
    }
  }
}
