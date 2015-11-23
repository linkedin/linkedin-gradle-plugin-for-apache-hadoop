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
package com.linkedin.gradle.scm;

import com.linkedin.gradle.scm.ScmPlugin;

import org.gradle.api.Project;
import org.gradle.api.distribution.Distribution;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

class ExcludeSourceTest {
  private Project project;
  private TestScmPlugin plugin;

  @Before
  public void setup() {
    plugin = new TestScmPlugin();
    project = ProjectBuilder.builder().build();
    project.apply plugin: 'distribution';

    // Create directories: dist, resources, src
    def folder = project.getProjectDir();
    project.mkdir(folder.absolutePath + "/dist");
    project.mkdir(folder.absolutePath + "/resources");
    project.mkdir(folder.absolutePath + "/src");

    // Create files inside the directories for testing
    createFilesForTesting(folder.absolutePath + "/dist","class", 5);
    createFilesForTesting(folder.absolutePath + "/resources","avro", 5);
    createFilesForTesting(folder.absolutePath + "/src","java", 5);
  }

  /**
   * Helper function to check whether the contents of the zip contain the expected files.
   *
   * @param expected The set of file names expected in the zip
   */
  private void checkExpectedZipFiles(Set<String> expected) {
    plugin.apply(project);

    def zipTask = project.getRootProject().tasks.findByName("buildSourceZip");
    zipTask.execute();
    Set<String> actual = new HashSet<String>();

    project.zipTree(((Zip)zipTask).archivePath).getFiles().each { file ->
      actual.add(file.name);
    };

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

  /**
   * Helper function to write a custom .scmPlugin.json file to a temporary directory.
   *
   * @param json The JSON string to write to the .scmPlugin.json file
   */
  private void writeTempScmPluginJson(String json) {
    String path = System.getProperty("java.io.tmpdir") + "/.scmPlugin.json"
    new File(path).withWriter { writer ->
      writer.write(json);
    }
  }

  /**
   * Exclude all directories except for src and exclude only a single file from src (sample0.java).
   */
  @Test
  public void testExclusionOfSingleFile() {
    String json = """{
      "sourceExclude": [
        "**.gradle",
        "**/build",
        "**/dist",
        "**/userHome",
        "**/resources",
        "src/sample0.java"
      ]
    }""";
    writeTempScmPluginJson(json);

    // sample0.java is excluded
    Set<String> expected = new HashSet<String>();
    expected.add("sample1.java");
    expected.add("sample2.java");
    expected.add("sample3.java");
    expected.add("sample4.java");
    checkExpectedZipFiles(expected);
  }

  /**
   * Exclude all the directories except for src.
   */
  @Test
  public void testExclusionOfDirectories() {
    String json = """{
      "sourceExclude": [
        "**.gradle",
        "**/build",
        "**/dist",
        "**/userHome",
        "**/resources"
      ]
    }""";
    writeTempScmPluginJson(json);

    Set<String> expected = new HashSet<String>();
    expected.add("sample0.java");
    expected.add("sample1.java");
    expected.add("sample2.java");
    expected.add("sample3.java");
    expected.add("sample4.java");
    checkExpectedZipFiles(expected);
  }

  /**
   * Include all directories and files.
   */
  @Test
  public void testInclusionOfDirectories(){
    String json = """{
      "sourceExclude": [
        "**.gradle",
        "**/build",
        "**/userHome"
      ]
    }""";
    writeTempScmPluginJson(json);

    Set<String> expected = new HashSet<String>();
    expected.add("sample0.avro");
    expected.add("sample1.avro");
    expected.add("sample2.avro");
    expected.add("sample3.avro");
    expected.add("sample4.avro");
    expected.add("sample0.java");
    expected.add("sample1.java");
    expected.add("sample2.java");
    expected.add("sample3.java");
    expected.add("sample4.java");
    expected.add("sample0.class");
    expected.add("sample1.class");
    expected.add("sample2.class");
    expected.add("sample3.class");
    expected.add("sample4.class");
    checkExpectedZipFiles(expected);
  }

  /**
   * Exclude files based on the file type: exclude *.avro and *.class files.
   */
  @Test
  public void testExclusionOfFileType() {
    String json = """{
      "sourceExclude": [
        "**/*.avro",
        "**/*.class",
        "**/.gradle",
        "**/build",
        "**/userHome"
      ]
    }""";
    writeTempScmPluginJson(json);

    Set<String> expected = new HashSet<String>();
    expected.add("sample0.java");
    expected.add("sample1.java");
    expected.add("sample2.java");
    expected.add("sample3.java");
    expected.add("sample4.java");
    checkExpectedZipFiles(expected);
  }

  /**
   * Add xml files to resources but exclude only one type of file (avro) from resources directory.
   */
  @Test
  public void testFilesInsideDirectory(){
    createFilesForTesting(project.getProjectDir().absolutePath + "/resources", "xml", 5);

    String json = """{
      "sourceExclude": [
        "**/*.avro",
        "**/*.class",
        "**/.gradle",
        "**/*.java",
        "**/build",
        "**/userHome",
        "resources/*.avro"
      ]
    }""";
    writeTempScmPluginJson(json);

    Set<String> expected = new HashSet<String>();
    expected.add("sample0.xml");
    expected.add("sample1.xml");
    expected.add("sample2.xml");
    expected.add("sample3.xml");
    expected.add("sample4.xml");
    checkExpectedZipFiles(expected);
  }

  class TestScmPlugin extends ScmPlugin {
    @Override
    void apply(Project project) {
      super.apply(project);
    }

    @Override
    String getPluginJsonPath(Project project) {
      return System.getProperty("java.io.tmpdir") + "/.scmPlugin.json";
    }
  }
}
