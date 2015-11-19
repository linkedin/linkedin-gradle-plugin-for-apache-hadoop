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
import com.linkedin.gradle.tests.HelperFunctions;

import org.gradle.api.Project;
import org.gradle.api.distribution.Distribution;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

class ExcludeSourceTest {
  private Project project;
  private TestScmPlugin plugin;
  private String scmPluginJsonPath;

  @Before
  public void setup() {
    plugin = new TestScmPlugin();
    project = ProjectBuilder.builder().build();
    project.apply plugin: 'distribution';

    // Write the .scmPlugin.json file to a temporary location for testing
    scmPluginJsonPath = System.getProperty("java.io.tmpdir") + "/.scmPlugin.json";

    // Create directories (dist, resources, src) and files for testing
    String folderPath = project.getProjectDir().getAbsolutePath();
    project.mkdir("${folderPath}/dist");
    project.mkdir("${folderPath}/resources");
    project.mkdir("${folderPath}/src");

    HelperFunctions.createFilesForTesting("${folderPath}/dist", "class", 5);
    HelperFunctions.createFilesForTesting("${folderPath}/resources", "avro", 5);
    HelperFunctions.createFilesForTesting("${folderPath}/src","java", 5);
  }

  /**
   * Helper function to write a custom .scmPlugin.json file to a temporary directory.
   *
   * @param json The JSON string to write to the .scmPlugin.json file
   */
  private void writeTempScmPluginJson(String json) {
    new File(scmPluginJsonPath).withWriter { writer ->
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
    plugin.apply(project);

    // sample0.java is excluded
    Set<String> expected = new HashSet<String>();
    expected.add("src/sample1.java");
    expected.add("src/sample2.java");
    expected.add("src/sample3.java");
    expected.add("src/sample4.java");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project.getRootProject(), "buildSourceZip", expected));
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
    plugin.apply(project);

    Set<String> expected = new HashSet<String>();
    expected.add("src/sample0.java");
    expected.add("src/sample1.java");
    expected.add("src/sample2.java");
    expected.add("src/sample3.java");
    expected.add("src/sample4.java");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project.getRootProject(), "buildSourceZip", expected));
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
    plugin.apply(project);

    Set<String> expected = new HashSet<String>();
    expected.add("resources/sample0.avro");
    expected.add("resources/sample1.avro");
    expected.add("resources/sample2.avro");
    expected.add("resources/sample3.avro");
    expected.add("resources/sample4.avro");
    expected.add("src/sample0.java");
    expected.add("src/sample1.java");
    expected.add("src/sample2.java");
    expected.add("src/sample3.java");
    expected.add("src/sample4.java");
    expected.add("dist/sample0.class");
    expected.add("dist/sample1.class");
    expected.add("dist/sample2.class");
    expected.add("dist/sample3.class");
    expected.add("dist/sample4.class");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project.getRootProject(), "buildSourceZip", expected));
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
    plugin.apply(project);

    Set<String> expected = new HashSet<String>();
    expected.add("src/sample0.java");
    expected.add("src/sample1.java");
    expected.add("src/sample2.java");
    expected.add("src/sample3.java");
    expected.add("src/sample4.java");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project.getRootProject(), "buildSourceZip", expected));
  }

  /**
   * Add xml files to resources but exclude only one type of file (avro) from resources directory.
   */
  @Test
  public void testFilesInsideDirectory(){
    HelperFunctions.createFilesForTesting(project.getProjectDir().absolutePath + "/resources", "xml", 5);

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
    plugin.apply(project);

    Set<String> expected = new HashSet<String>();
    expected.add("resources/sample0.xml");
    expected.add("resources/sample1.xml");
    expected.add("resources/sample2.xml");
    expected.add("resources/sample3.xml");
    expected.add("resources/sample4.xml");
    Assert.assertTrue(HelperFunctions.checkExpectedZipFiles(project.getRootProject(), "buildSourceZip", expected));
  }

  class TestScmPlugin extends ScmPlugin {
    @Override
    void apply(Project project) {
      super.apply(project);
    }

    @Override
    String getPluginJsonPath(Project project) {
      return scmPluginJsonPath;
    }
  }
}
