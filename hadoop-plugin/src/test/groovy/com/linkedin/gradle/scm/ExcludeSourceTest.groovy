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

import org.gradle.api.Project;
import org.gradle.api.distribution.Distribution;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.testfixtures.ProjectBuilder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.linkedin.gradle.hadoop.HadoopPlugin;
import com.linkedin.gradle.scm.ScmPlugin;

class ExcludeSourceTest {

  private Distribution distPlugin;
  private Project project;
  private TestScmPlugin plugin;

  @Before
  public void setup() {
    project = ProjectBuilder.builder().build();
    project.apply plugin: 'distribution';
    plugin = new TestScmPlugin();
    def folder = project.getProjectDir();

    // Create directories: src, resources, dist
    project.mkdir(folder.absolutePath + "/src");
    project.mkdir(folder.absolutePath + "/resources");
    project.mkdir(folder.absolutePath + "/dist");

    /**
     * Create files inside directories:
     *  src/sample0.java src/sample1.java src/sample2.java src/sample3.java src/sample4.java
     *  resources/sample0.avro resources/sample1.avro resources/sample2.avro resources/sample3.avro resources/sample4.avro
     *  dist/sample0.class dist/sample1.class dist/sample2.class dist/sample3.class dist/sample4.class
     */
    createFilesForTesting(folder.absolutePath + "/src","java", 5);
    createFilesForTesting(folder.absolutePath + "/resources","avro", 5);
    createFilesForTesting(folder.absolutePath + "/dist","class", 5);
  }

  private void createFilesForTesting(String dir, String ext, int number) {
    number.times {
      def filename = dir + "/sample" + it.toString() + "." + ext;
      PrintWriter writer = new PrintWriter(filename);
      writer.print("blah");
      writer.close();
    }
  }

  /**
   * Exclude all directories except for src and exclude only a single file from src (sample0.java).
   */
  @Test
  public void testExclusionOfSingleFile() {
    Set<String> actual = new HashSet<String>();

    // write a custom json .scmPlugin file
    String path = System.getProperty("java.io.tmpdir") + "/.scmPlugin.json"
    String jsonFile = "{ \n \"sourceExclude\": [ \n \"**/.gradle\", \n \"**/userHome\", \n \"**/dist\", \n \"**/resources\",\n \"src/sample0.java\", \n \"**/build\" \n ] \n }";
    PrintWriter writer = new PrintWriter(path);
    writer.print(jsonFile);
    writer.close();

    // sample0.java is excluded
    Set<String> expected = new HashSet<String>();
    expected.add("sample1.java");
    expected.add("sample2.java");
    expected.add("sample3.java");
    expected.add("sample4.java");

    plugin.apply(project);
    def task = project.getRootProject().getTasksByName("buildSourceZip", false);
    def zipTask = task.iterator().next();
    zipTask.execute();
    project.zipTree(((Zip)zipTask).archivePath).getFiles().each{ file -> actual.add(file.name); };
    Assert.assertEquals(expected, actual);
  }

  /**
   * Exclude all the directories except for src.
   */
  @Test
  public void testExclusionOfDirectories() {
    Set<String> actual = new HashSet<String>();

    // write custom json .scmPlugin file
    String path = System.getProperty("java.io.tmpdir") + "/.scmPlugin.json";
    String jsonFile = "{ \n \"sourceExclude\": [ \n \"**/.gradle\", \n \"**/userHome\", \n \"**/dist\", \n \"**/resources\", \n \"**/build\" \n ] \n }";
    PrintWriter writer = new PrintWriter(path);
    writer.print(jsonFile);
    writer.close();

    Set<String> expected = new HashSet<String>();
    expected.add("sample0.java");
    expected.add("sample1.java");
    expected.add("sample2.java");
    expected.add("sample3.java");
    expected.add("sample4.java");

    plugin.apply(project);
    def task = project.getRootProject().getTasksByName("buildSourceZip", false);
    def zipTask = task.iterator().next();
    zipTask.execute();
    project.zipTree(((Zip)zipTask).archivePath).getFiles().each{ file -> actual.add(file.name); };
    Assert.assertEquals(expected, actual);
  }

  /**
   * Include all directories and files.
   */
  @Test
  public void testInclusionOfDirectories(){
    String path = System.getProperty("java.io.tmpdir") + "/.scmPlugin.json";
    String jsonFile = "{ \n \"sourceExclude\": [ \n \"**/.gradle\", \n \"**/userHome\", \n \"**/build\" \n ] \n }";

    PrintWriter writer = new PrintWriter(path);
    writer.print(jsonFile);
    writer.close();
    plugin.apply(project);
    def task = project.getRootProject().getTasksByName("buildSourceZip", false);
    def zipTask = task.iterator().next();
    zipTask.execute();

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

    Set<String> actual = new HashSet<String>();
    project.zipTree(((Zip)zipTask).archivePath).getFiles().each{ file -> actual.add(file.name); };
    Assert.assertEquals(expected, actual);
  }

/**
 * Exclude files based on the file type: exclude *.avro and *.class files.
 */
  @Test
  public void testExclusionOfFileType() {
    String path = System.getProperty("java.io.tmpdir") + "/.scmPlugin.json";
    String jsonFile = "{ \n \"sourceExclude\": [ \n \"**/.gradle\", \n \"**/userHome\", \n \"**/*.class\", \n \"**/*.avro\", \n \"**/build\" \n ] \n }";
    PrintWriter writer = new PrintWriter(path);
    writer.print(jsonFile);
    writer.close();
    plugin.apply(project);
    def task = project.getRootProject().getTasksByName("buildSourceZip", false);
    def zipTask = task.iterator().next();
    zipTask.execute();

    Set<String> expected = new HashSet<String>();
    expected.add("sample0.java");
    expected.add("sample1.java");
    expected.add("sample2.java");
    expected.add("sample3.java");
    expected.add("sample4.java");

    Set<String> actual = new HashSet<String>();
    project.zipTree(((Zip)zipTask).archivePath).getFiles().each{ file -> actual.add(file.name); };
    Assert.assertEquals(expected, actual);
  }

  /**
   * Add xml files to resources but exclude only one type of file (avro) from resources directory.
   */
  @Test
  public void testFilesInsideDirectory(){
    createFilesForTesting(project.getProjectDir().absolutePath + "/resources", "xml", 5);
    String path = System.getProperty("java.io.tmpdir") + "/.scmPlugin.json";
    String jsonFile = "{ \n \"sourceExclude\": [ \n \"**/.gradle\", \n \"**/userHome\", \n \"**/*.class\", \n \"**/*.java\",\n \"resources/*.avro\",\n \"**/*.avro\", \n \"**/build\" \n ] \n }";
    PrintWriter writer = new PrintWriter(path);
    writer.print(jsonFile);
    writer.close();
    plugin.apply(project);
    def task = project.getRootProject().getTasksByName("buildSourceZip", false);
    def zipTask = task.iterator().next();
    zipTask.execute();

    Set<String> expected = new HashSet<String>();
    expected.add("sample0.xml");
    expected.add("sample1.xml");
    expected.add("sample2.xml");
    expected.add("sample3.xml");
    expected.add("sample4.xml");

    Set<String> actual = new HashSet<String>();
    project.zipTree(((Zip)zipTask).archivePath).getFiles().each{ file -> actual.add(file.name); };
    Assert.assertEquals(expected, actual);
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
