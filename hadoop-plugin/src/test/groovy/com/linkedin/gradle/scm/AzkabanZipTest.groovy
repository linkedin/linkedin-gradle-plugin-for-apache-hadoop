/*
 * Copyright 2014 LinkedIn Corp.
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

package com.linkedin.gradle.scm

import org.gradle.api.Project
import org.gradle.api.artifacts.Configuration
import org.gradle.api.file.CopySpec
import org.gradle.api.tasks.bundling.Zip
import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;
import org.gradle.testfixtures.ProjectBuilder
import com.linkedin.gradle.scm.ScmPlugin


public class AzkabanZipTest {
    private Project project;
    private ScmPluginTest plugin;
    private Closure closure;
    private File azkabanLibDirectory;
    private Configuration azkabanRuntime;
    private String clustername;
    String zipPath;

    @Before
    public void setup(){
        clustername = "magic";
        project = ProjectBuilder.builder().build();
        project.apply plugin: 'distribution';
        plugin = new ScmPluginTest();
        def Folder = project.getProjectDir();
        azkabanRuntime = project.getConfigurations().create("hadoopZipConf");
        closure = {}

        /**
        * create the project structure:
        *
        * AzRoot
        *   \_src
        *       \_test
        *       \_main
        *   \_resources
        *   \_conf
        *       \_jobs
        *   \_custom-lib
        *   \_sample
        *
        **/

        project.mkdir(Folder.absolutePath + "/AzRoot");
        project.mkdir(Folder.absolutePath + "/AzRoot/src");
        project.mkdir(Folder.absolutePath + "/AzRoot/resources");
        project.mkdir(Folder.absolutePath + "/AzRoot/conf");
        project.mkdir(Folder.absolutePath + "/AzRoot/conf/jobs");
        project.mkdir(Folder.absolutePath + "/AzRoot/src/main");
        project.mkdir(Folder.absolutePath + "/AzRoot/src/test");
        project.mkdir(Folder.absolutePath + "/AzRoot/custom-lib");
        project.mkdir(Folder.absolutePath + "/AzRoot/sample");

        // create files for testing
        createFilesForTesting(Folder.absolutePath + "/AzRoot/src/main","java",5);
        createFilesForTesting(Folder.absolutePath + "/AzRoot/src/test","testjava",5);
        createFilesForTesting(Folder.absolutePath + "/AzRoot/resources","avro",5);
        createFilesForTesting(Folder.absolutePath + "/AzRoot/conf/jobs","pig",5);
        createFilesForTesting(Folder.absolutePath + "/AzRoot/custom-lib","jar",5);
        createFilesForTesting(Folder.absolutePath + "/AzRoot/sample","txt",5);

    }

    private void createFilesForTesting(String dir, String ext,int number){
        number.times{
            def filename = dir +  "/sample" + it.toString() + "."  + ext;
            String toWrite = "blah";
            PrintWriter writer = new PrintWriter(filename);
            writer.print(toWrite);
            writer.close();
        }
    }


    @Test
    public void testHadoopZipExtension(){
        closure =  {
            from("AzRoot/src") {
                into "src"
                exclude "test"
                include "main/**/**"
            }
            from("AzRoot/resources/"){
                into "resources"
            }
            from("AzRoot/conf/jobs/"){
            }
        }

        Set<String> actual = new HashSet<String>();
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
        expected.add("test-sources.zip");

        plugin.apply(project);

        project.getRootProject().tasks["buildSourceZip"].execute();
        zipPath = project.getRootProject().tasks["buildSourceZip"].archivePath.path;

        def task = project.getTasksByName("${clustername}HadoopZip",false);
        def zipTask = task.iterator().next();
        zipTask.execute();

        project.zipTree(((Zip)zipTask).archivePath).getFiles().each {
            file->
                String str = file.path;
                int testIndex = str.indexOf("test-magic.zip");
                int rootIndex =  str.substring(testIndex).indexOf("/") + testIndex;
                actual.add(file.path.substring(rootIndex+1,str.length()));
        }
        Assert.assertEquals(expected,actual);
    }

    @Test
    public void testBasicZip(){
        Set<String> actual = new HashSet<String>();
        Set<String> expected = new HashSet<String>();
        expected.add("test-sources.zip");

        plugin.apply(project);

        project.getRootProject().tasks["buildSourceZip"].execute();
        zipPath = project.getRootProject().tasks["buildSourceZip"].archivePath.path;

        def task = project.getTasksByName("${clustername}HadoopZip",false);
        def zipTask = task.iterator().next();
        zipTask.execute();

        project.zipTree(((Zip)zipTask).archivePath).getFiles().each {
            file->
                String str = file.path;
                int testIndex = str.indexOf("test-magic.zip");
                int rootIndex =  str.substring(testIndex).indexOf("/") + testIndex;
                actual.add(file.path.substring(rootIndex+1,str.length()));
        }
        Assert.assertEquals(expected,actual);
    }

    @Test
    public void testHadoopConf(){
        azkabanRuntime.getDependencies().add(project.getDependencies().create(project.fileTree(new File("AzRoot","custom-lib"))));

        Set<String> actual = new HashSet<String>();
        Set<String> expected = new HashSet<String>();

        expected.add("lib/sample0.jar");
        expected.add("lib/sample1.jar");
        expected.add("lib/sample2.jar");
        expected.add("lib/sample3.jar");
        expected.add("lib/sample4.jar");
        expected.add("test-sources.zip");

        plugin.apply(project);

        project.getRootProject().tasks["buildSourceZip"].execute();
        zipPath = project.getRootProject().tasks["buildSourceZip"].archivePath.path;

        def task = project.getTasksByName("${clustername}HadoopZip",false);
        def zipTask = task.iterator().next();
        zipTask.execute();

        project.zipTree(((Zip)zipTask).archivePath).getFiles().each {
            file->
                String str = file.path;
                int testIndex = str.indexOf("test-magic.zip");
                int rootIndex =  str.substring(testIndex).indexOf("/") + testIndex;
                actual.add(file.path.substring(rootIndex+1,str.length()));
        }
        Assert.assertEquals(expected,actual);
    }

    class HadoopZipExtensionTest extends HadoopZipExtension {

        public HadoopZipExtensionTest(Project project){
            super(project);
        }

        @Override
        public List<CopySpec> getContentList(String cluster){
            List<CopySpec> hadoopZipList = new ArrayList<CopySpec>();
            hadoopZipList.add(project.copySpec(closure));
            return hadoopZipList;
        }

        @Override
        public Map<String,List<CopySpec>> getClusterMap(){
            List<CopySpec> hadoopZipList = new ArrayList<CopySpec>();
            hadoopZipList.add(project.copySpec(closure));
            Map<String,List<CopySpec>> clusterMap = new HashMap<String,List<CopySpec>>();
            clusterMap.put(clustername,hadoopZipList);
            return clusterMap;
        }
    }

    class ScmPluginTest extends ScmPlugin{

        @Override
        def getAzkabanZipLibDir(project){
            return azkabanLibDirectory!=null?azkabanLibDirectory:"lib";
        }

        @Override
        Configuration prepareConfiguration(Project project){
            return azkabanRuntime;
        }

        @Override
        def createExtension(String var1,Object var2, Project project,Object... var3){
            return project.extensions.add("hadoopZip",new HadoopZipExtensionTest(project));
        }

        @Override
        String getSourceZipFilePath(Project project) {
            project.logger.lifecycle("called ${zipPath}");
            return zipPath;
        }
    }
}