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
package com.linkedin.gradle.hadoopValidator.PigValidator


import com.linkedin.gradle.hadoopdsl.NamedScope
import com.linkedin.gradle.hadoopdsl.job.PigJob
import com.linkedin.gradle.hdfs.HdfsFileSystem
import org.apache.hadoop.fs.Path
import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction

import java.util.regex.Matcher
import java.util.regex.Pattern


/**
 * PigDataValidator is the class that provides the Task for validation of datafiles mentioned in the Apache Pig Scripts
 * in the project.
 */
class PigDataValidator extends DefaultTask implements PigValidator {
  Map<PigJob, NamedScope> jobMap
  boolean error = 0
  ArrayList<Tuple> err_paths = null
  Properties properties
  def fileSystem

  /**
   * Extracts all data filenames mentioned in the Pig script file
   * @param file The Pig script which is to be validated
   * @return data The List of data filenames in the script file
   */
  static List<Tuple> extractData(File file) {
    ArrayList<Tuple> data = new ArrayList<Tuple>()
    int state = 0
    String _partLine
    Matcher matcher
    String load_command
    file.eachLine { line, count ->
      line = line.trim()
      while (true) {
        if (state == 0) {
          if (line.contains("/*")) {
            state = 1
            _partLine = line.substring(line.indexOf("/*") + 2)
            line = line.substring(0, line.indexOf("/*"))
          }
          if (line.contains("--")) {
            line = line.substring(0, line.indexOf("--"))
            state = 0
          }

          matcher = line =~ "(?i)LOAD\\s*'.*?'"
          //matcher = Pattern.compile("\\bload\\b", Pattern.CASE_INSENSITIVE).matcher(line)
          while (matcher.find()) {
            load_command = matcher.group()
            String quoted_fileName = line.substring(matcher.start(),matcher.end()).tokenize()[1]

            data.add(new Tuple(quoted_fileName[1..-2], count, load_command))
          }
          if (state == 1) {
            line = _partLine
            state = 2
          } else {
            break
          }
        } else {
          if (line.contains("*/")) {
            line = line.substring(line.indexOf("*/") + 2)
            state = 0
          } else {
            break
          }
        }
      }
    }
    return data
  }

  /**
   * Validates the data files mentioned in the Apache Pig Scripts
   * in the project. This is the Task Action Function
   */
  @TaskAction
  void validate() {
    String path

    //data is an arraylist of tuple(data-dependency-name, linenumber)
    ArrayList<Tuple> data
    File script
    String alt_load_command
    String fileText
    if(err_paths==null){
      err_paths = new ArrayList<Tuple>()
    }
    err_paths.clear()
    error=false

    InputStream krbInputStream = this.getClass().getClassLoader().getResourceAsStream("oozie-krb5.conf")
    File krb5 = new File(System.getProperty("java.io.tmpdir"), "krb5.conf")
    OutputStream krbOutputStream = new FileOutputStream(krb5)

    int read
    byte[] bytes = new byte[1024]
    while((read=krbInputStream.read(bytes))!=-1){
      krbOutputStream.write(bytes,0,read)
    }
    initHdfsFileSystem(krb5)

    URI clusterURI
    String clusterURIString
    if(properties!=null && properties.containsKey("NameNode")){
      clusterURIString = properties.getProperty("NameNode")
    }else {
      throw new GradleException("Please specify NameNode address in ${project.projectDir}/.hadoopValidatorProperties")
    }
    if(!(clusterURIString ==~ /hdfs\:\/\/.*?\:[0-9]+/)){
      throw new GradleException("Invalid NameNode specification in ${project.projectDir}/.hadoopValidatorProperties\n" +
          "Please specify NameNode address as NameNode=hdfs://<namenode>:<port>")
    }
    clusterURIString = "web" + clusterURIString.replaceFirst(/\:[0-9]+/,":50070")

    clusterURI = URI.create(clusterURIString)


    fileSystem.initialize(clusterURI)
    jobMap.each{PigJob pigJob, NamedScope parentScope ->
      script = new File(pigJob.script)
      if (script.name.endsWith(".pig")) {
        project.logger.lifecycle("Checking file: \t $script")
        File subst_file = new File("${script}.substituted")
        fileText = subst_file.text
        data = extractData(subst_file)
        data.each {
          //i is a data file tuple where i[0] is the name of file and i[1] is lineno of file in the script
          i ->
          try{
            path = getPath(i[0])
            boolean e = !fileSystem.exists(new Path(path))
            if (e) {
              err_paths.add(new Tuple(script,path,i[1]))
              error = true
            }
          }catch(Exception exception){
            err_paths.add(new Tuple(script,path,i[1],exception.getMessage()))
            error = true
          }
          alt_load_command = "LOAD '$clusterURIString/$path'"
          fileText=fileText.replaceAll(Pattern.quote(i[2]),alt_load_command)
        }
        subst_file.write(fileText)
      }
    }

    krb5.delete()
    if(krbInputStream!=null){
      krbInputStream.close()
    }
    if(krbOutputStream!=null){
      krbOutputStream.close()
    }
  }

  /**
   * Gives the resolved pathName. Organizations may use their own path formats which need to be resolved to standard pathnames
   * @param pathName The pathname to be resolved
   * @return pathName The resolved pathName
   */
  String getPath(String pathName){
    return pathName
  }

  /**
   * getter for all incorrect paths found across all pig scripts generated.
   * Can be used to report all the errors together in one place.
   * @return List of incorrect paths, with associated error messages, containing script file name.
   */
  ArrayList<Tuple> getIncorrectPaths(){
    return err_paths
  }

  /**
   * initializes HdfsFilesystem for WebHdfsAccess in order to check validity of dependencies
   * @param krb5 the kerberos configuration file to configure kerberos access
   *
   * Subclasses may override this method to provide their own HdfsFileSystem
   */
  void initHdfsFileSystem(File krb5){
    fileSystem = new HdfsFileSystem(project,krb5)
  }

}