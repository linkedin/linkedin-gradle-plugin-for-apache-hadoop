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
package com.linkedin.gradle.validator.pig

import com.linkedin.gradle.hadoopdsl.NamedScope
import com.linkedin.gradle.hadoopdsl.job.PigJob
import com.linkedin.gradle.hdfs.HdfsFileSystem
import com.linkedin.gradle.validator.hadoop.HadoopValidatorUtil

import java.util.regex.Matcher

import org.apache.hadoop.fs.Path

import org.gradle.api.DefaultTask
import org.gradle.api.tasks.TaskAction

/**
 * The PigDataValidator class provides a task for validation of data files mentioned in the Apache
 * Pig Scripts in the project.
 */
class PigDataValidator extends DefaultTask implements PigValidator {
  Map<PigJob, NamedScope> jobMap;
  boolean error = 0;
  ArrayList<Tuple> err_paths = null;
  Properties properties;
  def fileSystem;

  /**
   * Extracts all data file names mentioned in the given Pig script.
   *
   * @param file The Pig script which is to be validated
   * @return The List of data file names in the script file
   */
  static List<Tuple> extractData(File file) {
    ArrayList<Tuple> data = new ArrayList<Tuple>();
    int state = 0;
    String _partLine;
    Matcher matcher;
    file.eachLine { line, count ->
      line = line.trim();
      while (true) {
        if (state == 0) {
          if (line.contains("/*")) {
            state = 1;
            _partLine = line.substring(line.indexOf("/*") + 2);
            line = line.substring(0, line.indexOf("/*"));
          }
          if (line.contains("--")) {
            line = line.substring(0, line.indexOf("--"));
            state = 0;
          }

          matcher = line =~ "(?i)LOAD\\s*'.*?'";
          while (matcher.find()) {
            String quoted_fileName = line.substring(matcher.start(), matcher.end()).tokenize()[1];

            data.add(new Tuple(quoted_fileName[1..-2], count));
          }
          if (state == 1) {
            line = _partLine;
            state = 2;
          } else {
            break;
          }
        } else {
          if (line.contains("*/")) {
            line = line.substring(line.indexOf("*/") + 2);
            state = 0;
          } else {
            break;
          }
        }
      }
    }
    return data;
  }

  /**
   * Task that validates the data files mentioned in the Apache Pig Scripts in the project.
   */
  @TaskAction
  void validate() {
    String path;

    //data is an arraylist of tuple(data-dependency-name, linenumber)
    ArrayList<Tuple> data;
    File script;
    if (err_paths == null) {
      err_paths = new ArrayList<Tuple>();
    }
    err_paths.clear();
    error = false;

    InputStream krbInputStream = this.getClass().getClassLoader().getResourceAsStream("krb5.conf");
    File krb5 = new File(System.getProperty("java.io.tmpdir"), "krb5.conf");
    OutputStream krbOutputStream = new FileOutputStream(krb5);

    int read;
    byte[] bytes = new byte[1024];
    while ((read = krbInputStream.read(bytes)) != -1) {
      krbOutputStream.write(bytes, 0, read);
    }
    System.setProperty("java.security.krb5.conf", krb5.getAbsolutePath());
    initHdfsFileSystem(krb5);

    URI clusterURI;
    String clusterURIString = HadoopValidatorUtil.getNameNodeAddress(properties,"${project.projectDir}");

    clusterURIString = "web" + clusterURIString.replaceFirst(/\:[0-9]+/, ":50070");
    clusterURI = URI.create(clusterURIString);

    fileSystem.initialize(clusterURI);
    jobMap.each { PigJob pigJob, NamedScope parentScope ->
      script = new File(pigJob.script);
      if (script.name.endsWith(".pig")) {
        project.logger.lifecycle("Checking file: \t $script");
        File subst_file = new File("${script}.substituted");
        data = extractData(subst_file);
        data.each {
          //i is a data file tuple where i[0] is the name of file and i[1] is lineno of file in the script
          i ->
          path = i[0]
          try {
            path = getPath(i[0]);
            boolean e = !fileSystem.exists(new Path(path));
            if (e) {
              err_paths.add(new Tuple(script, path, i[1]));
              error = true;
            }
          } catch (Exception exception) {
            err_paths.add(new Tuple(script, path, i[1], exception.getMessage()));
            error = true;
          }
        }
      }
    }

    krb5.delete();
    if (krbInputStream != null) {
      krbInputStream.close();
    }
    if (krbOutputStream != null) {
      krbOutputStream.close();
    }
  }

  /**
   * Gives the resolved path name. Organizations may use their own path formats which need to be
   * resolved to standard path names.
   *
   * @param pathName The path name to be resolved
   * @return The resolved path name
   */
  String getPath(String pathName) {
    return pathName;
  }

  /**
   * Getter for all incorrect paths found across all pig scripts generated. Can be used to report
   * all the errors together in one place.
   *
   * @return List of incorrect paths, with associated error messages, containing script file name.
   */
  ArrayList<Tuple> getIncorrectPaths() {
    return err_paths;
  }

  /**
   * Initializes HdfsFilesystem for WebHdfsAccess in order to check validity of dependencies.
   * Subclasses may override this method to provide their own HdfsFileSystem.
   *
   * @param krb5 The Kerberos configuration file to configure Kerberos access
   */
  void initHdfsFileSystem(File krb5) {
    fileSystem = new HdfsFileSystem(project, krb5);
  }
}
