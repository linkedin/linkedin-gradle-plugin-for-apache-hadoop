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

import java.nio.file.Paths
import java.util.regex.Matcher
import java.util.regex.Pattern

import org.apache.hadoop.fs.Path

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.tasks.TaskAction

/**
 * The PigDependencyValidator class provides a task for validation of dependencies mentioned in the
 * Apache Pig Scripts in the project.
 */
class PigDependencyValidator extends DefaultTask implements PigValidator {

  static final int LOCAL_REL = 0, LOCAL_ABS = 1, HDFS = 2, REPO = 3;

  Map<PigJob, NamedScope> jobMap;
  Properties properties;
  def fileSystem;
  boolean error = 0;
  ArrayList<Tuple> err_paths = null;
  ArrayList<File> zipContents;
  String libpath;

  /**
   * Task that validates the dependencies mentioned in the Apache Pig Scripts in the project.
   */
  @TaskAction
  void validate() {
    String path;

    //project.dependencies
    ArrayList<Dependency> udfs = new ArrayList<Dependency>();
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

    URI clusterURI = null;
    String clusterURIString;
    String repo_url = null;
    String fileText;
    String alt_reg_command;
    Matcher matcher;

    jobMap.each { PigJob pigJob, NamedScope parentScope ->
      script = new File(pigJob.script);
      if (script.name.endsWith(".pig")) {
        project.logger.lifecycle("Checking file: \t $script");
        File subst_file = new File("${script}.substituted");
        udfs = extractDependencies(subst_file);
        fileText = subst_file.text;
        udfs.each { dep ->
          path = dep.filename;
          if (dep.pathType == Dependency.PathType.HDFS) {
            path = getPath(path);
            if (clusterURI == null) {
              initHdfsFileSystem(krb5);
              clusterURIString = HadoopValidatorUtil.getNameNodeAddress(properties,"${project.projectDir}");

              clusterURIString = "web" + clusterURIString.replaceFirst(/\:[0-9]+/, ":50070");
              clusterURI = URI.create(clusterURIString);
              fileSystem.initialize(clusterURI);
            }

            alt_reg_command = "REGISTER $clusterURIString/$path";
            fileText = fileText.replaceAll(Pattern.quote(dep.reg_command), Pattern.quote(alt_reg_command));

            boolean e = !fileSystem.exists(new Path(path));
            if (e) {
              err_paths.add(new Tuple(script, "Dependency validator found invalid path $path at line<${dep.lineno}>"));
              error = true;
            }
          } else if (dep.pathType == Dependency.PathType.REPO) {

            if (repo_url == null) {
              try{
                repo_url = HadoopValidatorUtil.getRepoURL(properties,"${project.projectDir}");
              }catch(GradleException e){
                throw new GradleException(e.getMessage()+":- Needed for checking artifact $path at line<${dep.lineno}>");
              }
            }

            //check artifactory
            ArrayList<String> udf = path.split(':');

            String udf_name = udf[1];
            String udf_groupID = udf[0].replaceAll(Pattern.quote("."), "/");
            String udf_version = udf[2];
            String url = repo_url + (repo_url[-1] == '/' ? '' : '/') + udf_groupID + '/' + udf_name + '/' +
                udf_version + '/' + "${udf_name}-${udf_version}.jar";
            try {
              checkResponse(url, dep.lineno);
            } catch (Exception e) {
              err_paths.add(new Tuple(script, e.getMessage()));
              error = true;
            }
          } else if (dep.pathType == Dependency.PathType.LOCAL_ABSOLUTE) {
            project.logger.warn(
                "path $path at line<${dep.lineno}> includes keyword 'file:' which is used to denote absolute file path. It may not be the intended path.");
          } else if (dep.pathType == Dependency.PathType.LOCAL_RELATIVE) {

            String filename = Paths.get(path).getFileName().toString();

            int _flag = 0;
            zipContents.each {
              if (_flag != 2 && it.name == filename) {
                _flag = 1;
                matcher = dep.reg_command =~ /'.*?'/;
                if (matcher.find()) {
                  alt_reg_command = dep.reg_command[0..matcher.start() - 1] + "$it" +
                      dep.reg_command.substring(matcher.end());
                } else {
                  alt_reg_command = "REGISTER $it";
                }
                libpath = libpath[-1] == '/' ? libpath : (libpath + '/');
                if (Paths.get(libpath + it.name).compareTo(Paths.get(path)) == 0) {
                  _flag = 2;
                }
              }
            }
            if (_flag == 1) {
              project.logger.warn(
                  "The dependency $path at line<${dep.lineno}> does not refer to a jar in libPath $libpath. It may not be the intended path.");
            } else if (_flag == 0) {
              err_paths.add(new Tuple(script,
                  "Dependency validator found invalid path $path at line<${dep.lineno}>. Such a file is not included in the zip."));
              error = true;
            }
            fileText = fileText.replaceAll(Pattern.quote(dep.reg_command), alt_reg_command);
          }
        }
        subst_file.write(fileText);
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
   * Extracts all the dependencies mentioned in the given Pig script.
   *
   * @param file The Pig script which is to be validated
   * @return The List of dependencies in the script file
   */
  static List<Dependency> extractDependencies(File file) {
    ArrayList<Dependency> udfs = new ArrayList<Dependency>();
    int state = 0;
    String _partLine; ;
    Matcher matcher;

    file.eachLine { line, count ->
      line = line.trim();
      while (true) {
        if (state == 0) {
          if (line.contains("/*")) {
            state = 1;
            _partLine = line.substring(line.indexOf("/*") + 2)
            line = line.substring(0, line.indexOf("/*"));
          }
          if (line.contains("--")) {
            line = line.substring(0, line.indexOf("--"));
            state = 0;
          }

          //matcher = Pattern.compile("\\bREGISTER\\b", Pattern.CASE_INSENSITIVE).matcher(line)
          matcher = line =~ "(?i)REGISTER\\s.*?(?=[\\s;])";
          while (matcher.find()) {
            String reg_command = matcher.group();

            String quoted_fileName = line.substring(matcher.start(), matcher.end()).tokenize()[1];
            String fileName = unquote(quoted_fileName);

            Matcher _m;
            if (fileName.length() > 7 && fileName[0..6] == "hdfs://") {

              fileName = fileName.replaceFirst(/\:[0-9]+/, ":50070");
              _m = (fileName =~ /\:50070/);
              _m.find();
              String uri = "web" + fileName[0..<_m.end()];
              udfs.add(new Dependency(URLEncoder.encode(fileName[(_m.end() + 1)..-1], "UTF-8"), count, Dependency.PathType.HDFS, uri, reg_command));
            } else if (fileName.length() > 6 && fileName[0..5] == "ivy://") {
              udfs.add(new Dependency(fileName[6..-1], count, Dependency.PathType.REPO, null, reg_command));
            } else if (fileName.length() > 5 && fileName[0..4] == "file:") {
              udfs.add(new Dependency(fileName[5..-1], count, Dependency.PathType.LOCAL_ABSOLUTE, null, reg_command));
            } else {
              udfs.add(new Dependency(fileName, count, Dependency.PathType.LOCAL_RELATIVE, null, reg_command));
            }
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
    return udfs;
  }

  /**
   * Utility function for unquoting a string if it is quoted.
   *
   * @param s The string to be unquoted
   * @return The unquoted string
   */
  static String unquote(String s) {
    if (s != null && ((s.startsWith("\"") && s.endsWith("\"")) || (s.startsWith("\'") && s.endsWith("\'")))) {
      s = s.substring(1, s.length() - 1);
    }
    return s;
  }

  /**
   * Checks the response from the artifact location URL in the Ivy repository supplied.
   *
   * @param urlString The URL of the artifact location in the repository
   * @param lineno The line no of the dependency in the Pig script file where it is mentioned
   * @throws MalformedURLException If the URL is malformed.
   * @throws IOException If response code is not 200, i.e. if the response is not OK
   */
  static void checkResponse(String urlString, int lineno)
      throws MalformedURLException, IOException {
    URL url = new URL(urlString);
    HttpURLConnection huc = (HttpURLConnection) url.openConnection();
    huc.setRequestMethod("HEAD");
    huc.connect();
    if (huc.getResponseCode() != 200) {
      throw new GradleException(
          "Dependency validator found invalid path for jar:- $urlString at line<$lineno>\n" + huc.getResponseMessage());
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
   * Getter for all incorrect paths found across all pig scripts generated.
   * Can be used to report all the errors together in one place.
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

/**
 * Objects of this class are made for each of the dependencies mentioned in Pig scripts.
 */
class Dependency {

  public enum PathType {

    //static final int LOCAL_REL = 0, LOCAL_ABS = 1, HDFS = 2, REPO = 3;

    //Local Dependency
    LOCAL_RELATIVE(0, 'This indicates a local dependency'),

    //Local Dependency with absolute path mentioned
        LOCAL_ABSOLUTE(1, 'This indicates a local dependency, but one with absolute path mentioned'),

    //Dependency residing in HDFS
        HDFS(2, 'Used for dependencies which must be fetched from HDFS'),

    //Dependency residing in ivy repository
        REPO(3, 'Used for dependencies which must be fetched from the ivy repository')

    private int value;
    private String description;

    private PathType(int value, String description) {
      this.value = value;
      this.description = description;
    }
  }

  String filename = null;
  int lineno;
  PathType pathType;
  String uri
  String reg_command;

  Dependency(String filename, int lineno, PathType pathType, String uri, String reg_command) {
    this.filename = filename;
    this.lineno = lineno;
    this.pathType = pathType;
    this.uri = uri;
    this.reg_command = reg_command;
  }

  @Override
  String toString() {
    return "Dependency<path ${filename} at line<${lineno}> of type ${getTypePath()} and reg_command<$reg_command> >";
  }

  /**
   * Returns the string information corresponding to the input path type.
   *
   * @return Info about the path type
   */
  String getTypePath() {
    switch (pathType) {
      case PathType.HDFS:
        return "HDFS cluster uri:-$uri";
        break;
      case PathType.LOCAL_ABSOLUTE:
        return "Local file with absolute path";
        break;
      case PathType.LOCAL_RELATIVE:
        return "Local file with relative path";
        break;
      case PathType.REPO:
        return "ivy repo path";
        break;
    }
  }
}
