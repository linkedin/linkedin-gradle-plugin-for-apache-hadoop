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
package com.linkedin.gradle.libangbang;

class BangBangCommand {

  List<String> tasks;
  Map<String, String> confs;
  String gradleArguments;
  String gradleFile;
  String scriptFile;

  private static final BANGBANG = "bangbang";
  private static final TASKS = "--gradle-tasks";
  private static final CONF = "--conf";
  private static final SCRIPT_FILE = "--script-file";
  private static final GRADLE_FILE = "--gradle-file";
  private static final GRADLE_ARGUMENTS = "--gradle-arguments";

  /**
   * Builder for the BangBangCommand
   **/
  public static class Builder {

    private List<String> tasks;
    private Map<String, String> confs;
    private String gradleArguments;
    private String gradleFile;
    private String scriptFile;

    public Builder() {
      tasks = new ArrayList<String>();
      confs = new HashMap<String, String>();
    }

    Builder setTasks(List<String> tasks) {
      this.tasks.addAll(tasks);
      return this;
    }

    Builder setConfs(Map<String, String> confs) {
      this.confs.putAll(confs);
      return this;
    }

    Builder setGradleArguments(String gradleArguments) {
      this.gradleArguments = gradleArguments
      return this;
    }

    Builder setGradleFile(String gradleFile) {
      this.gradleFile = gradleFile
      return this;
    }

    Builder setScriptFile(String scriptFile) {
      this.scriptFile = scriptFile
      return this;
    }

    public BangBangCommand build() {
      return new BangBangCommand(this);
    }
  }

  private BangBangCommand(Builder builder) {
    this.tasks = builder.tasks;
    this.confs = builder.confs;
    this.gradleArguments = builder.gradleArguments;
    this.gradleFile = builder.gradleFile;
    this.scriptFile = builder.scriptFile;
  }

  /**
   * Returns the command as string
   * @return The command as string
   */
  public String getCommandAsString() {
    List<String> argList = new ArrayList<String>();

    argList.add(BANGBANG);

    if (tasks != null && !tasks.empty) {
      argList.add(TASKS);
      argList.add(this.tasks.join(","));
    }

    if (confs != null && !confs.isEmpty()) {
      confs.each { key, value ->
        argList.add(CONF);
        argList.add(String.format("%s=%s", key, value));
      }
    }

    if (gradleArguments != null && !gradleArguments.empty) {
      argList.add(GRADLE_ARGUMENTS);
      argList.add("\"${gradleArguments}\"");
    }

    if (gradleFile != null && scriptFile != null) {
      throw new RuntimeException("Only one of gradleFile or scriptFile must be specified");
    } else if (gradleFile != null) {
      argList.add(GRADLE_FILE);
      argList.add(gradleFile);
    } else if (scriptFile != null) {
      argList.add(SCRIPT_FILE);
      argList.add(scriptFile);
    }

    return argList.join(" ");
  }
}
