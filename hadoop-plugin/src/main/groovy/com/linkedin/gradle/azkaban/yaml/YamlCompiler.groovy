/*
 * Copyright 2017 LinkedIn Corp.
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
package com.linkedin.gradle.azkaban.yaml;

import com.linkedin.gradle.hadoopdsl.BaseCompiler;
import com.linkedin.gradle.hadoopdsl.BaseNamedScopeContainer;
import com.linkedin.gradle.hadoopdsl.HadoopDslExtension;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Namespace;
import com.linkedin.gradle.hadoopdsl.Workflow;
import org.gradle.api.Project;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

/**
 * Turns the internal representations of workflows, jobs, and properties into their
 * corresponding Yaml counterparts then creates and fills the <workflow-name>.flow(s) and
 * <project-name>.project yaml files.
 *
 * The usage of Yaml compilation vs .job/.properties compilation is user-controlled within their
 * workflows.gradle file (or equivalent). The default is .job/.properties for now.
 * TODO reallocf change default to .flow/.project in the future
 */
class YamlCompiler extends BaseCompiler {
  Yaml yamlDumper;
  YamlProject yamlProject;

  /**
   * Constructor for the YamlCompiler.
   *
   * @param project The Gradle project
   */
  YamlCompiler(Project project) {
    super(project);
    this.yamlDumper = setupYamlObject();
    this.yamlProject = new YamlProject(project.name);
  }

  /**
   * Cleans up generated files from the build directory.
   *
   * @param buildDirectoryFile Java File object representing the build directory
   */
  @Override
  void cleanBuildDirectory(File buildDirectoryFile) {
    buildDirectoryFile.eachFileRecurse(groovy.io.FileType.FILES) { f ->
      String fileName = f.getName().toLowerCase();
      if (fileName.endsWith(".flow") || fileName.endsWith(".project")) {
        f.delete();
      }
    }
  }

  /**
   * Selects the build directory.
   *
   * @param hadoop The HadoopDslExtension object
   * @return The build directory for this compiler
   */
  @Override
  String getBuildDirectory(HadoopDslExtension hadoop) {
    return hadoop.buildDirectory;
  }

  /**
   * At creation of compiler, visit project.
   * Only one project will ever exist per hadoop { } closure.
   */
  @Override
  void doOnceAfterCleaningBuildDirectory() {
    visitProject();
  }

  /**
   * Create and customize a Yaml object.
   * DumperOptions.FlowStyle.BLOCK indents the yaml in the expected, most readable way.
   *
   * @return new properly setup Yaml object
   */
  private static Yaml setupYamlObject() {
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    return new Yaml(options);
  }

  /**
   * Instead of visiting properties, jobs, and propertySets as done in BaseNamedScopeContainer,
   * only visit workflows and namespaces.
   *
   * Don't create any new directories - designed to be flat as opposed to nested dir structure.
   *
   * @param container The DSL element subclassing BaseNamedScopeContainer
   */
  @Override
  void visitScopeContainer(BaseNamedScopeContainer container) {
    // Save the last scope information
    NamedScope oldParentScope = this.parentScope;

    // Set the new parent scope
    this.parentScope = container.scope;

    // Visit each workflow
    container.workflows.each { Workflow workflow ->
      visitWorkflow(workflow);
    }

    // Visit each child namespace
    container.namespaces.each { Namespace namespace ->
      visitNamespace(namespace);
    }

    // Restore the last parent scope
    this.parentScope = oldParentScope;
  }

  /**
   * Dumps the YamlProject for this YamlCompiler.
   */
  void visitProject() {
    File out = new File(this.parentDirectory, "${this.yamlProject.name}.project");
    FileWriter fileWriter = new FileWriter(out);
    this.yamlDumper.dump(this.yamlProject.yamlize(), fileWriter);

    // Set to read-only to remind people that they should not be editing auto-generated yaml files.
    out.setWritable(false);
    fileWriter.close()
  }

  /**
   * Construct YamlWorkflow from Workflow then dump the YamlWorkflow.
   *
   * @param workflow The workflow to be converted into Yaml and dumped
   */
  @Override
  void visitWorkflow(Workflow workflow) {
    YamlWorkflow yamlWorkflow = new YamlWorkflow(workflow);

    File out = new File(this.parentDirectory, "${workflow.name}.flow");
    FileWriter fileWriter = new FileWriter(out);
    this.yamlDumper.dump(yamlWorkflow.yamlize(), fileWriter);

    // Set to read-only to remind people that they should not be editing auto-generated yaml files.
    out.setWritable(false);
    fileWriter.close()
  }

}
