package com.linkedin.gradle.azkaban.yaml

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
 * corresponding Yaml counterparts then creates and fills the <workflow-name>.flow and
 * <project-name>.project yaml files.
 *
 * The usage of Yaml compilation vs .job/.properties compilation is user-controlled within their
 * workflows.gradle file (or equivalent). The default is .job/.properties for now.
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
   * Helper method for DSL elements that subclass BaseNamedScopeContainer.
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
   * Construct YamlCompiler from Workflow.
   * Expects that buildFlowTargets method has already been executed.
   *
   * @param workflow The built workflow to be converted into Yaml and output
   * @param parentScope The parent scope of the workflow
   */
  @Override
  void visitWorkflow(Workflow workflow) {
    YamlWorkflow yamlWorkflow = new YamlWorkflow(workflow);
    YamlProject yamlProject = new YamlProject();
    // todo reallocf rethink dumpYamlFile params
    dumpYamlFile(yamlWorkflow, this.parentDirectory, "${workflow.name}.flow");
    dumpYamlFile(yamlProject, this.parentDirectory, "${project.name}.project");
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
   * Write the YamlWorkflow and YamlProject yaml files into the proper directory.
   *
   * @param parentDirectory The directory where the Yaml Workflow will be written to
   * @param workflowName The name of the workflow being output
   */
  private void dumpYamlFile(YamlObject yamlObject, String parentDirectory, String fileName) {
    File out = new File(parentDirectory, fileName);
    FileWriter fileWriter = new FileWriter(out);
    this.yamlDumper.dump(yamlObject.yamlize(), fileWriter);

    // Set to read-only to remind people that they should not be editing auto-generated yaml files.
    out.setWritable(false);
    fileWriter.close()
  }

}
