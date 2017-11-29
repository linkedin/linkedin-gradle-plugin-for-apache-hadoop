package com.linkedin.gradle.azkaban.yaml;

import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Workflow;
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
class YamlCompiler {
  Yaml yaml;
  YamlWorkflow yamlWorkflow;
  YamlProject yamlProject;

  /**
   * Construct YamlCompiler from Workflow.
   * Expects that buildFlowTargets method has already been executed.
   *
   * @param workflow The built workflow to be converted into Yaml and output
   * @param parentScope The parent scope of the workflow
   */
  YamlCompiler(Workflow workflow, NamedScope parentScope) {
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    yaml = new Yaml(options);
    yamlWorkflow = new YamlWorkflow(workflow, parentScope, false);
    yamlProject = new YamlProject();
  }

  /**
   * Write the YamlWorkflow and YamlProject yaml files into the proper directory
   *
   * @param parentDirectory The directory where the Yaml Workflow will be written to
   * @param workflowName The name of the workflow being output
   */
  void dumpYaml(String parentDirectory, String workflowName, String projectName) {
    def initAndDumpYamlFile = { yamlObject, fileName ->
      FileWriter fileWriter = new FileWriter(new File(parentDirectory, fileName));
      yaml.dump(yamlObject.yamlize(), fileWriter);
      fileWriter.close()
    };
    initAndDumpYamlFile(yamlWorkflow, "${workflowName}.flow");
    initAndDumpYamlFile(yamlProject, "${projectName}.project");
  }

}
