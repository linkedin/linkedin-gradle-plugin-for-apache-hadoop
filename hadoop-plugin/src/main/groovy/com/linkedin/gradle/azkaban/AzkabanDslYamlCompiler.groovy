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
package com.linkedin.gradle.azkaban;

import com.linkedin.gradle.hadoopdsl.BaseCompiler;
import com.linkedin.gradle.hadoopdsl.BaseNamedScopeContainer;
import com.linkedin.gradle.hadoopdsl.HadoopDslExtension;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Namespace;
import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.StartJob;
import com.linkedin.gradle.hadoopdsl.job.SubFlowJob;
import org.gradle.api.Project;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_FLOW_VERSION;

/**
 * Translates user-defined workflows, jobs, and properties into <workflow-name>.flow(s) and
 * <project-name>.project yaml files.
 *
 * The usage of Yaml compilation vs .job/.properties compilation is user-controlled within their
 * workflows.gradle file (or equivalent). The default is .job/.properties for now.
 * TODO reallocf change default to .flow/.project in the future
 */
class AzkabanDslYamlCompiler extends BaseCompiler {
  Yaml yamlDumper;
  String yamlProjectName;

  /**
   * Constructor for the AzkabanDslYamlCompiler.
   *
   * @param project The Gradle project
   */
  AzkabanDslYamlCompiler(Project project) {
    super(project);
    this.yamlDumper = setupYamlObject();
    this.yamlProjectName = project.name;
  }

  /**
   * Cleans up generated files from the build directory.
   *
   * Clean up both .job/.properties as well as .flow/.project for easy Flow 2.0 upgrade/rollback.
   *
   * @param buildDirectoryFile Java File object representing the build directory
   */
  @Override
  void cleanBuildDirectory(File buildDirectoryFile) {
    buildDirectoryFile.eachFileRecurse(groovy.io.FileType.FILES) { f ->
      String fileName = f.getName().toLowerCase();
      if (fileName.endsWith(".job") || fileName.endsWith(".properties") ||
              fileName.endsWith(".flow") || fileName.endsWith(".project")) {
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
   * Dumps the YamlProject for this AzkabanDslYamlCompiler.
   */
  void visitProject() {
    File out = new File(this.parentDirectory, "${this.yamlProjectName}.project");
    FileWriter fileWriter = new FileWriter(out);
    this.yamlDumper.dump(yamlizeProject(), fileWriter);

    // Set to read-only to remind people that they should not be editing auto-generated yaml files.
    out.setWritable(false);
    fileWriter.close();
  }

  /**
   * Yamlize given workflow then dump as yaml into .flow file.
   *
   * @param workflow The workflow to be converted into Yaml and dumped
   */
  @Override
  void visitWorkflow(Workflow workflow) {
    Map yamlizedWorkflow = this.yamlizeWorkflow(workflow, false);

    File out = new File(this.parentDirectory, "${workflow.name}.flow");
    FileWriter fileWriter = new FileWriter(out);
    this.yamlDumper.dump(yamlizedWorkflow, fileWriter);

    // Set to read-only to remind people that they should not be editing auto-generated yaml files.
    out.setWritable(false);
    fileWriter.close();
  }

  /**
   * Take in workflow and turn into map to be output in yaml file.
   *
   * @param workflow Workflow to be turned into map.
   * @param isSubflow Boolean whether or not the workflow is a subflow.
   * @return Map representing workflow to be output in yaml file.
   */
  Map yamlizeWorkflow(Workflow workflow, boolean isSubflow) {
    Map yamlizedWorkflow = [:];

    // Add workflow name if subflow
    if (isSubflow) {
      yamlizedWorkflow["name"] = workflow.name;
    }
    // Add type "flow" if subflow
    if (isSubflow) {
      yamlizedWorkflow["type"] = "flow";
    }
    // Add dependencies if there are any
    if (!workflow.parentDependencies.isEmpty()) {
      yamlizedWorkflow["dependsOn"] = workflow.parentDependencies.toList();
    }
    // Add configs if there are any
    Map<String, String> config = buildWorkflowConfig(workflow, isSubflow);
    if (!config.isEmpty()) {
      yamlizedWorkflow["config"] = config;
    }
    // Add jobs and subflows in one item - nodes - if there are any
    List nodes = buildNodes(workflow);
    if (!nodes.isEmpty()) {
      yamlizedWorkflow["nodes"] = nodes;
    }

    return yamlizedWorkflow;
  }

  /**
   * Take in job and turn into map to be output in yaml file.
   *
   * @param job Job to be turned into map.
   * @return Map representing job to be output in yaml file.
   */
  Map yamlizeJob(Job job) {
    Map yamlizedJob = [:];

    // Add job name
    yamlizedJob["name"] = job.name;
    // Add job type
    yamlizedJob["type"] = job.jobProperties["type"];
    // Add job dependencies if there are any
    if (!job.dependencyNames.isEmpty()) {
      yamlizedJob["dependsOn"] = job.dependencyNames.toList();
    }
    // Add job configs if there are any
    Map<String, String> config = job.buildProperties(this.parentScope);
    // Remove type and dependencies from config because they're represented elsewhere
    config.remove("type");
    config.remove("dependencies");
    if (!config.isEmpty()) {
      yamlizedJob["config"] = config;
    }

    return yamlizedJob;
  }

  Map yamlizeProject() {
    Map result = [:];
    result.put("azkaban-flow-version", AZK_FLOW_VERSION);
    return result;
  }

  /**
   * Create the workflow config from the properties in the namespace.
   *
   * @param workflow Workflow being converted
   * @param subflow Boolean regarding whether or not the workflow is a subflow
   * @return result Map of all configs associated with the workflow
   */
  private Map<String, String> buildWorkflowConfig(Workflow workflow, boolean isSubflow) {
    Map<String, String> result = [:];
    // Add all global properties to all workflows that aren't subflows
    if (!isSubflow) {
      result << addGlobalWorkflowProperties(workflow.scope.nextLevel);
    }
    // Build all workflow properties after global properties so if same property is defined in both
    // then the local workflow property is selected
    workflow.properties.each { Properties prop ->
      result << prop.buildProperties(workflow.scope.nextLevel);
    }
    return result;
  }

  /**
   * Return all globally defined properties by recursing upward from given scope until root level
   * is reached.
   *
   * @param scope Scope containing properties to be applied
   * @return Map of recursively found properties
   */
  private Map<String, String> addGlobalWorkflowProperties(NamedScope scope) {
    // Stop recursing when root level is reached - don't include root level properties.
    if (scope.nextLevel == null) {
      return [:];
    }
    // Build properties from current level and add them to map to be returned.
    Map <String, String> thisLevelProperties = [:];
    scope.thisLevel.each { key, val ->
      // Could contain things other than Properties such as workflows,
      // so make sure to only include Properties objects.
      if (val.getClass() == Properties) {
        thisLevelProperties << ((Properties) val).buildProperties(scope.nextLevel);
      }
    }
    // If same property is defined twice, take the most local property
    // i.e. workflow property taken over global property.
    return addGlobalWorkflowProperties(scope.nextLevel) << thisLevelProperties;
  }

  /**
   * Create the workflow nodes from the jobs/subflows defined in the workflow.
   * Do not include StartJobs and SubFlowJobs because they aren't needed in Flow 2.0
   * Still include LaunchJobs because the DAG engine still requires them - they may not be needed
   * in the future.
   *
   * @param workflow Workflow being constructed
   * @return result List of all nodes converted to String Maps
   */
  private List buildNodes(Workflow workflow) {
    List result = [];

    // Add all jobs except StartJobs and SubFlowJobs
    workflow.jobsToBuild.each { Job job ->
      if (job.class != StartJob.class && job.class != SubFlowJob.class) {
        result.add(yamlizeJob(job));
      }
    }
    // Add all subflows
    workflow.flowsToBuild.each { Workflow subflow ->
      result.add(yamlizeWorkflow(subflow, true));
    }
    // Remove all StartJobs and SubFlowJobs from dependencies of other jobs
    workflow.jobsToBuild.each { Job job ->
      if (job.class == StartJob.class || job.class == SubFlowJob.class) {
        result.each { node ->
          if (node["dependsOn"]) {
            node["dependsOn"].remove(job.name);
            // Remove dependsOn key/val pair from node if dependsOn is now empty
            if (node["dependsOn"].isEmpty()) {
              node.remove("dependsOn");
            }
          }
        }
      }
    }
    return result;
  }
}
