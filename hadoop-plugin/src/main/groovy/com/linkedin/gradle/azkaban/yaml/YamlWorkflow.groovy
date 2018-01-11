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

import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.job.StartJob;
import com.linkedin.gradle.hadoopdsl.job.SubFlowJob;

/**
 * Representation of a workflow in .flow file for Azkaban Flow 2.0
 *
 * TODO reallocf sort list of nodes into linearization of flow DAG
 *
 * Example .flow file:
 *
 * ---
 * config:
 *   user.to.proxy: azktest
 *   param.hadoopOutData: /tmp/wordcounthadoopout
 *   param.inData: /tmp/wordcountpigin
 *   param.outData: /tmp/wordcountpigout
 *
 * nodes:
 * - name: pigWordCount1
 *   type: pig
 *   config:
 *     pig.script: src/main/pig/wordCountText.pig
 * - name: hadoopWC1
 *   type: hadoopJava
 *   dependsOn:
 *   - pigWordCount1
 *   config:
 *     classpath: ./*
 *     force.output.overwrite: true
 *     input.path: ${param.inData}
 *     job.class: com.linkedin.wordcount.WordCount
 *     main.args: ${param.inData} ${param.hadoopOutData}
 *     output.path: ${param.hadoopOutData}
 * - name: hive1
 *   type: hive
 *   config:
 *     hive.script: src/main/hive/showdb.q
 * - name: NoOpTest1
 *   type: noop
 * - name: hive2
 *   type: hive
 *   dependsOn:
 *   - hive1
 *   config:
 *     hive.script: src/main/hive/showTables.sql
 * - name: java1
 *   type: javaprocess
 *   config:
 *     Xms: 96M
 *     java.class: com.linkedin.foo.HelloJavaProcessJob
 * - name: jobCommand1
 *   type: command
 *   config:
 *     command: echo "hello world from job_command_1"
 * - name: jobCommand2
 *   type: command
 *   dependsOn:
 *   - jobCommand1
 *   config:
 *     command: echo "hello world from job_command_2"
 * ---
 */
class YamlWorkflow implements YamlObject {
  String name;
  String type;
  List dependsOn;
  Map config;
  List nodes;

  /**
   * Base constructor for YamlWorkflow.
   *
   * @param workflow The workflow to be converted into Yaml
   */
  YamlWorkflow(Workflow workflow) {
    this(workflow, false);
  }

  /**
   * Construct YamlWorkflow from Workflow.
   *
   * @param workflow Workflow to be converted into Yaml
   * @param subflow Boolean regarding whether or not the workflow is a subflow
   */
  YamlWorkflow(Workflow workflow, boolean isSubflow) {
    // Include name/type in yaml only if the workflow is a subflow
    name = isSubflow ? workflow.name : null;
    type = isSubflow ? "flow" : null;
    dependsOn = workflow.parentDependencies.toList();
    config = buildConfig(workflow, isSubflow);
    nodes = buildNodes(workflow);
  }

  /**
   * Create the workflow config from the properties in the namespace.
   *
   * @param workflow Workflow being converted
   * @param subflow Boolean regarding whether or not the workflow is a subflow
   * @return result Map of all configs associated with the workflow
   */
  private static Map<String, String> buildConfig(Workflow workflow, boolean isSubflow) {
    Map<String, String> result = [:];
    // Add all global properties to all workflows that aren't subflows
    if (!isSubflow) {
      result << addGlobalProperties(workflow.scope.nextLevel);
    }
    // Build all workflow properties global root properties so if same property is defined
    // then the workflow property is selected
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
  private static Map<String, String> addGlobalProperties(NamedScope scope) {
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
    return addGlobalProperties(scope.nextLevel) << thisLevelProperties;
  }

  /**
   * Create the workflow nodes from the jobs/subflows defined in the workflow.
   * Instead of storing YamlJobs/YamlWorkflows themselves, store as maps in order to simplify
   * yaml output.
   * Do not include StartJobs and SubFlowJobs because they aren't needed in Flow 2.0
   * Still include LaunchJobs because the DAG engine still requires them - they may not be needed
   * in the future.
   *
   * @param workflow Workflow being constructed
   * @return result List of all nodes converted to String Maps
   */
  private static List buildNodes(Workflow workflow) {
    List result = [];

    // Add all jobs except StartJobs and SubFlowJobs
    workflow.jobsToBuild.each { Job job ->
      if (job.class != StartJob.class && job.class != SubFlowJob.class) {
        result.add((new YamlJob(job, workflow.scope)).yamlize());
      }
    }
    // Add all subflows
    workflow.flowsToBuild.each { Workflow subflow ->
      result.add((new YamlWorkflow(subflow, true)).yamlize());
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

  /**
   * @return Map detailing exactly what should be printed in Yaml
   * will not include name, type, dependsOn, config, or nodes if it is false
   * (i.e. dependsOn not defined)
   */
  @Override
  Map yamlize() {
    Map result = [:];
    def addToMapIfNotNull = { val, valName ->
      if (val) {
        result.put(valName, val);
      }
    };
    addToMapIfNotNull(name, "name");
    addToMapIfNotNull(type, "type");
    addToMapIfNotNull(dependsOn, "dependsOn");
    addToMapIfNotNull(config, "config");
    addToMapIfNotNull(nodes, "nodes");
    return result;
  }
}
