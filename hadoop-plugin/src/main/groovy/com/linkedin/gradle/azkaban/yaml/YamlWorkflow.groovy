package com.linkedin.gradle.azkaban.yaml;

import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.job.LaunchJob;
import com.linkedin.gradle.hadoopdsl.job.StartJob;

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
class YamlWorkflow {
  String name;
  String type;
  List dependsOn;
  Map config;
  List nodes;

  /**
   * Construct YamlJob from Job and the Job's parent scope
   *
   * @param workflow The workflow to be converted into Yaml
   * @param parentScope The parent scope of the workflow
   * @param subflow Boolean regarding whether or not the workflow is a subflow
   */
  YamlWorkflow(Workflow workflow, NamedScope parentScope, boolean isSubflow) {
    // Include name/type in yaml only if the workflow is a subflow
    name = isSubflow ? workflow.name : null;
    type = isSubflow ? "flow" : null;
    dependsOn = workflow.parentDependencies.toList();
    config = buildConfig(workflow, parentScope, isSubflow);
    nodes = buildNodes(workflow, parentScope);
  }

  /**
   * Create the workflow config from the properties in the namespace
   *
   * @param workflow The workflow being constructed
   * @param parentScope The parent scope of the workflow being constructed
   * @param subflow Boolean regarding whether or not the workflow is a subflow
   * @return result The map of all properties associated with the workflow
   */
  private static Map buildConfig(Workflow workflow, NamedScope parentScope, boolean isSubflow) {
    Map<String, String> result = [:];
    // For the root workflow, add all global properties
    if (!isSubflow) {
      NamedScope oldParentScope = (NamedScope) parentScope.properties["nextLevel"];
      oldParentScope.properties["thisLevel"].each { key, val ->
        // thisLevel contains properties as well as workflows,
        // so make sure to only parse and include Properties objects
        if (val.getClass() == Properties) {
          result << ((Properties) val).buildProperties(oldParentScope)
        }
      }
    }
    // Build all workflow properties after root properties so if same property is defined
    // then the workflow property is selected
    workflow.properties.each { Properties prop ->
      result << prop.buildProperties(parentScope);
    }
    return result;
  }

  /**
   * Create the workflow nodes from the jobs/subflows defined in the workflow
   * Instead of storing YamlJobs/YamlWorkflows themselves, store as maps in order to simplify
   * yaml output
   * Do not include LaunchJobs and StartJobs because they aren't needed in Flow 2.0
   *
   * @param workflow The workflow being constructed
   * @param parentScope The parent scope of the workflow being constructed
   * @return result List of all nodes converted to String Maps
   */
  private static List buildNodes(Workflow workflow, NamedScope parentScope) {
    List result = [];

    // Add all jobs except LaunchJobs and StartJobs
    workflow.jobsToBuild.each { Job job ->
      if (job.class != LaunchJob.class && job.class != StartJob.class) {
        result.add((new YamlJob(job, parentScope)).yamlize());
      }
    }
    // Add all subflows
    workflow.flowsToBuild.each { Workflow subflow ->
      result.add((new YamlWorkflow(subflow, subflow.scope, true)).yamlize());
    }
    // Remove all LaunchJobs and StartJobs from dependencies of other jobs
    workflow.jobsToBuild.each { Job job ->
      if (job.class == LaunchJob.class || job.class == StartJob.class) {
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
   * @return String Map detailing exactly what should be printed in Yaml
   * will not include name, type, dependsOn, config, or nodes if it is false
   * (i.e. dependsOn not defined)
   */
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
