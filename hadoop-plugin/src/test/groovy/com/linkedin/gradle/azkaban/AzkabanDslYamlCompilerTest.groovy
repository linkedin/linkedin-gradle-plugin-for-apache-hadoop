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

import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.LaunchJob;
import com.linkedin.gradle.hadoopdsl.job.StartJob;
import com.linkedin.gradle.hadoopdsl.job.SubFlowJob;
import org.gradle.api.Project;
import org.junit.Before;
import org.junit.Test;

import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_FLOW_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AzkabanDslYamlCompilerTest {
  NamedScope mockHadoopScope = mock(NamedScope.class);
  NamedScope mockRootScope = mock(NamedScope.class);
  Job mockJob = mock(Job.class);
  NamedScope mockWorkflowScope = mock(NamedScope.class);
  Workflow mockWorkflow = mock(Workflow.class);
  NamedScope mockSubflowScope = mock(NamedScope.class);
  Workflow mockSubflow = mock(Workflow.class);
  AzkabanDslYamlCompiler yamlCompiler;

  @Before
  public void setup() {
    when(mockHadoopScope.nextLevel).thenReturn(mockRootScope);
    when(mockHadoopScope.properties).thenReturn("");
    when(mockRootScope.nextLevel).thenReturn(null);

    when(mockJob.name).thenReturn("testJob");
    when(mockJob.jobProperties).thenReturn(["type": "testJobtype"]);
    when(mockJob.dependencyNames).thenReturn([].toSet());
    when(mockJob.buildProperties(mockWorkflowScope)).thenReturn([:]);

    when(mockWorkflow.name).thenReturn("testFlow");
    when(mockWorkflow.scope).thenReturn(mockWorkflowScope);
    when(mockWorkflow.parentDependencies).thenReturn([].toSet());
    when(mockWorkflow.properties).thenReturn([]);
    when(mockWorkflow.jobsToBuild).thenReturn([]);
    when(mockWorkflow.flowsToBuild).thenReturn([]);
    when(mockWorkflowScope.nextLevel).thenReturn(mockHadoopScope);

    when(mockSubflow.name).thenReturn("subflowTest");
    when(mockSubflow.scope).thenReturn(mockSubflowScope);
    when(mockSubflow.parentDependencies).thenReturn(["testFlow"].toSet());
    when(mockSubflow.properties).thenReturn([]);
    when(mockSubflow.jobsToBuild).thenReturn([]);
    when(mockSubflow.flowsToBuild).thenReturn([]);
    when(mockSubflowScope.nextLevel).thenReturn(mockWorkflowScope);

    Project mockProject = mock(Project.class);
    when(mockProject.name).thenReturn("test");
    yamlCompiler = new AzkabanDslYamlCompiler(mockProject);
    yamlCompiler.parentScope = mockHadoopScope;
  }

  @Test
  public void TestSimpleYamlWorkflow() {
    Map yamlizedWorkflow = yamlCompiler.yamlizeWorkflow(mockWorkflow, false);
    assertFalse(yamlizedWorkflow.containsKey("name"));
    assertFalse(yamlizedWorkflow.containsKey("type"));
    assertFalse(yamlizedWorkflow.containsKey("dependsOn"));
    assertFalse(yamlizedWorkflow.containsKey("config"));
    assertFalse(yamlizedWorkflow.containsKey("nodes"));
  }

  @Test
  public void TestComplicatedYamlWorkflow() {
    Properties props = new Properties("testProperties");
    props.setJobProperty("configKey1", "configVal1");
    props.setJobProperty("configKey2", "configVal2");
    when(mockWorkflow.properties).thenReturn([props]);
    when(mockWorkflow.jobsToBuild).thenReturn([mockJob]);
    when(mockWorkflow.flowsToBuild).thenReturn([mockSubflow]);

    Map yamlizedWorkflow = yamlCompiler.yamlizeWorkflow(mockWorkflow, false);
    assertFalse(yamlizedWorkflow.containsKey("name"));
    assertFalse(yamlizedWorkflow.containsKey("type"));
    assertFalse(yamlizedWorkflow.containsKey("dependsOn"));
    assertEquals(["configKey1": "configVal1", "configKey2": "configVal2"], yamlizedWorkflow["config"]);

    List sortedNodes = ((List) yamlizedWorkflow["nodes"]).sort();
    Map yamlizedSubflow = (Map) sortedNodes[0];
    assertEquals("subflowTest", yamlizedSubflow["name"]);
    assertEquals("flow", yamlizedSubflow["type"]);
    assertEquals(["testFlow"], yamlizedSubflow["dependsOn"]);
    assertFalse(yamlizedSubflow.containsKey("config"));
    assertFalse(yamlizedSubflow.containsKey("nodes"));
    Map yamlizedJob = (Map) sortedNodes[1];
    assertEquals("testJob", yamlizedJob["name"]);
    assertEquals("testJobtype", yamlizedJob["type"]);
    assertFalse(yamlizedJob.containsKey("dependsOn"));
    assertFalse(yamlizedJob.containsKey("config"));
  }

  @Test
  public void TestSubflow() {
    Map yamlizedSubflow = yamlCompiler.yamlizeWorkflow(mockSubflow, true);
    assertEquals("subflowTest", yamlizedSubflow["name"]);
    assertEquals("flow", yamlizedSubflow["type"]);
    assertEquals([mockWorkflow.name], yamlizedSubflow["dependsOn"]);
    assertFalse(yamlizedSubflow.containsKey("config"));
    assertFalse(yamlizedSubflow.containsKey("nodes"));
  }

  @Test
  public void TestAddGlobalProperties() {
    Properties props = new Properties("testProperties");
    props.setJobProperty("globalProp1", "val1");
    props.setJobProperty("globalProp2", "val2");
    when(mockHadoopScope.thisLevel).thenReturn(["globalProps": props]);
    when(mockWorkflow.flowsToBuild).thenReturn([mockSubflow]);

    Map yamlizedWorkflow = yamlCompiler.yamlizeWorkflow(mockWorkflow, false);
    assertFalse(yamlizedWorkflow.containsKey("name"));
    assertFalse(yamlizedWorkflow.containsKey("type"));
    assertFalse(yamlizedWorkflow.containsKey("dependsOn"));
    assertEquals(["globalProp1": "val1", "globalProp2": "val2"], yamlizedWorkflow["config"]);

    Map yamlizedSubflow = (Map) ((List) yamlizedWorkflow["nodes"])[0];
    assertEquals("subflowTest", yamlizedSubflow ["name"]);
    assertEquals("flow", yamlizedSubflow["type"]);
    assertEquals([mockWorkflow.name], yamlizedSubflow["dependsOn"]);
    assertFalse(yamlizedSubflow.containsKey("config"));
    assertFalse(yamlizedSubflow.containsKey("nodes"));
  }

  @Test
  public void TestWorkflowPropertyOverGlobalProperty() {
    Properties globalProps = new Properties("globalProperties");
    globalProps.setJobProperty("sharedProp", "wrongVal");
    when(mockHadoopScope.thisLevel).thenReturn(["globalProps": globalProps]);
    Properties workflowProps = new Properties("workflowProperties");
    workflowProps.setJobProperty("sharedProp", "correctVal");
    when(mockWorkflow.properties).thenReturn([workflowProps]);

    Map yamlizedWorkflow = yamlCompiler.yamlizeWorkflow(mockWorkflow, false);
    assertFalse(yamlizedWorkflow.containsKey("name"));
    assertFalse(yamlizedWorkflow.containsKey("type"));
    assertFalse(yamlizedWorkflow.containsKey("dependsOn"));
    assertEquals(["sharedProp": "correctVal"], yamlizedWorkflow["config"]);
    assertFalse(yamlizedWorkflow.containsKey("nodes"));
  }

  @Test
  public void TestWorkflowPropertyNestedGlobalProperty() {
    NamedScope mockParentScope = mock(NamedScope.class);
    Properties parentProps = new Properties("parentProperties");
    parentProps.setConfProperty("parentProp", "parentVal");
    when(mockParentScope.thisLevel).thenReturn(["parentProps": parentProps]);
    when(mockParentScope.nextLevel).thenReturn(mockHadoopScope);

    Properties globalProps = new Properties("globalProperties");
    globalProps.setJobProperty("globalProp", "globalVal");
    globalProps.setConfProperty("parentProp", "wrongParentVal");
    when(mockHadoopScope.thisLevel).thenReturn(["globalProps": globalProps]);
    Properties workflowProps = new Properties("workflowProperties");
    workflowProps.setJobProperty("workflowProp", "workflowVal");
    when(mockWorkflow.properties).thenReturn([workflowProps]);
    when(mockWorkflowScope.nextLevel).thenReturn(mockParentScope);

    Map yamlizedWorkflow = yamlCompiler.yamlizeWorkflow(mockWorkflow, false);
    Map expectedConfig = ["workflowProp": "workflowVal",
                          "hadoop-inject.parentProp": "parentVal",
                          "globalProp": "globalVal"];
    assertFalse(yamlizedWorkflow.containsKey("name"));
    assertFalse(yamlizedWorkflow.containsKey("type"));
    assertFalse(yamlizedWorkflow.containsKey("dependsOn"));
    assertEquals(expectedConfig, yamlizedWorkflow["config"]);
    assertFalse(yamlizedWorkflow.containsKey("nodes"));
  }

  @Test
  public void TestRemoveStartAndSubflowJobs() {
    StartJob startJob = new StartJob("startJob");
    SubFlowJob subFlowJob = new SubFlowJob("subFlowJob");
    Job mockJobTwo = mock(Job.class);
    when(mockJobTwo.name).thenReturn("testJobTwo");
    when(mockJobTwo.jobProperties).thenReturn(["type": "testJobtype"]);
    when(mockJobTwo.dependencyNames).thenReturn([].toSet());
    when(mockJobTwo.buildProperties(mockHadoopScope)).thenReturn([:]);
    LaunchJob launchJob = new LaunchJob("launchJob"); // Make sure LaunchJob is not deleted
    launchJob.dependencyNames = [mockJob.name];

    when(mockJob.dependencyNames).thenReturn([startJob.name, subFlowJob.name,
                                              "testJobTwo"].toSet());
    when(mockWorkflow.jobsToBuild).thenReturn([startJob, subFlowJob, mockJobTwo, mockJob,
                                               launchJob]);

    Map yamlizedWorkflow = yamlCompiler.yamlizeWorkflow(mockWorkflow, false);
    assertFalse(yamlizedWorkflow.containsKey("name"));
    assertFalse(yamlizedWorkflow.containsKey("type"));
    assertFalse(yamlizedWorkflow.containsKey("dependsOn"));
    assertFalse(yamlizedWorkflow.containsKey("config"));
    assertEquals(3, ((List) yamlizedWorkflow["nodes"]).size());

    List sortedNodes = ((List) yamlizedWorkflow["nodes"]).sort();
    Map yamlizedJobTwo = (Map) sortedNodes[0];
    assertEquals("testJobTwo", yamlizedJobTwo["name"]);
    assertEquals("testJobtype", yamlizedJobTwo["type"]);
    assertFalse(yamlizedJobTwo.containsKey("dependsOn"));
    assertFalse(yamlizedJobTwo.containsKey("config"));
    Map yamlizedLaunchJob = (Map) sortedNodes[1];
    assertEquals("launchJob", yamlizedLaunchJob["name"]);
    assertEquals("noop", yamlizedLaunchJob["type"]);
    assertEquals(["testJob"], yamlizedLaunchJob["dependsOn"]);
    assertFalse(yamlizedLaunchJob.containsKey("config"));
    Map yamlizedJob = (Map) sortedNodes[2];
    assertEquals("testJob", yamlizedJob["name"]);
    assertEquals("testJobtype", yamlizedJob["type"]);
    assertEquals([mockJobTwo.name], yamlizedJob["dependsOn"]);
    assertFalse(yamlizedJob.containsKey("config"));
  }

  @Test
  public void TestSimpleYamlJob() {
    NamedScope mockNamedScope = mock(NamedScope.class);
    Job mockJob = mock(Job.class);
    when(mockJob.name).thenReturn("test");
    when(mockJob.jobProperties).thenReturn(["type": "testJobtype"]);
    when(mockJob.dependencyNames).thenReturn([].toSet());
    when(mockJob.buildProperties(mockNamedScope)).thenReturn([:]);

    Map yamlizedJob = yamlCompiler.yamlizeJob(mockJob);
    assertEquals("test", yamlizedJob["name"]);
    assertEquals("testJobtype", yamlizedJob["type"]);
    assertFalse(yamlizedJob.containsKey("dependsOn"));
    assertFalse(yamlizedJob.containsKey("config"));
  }

  @Test
  public void TestComplicatedYamlJob() {
    Job mockJob = mock(Job.class);
    when(mockJob.name).thenReturn("test");
    when(mockJob.jobProperties).thenReturn(["type": "testJobtype"]);
    when(mockJob.dependencyNames).thenReturn(["dependency1", "dependency2"].toSet());
    when(mockJob.buildProperties(yamlCompiler.parentScope)).thenReturn(["configKey1" : "configVal1",
                                                                        "configKey2": "configVal2"]);

    Map yamlizedJob = yamlCompiler.yamlizeJob(mockJob);
    assertEquals("test", yamlizedJob["name"]);
    assertEquals("testJobtype", yamlizedJob["type"]);
    assertEquals(["dependency1", "dependency2"], ((List) yamlizedJob["dependsOn"]).sort());
    assertEquals(["configKey1": "configVal1", "configKey2": "configVal2"], yamlizedJob["config"]);
  }

  @Test
  public void TestYamlProject() {
    Map yamlizedProject = yamlCompiler.yamlizeProject();
    // Check yamlized project returns expected version.
    // The third argument - 0 - refers to the possible difference between two doubles.
    // There should be no difference between AZK_FLOW_VERSION and the yamlProject flow version.
    assertEquals(AZK_FLOW_VERSION, (double) yamlizedProject["azkaban-flow-version"], 0);
    // Check yamlCompiler picking up expected project name.
    assertEquals("test", yamlCompiler.yamlProjectName);
  }
}
