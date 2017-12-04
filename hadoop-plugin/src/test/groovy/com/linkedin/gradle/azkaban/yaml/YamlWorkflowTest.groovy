package com.linkedin.gradle.azkaban.yaml;

import com.linkedin.gradle.hadoopdsl.NamedScope
import com.linkedin.gradle.hadoopdsl.Properties
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.Job
import com.linkedin.gradle.hadoopdsl.job.LaunchJob
import com.linkedin.gradle.hadoopdsl.job.StartJob;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class YamlWorkflowTest {

  NamedScope mockNamedScope = mock(NamedScope.class);
  NamedScope mockRootNamedScope = mock(NamedScope.class);
  Job mockJob = mock(Job.class);
  Workflow mockWorkflow = mock(Workflow.class);
  Workflow mockSubflow = mock(Workflow.class);

  @Before
  public void setup() {
    when(mockNamedScope.nextLevel).thenReturn(mockRootNamedScope);
    when(mockRootNamedScope.properties).thenReturn("");

    when(mockJob.name).thenReturn("testJob");
    when(mockJob.jobProperties).thenReturn(["type": "testJobtype"]);
    when(mockJob.dependencyNames).thenReturn([].toSet());
    when(mockJob.buildProperties(mockNamedScope)).thenReturn([:]);

    when(mockWorkflow.name).thenReturn("testFlow");
    when(mockWorkflow.parentDependencies).thenReturn([].toSet());
    when(mockWorkflow.properties).thenReturn([]);
    when(mockWorkflow.jobsToBuild).thenReturn([]);
    when(mockWorkflow.flowsToBuild).thenReturn([]);

    when(mockSubflow.name).thenReturn("subflowTest");
    when(mockSubflow.parentDependencies).thenReturn(["testFlow"].toSet());
    when(mockSubflow.properties).thenReturn([]);
    when(mockSubflow.jobsToBuild).thenReturn([]);
    when(mockSubflow.flowsToBuild).thenReturn([]);
  }

  @Test
  public void TestSimpleYamlWorkflow() {
    Map yamlizedWorkflow = new YamlWorkflow(mockWorkflow, mockNamedScope, false).yamlize();
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

    Map yamlizedWorkflow = new YamlWorkflow(mockWorkflow, mockNamedScope, false).yamlize();
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
    Map yamlizedSubflow = new YamlWorkflow(mockSubflow, mockNamedScope, true).yamlize();
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
    when(mockRootNamedScope.thisLevel).thenReturn(["globalProps": props]);
    when(mockWorkflow.flowsToBuild).thenReturn([mockSubflow]);

    Map yamlizedWorkflow = new YamlWorkflow(mockWorkflow, mockNamedScope, false).yamlize();
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
    when(mockRootNamedScope.thisLevel).thenReturn(["globalProps": globalProps]);
    Properties workflowProps = new Properties("workflowProperties");
    workflowProps.setJobProperty("sharedProp", "correctVal");
    when(mockWorkflow.properties).thenReturn([workflowProps]);

    Map yamlizedWorkflow = new YamlWorkflow(mockWorkflow, mockNamedScope, false).yamlize();
    assertFalse(yamlizedWorkflow.containsKey("name"));
    assertFalse(yamlizedWorkflow.containsKey("type"));
    assertFalse(yamlizedWorkflow.containsKey("dependsOn"));
    assertEquals(["sharedProp": "correctVal"], yamlizedWorkflow["config"]);
    assertFalse(yamlizedWorkflow.containsKey("nodes"));
  }

  @Test
  public void TestRemoveLaunchAndStartJobs() {
    StartJob startJob = new StartJob("startJob");
    LaunchJob launchJob = new LaunchJob("launchJob");
    Job mockJobTwo = mock(Job.class);
    when(mockJobTwo.name).thenReturn("testJobTwo");
    when(mockJobTwo.jobProperties).thenReturn(["type": "testJobtype"]);
    when(mockJobTwo.dependencyNames).thenReturn([].toSet());
    when(mockJobTwo.buildProperties(mockNamedScope)).thenReturn([:]);

    when(mockJob.dependencyNames).thenReturn([startJob.name, launchJob.name, "testJobTwo"].toSet());
    when(mockWorkflow.jobsToBuild).thenReturn([startJob, launchJob, mockJobTwo, mockJob]);

    Map yamlizedWorkflow = new YamlWorkflow(mockWorkflow, mockNamedScope, false).yamlize();
    assertFalse(yamlizedWorkflow.containsKey("name"));
    assertFalse(yamlizedWorkflow.containsKey("type"));
    assertFalse(yamlizedWorkflow.containsKey("dependsOn"));
    assertFalse(yamlizedWorkflow.containsKey("config"));
    assertEquals(2, ((List) yamlizedWorkflow["nodes"]).size());

    List sortedNodes = ((List) yamlizedWorkflow["nodes"]).sort().reverse();
    Map yamlizedJob = (Map) sortedNodes[0];
    assertEquals("testJob", yamlizedJob["name"]);
    assertEquals("testJobtype", yamlizedJob["type"]);
    assertEquals([mockJobTwo.name], yamlizedJob["dependsOn"]);
    assertFalse(yamlizedJob.containsKey("config"));
    Map yamlizedJobTwo = (Map) sortedNodes[1];
    assertEquals("testJobTwo", yamlizedJobTwo["name"]);
    assertEquals("testJobtype", yamlizedJobTwo["type"]);
    assertFalse(yamlizedJobTwo.containsKey("dependsOn"));
    assertFalse(yamlizedJobTwo.containsKey("config"));
  }
}
