package com.linkedin.gradle.azkaban.yaml;

import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Workflow;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class YamlCompilerTest {
  @Test
  public void TestYamlCompiler() {
    NamedScope mockNamedScope = mock(NamedScope.class);
    NamedScope mockRootNamedScope = mock(NamedScope.class);
    when(mockNamedScope.nextLevel).thenReturn(mockRootNamedScope);
    when(mockRootNamedScope.properties).thenReturn("");

    Workflow mockWorkflow = mock(Workflow.class);
    when(mockWorkflow.name).thenReturn("testFlow");
    when(mockWorkflow.parentDependencies).thenReturn([].toSet());
    when(mockWorkflow.properties).thenReturn([]);
    when(mockWorkflow.jobsToBuild).thenReturn([]);
    when(mockWorkflow.flowsToBuild).thenReturn([]);

    YamlCompiler yamlCompiler = new YamlCompiler(mockWorkflow, mockNamedScope);
    assertEquals(new YamlWorkflow(mockWorkflow, mockNamedScope, false).yamlize(),
            yamlCompiler.yamlWorkflow.yamlize());
    assertEquals(new YamlProject().yamlize(), yamlCompiler.yamlProject.yamlize());
  }
}
