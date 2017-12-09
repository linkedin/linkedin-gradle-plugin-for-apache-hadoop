package com.linkedin.gradle.azkaban.yaml;

import org.gradle.api.Project;
import org.junit.Test

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

class YamlCompilerTest {
  @Test
  public void TestYamlCompiler() {
    Project mockProject = mock(Project.class);

    YamlCompiler yamlCompiler = new YamlCompiler(mockProject);
    assertEquals(new YamlProject("test").yamlize(), yamlCompiler.yamlProject.yamlize());
  }
}
