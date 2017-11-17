package com.linkedin.gradle.azkaban.yaml;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

class YamlProjectTest {
  @Test
  public void TestYamlProject() {
    Map yamlizedProject = new YamlProject().yamlize();

    assertEquals(2.0, (float) yamlizedProject["Azkaban-Flow-Version"], 0);
  }
}
