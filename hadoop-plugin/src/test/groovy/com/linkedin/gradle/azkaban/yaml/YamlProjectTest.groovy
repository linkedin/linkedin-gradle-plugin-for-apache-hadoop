package com.linkedin.gradle.azkaban.yaml;

import org.junit.Test;

import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_FLOW_VERSION;
import static org.junit.Assert.assertEquals;

class YamlProjectTest {
  @Test
  public void TestYamlProject() {
    Map yamlizedProject = new YamlProject().yamlize();

    assertEquals(AZK_FLOW_VERSION, (float) yamlizedProject["Azkaban-Flow-Version"], 0);
  }
}
