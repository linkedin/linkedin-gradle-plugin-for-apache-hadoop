package com.linkedin.gradle.azkaban.yaml;

import org.junit.Test;

import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_FLOW_VERSION;
import static org.junit.Assert.assertEquals;

class YamlProjectTest {
  @Test
  public void TestYamlProject() {
    YamlProject yamlProject = new YamlProject("testProject");
    Map yamlizedProject = yamlProject.yamlize();

    assertEquals("testProject", yamlProject.name);
    // The third argument - 0 - refers to the possible difference between two doubles.
    // There should be no difference between AZK_FLOW_VERSION and the yamlProject flow version.
    assertEquals(AZK_FLOW_VERSION, (double) yamlizedProject["azkaban-flow-version"], 0);
  }
}
