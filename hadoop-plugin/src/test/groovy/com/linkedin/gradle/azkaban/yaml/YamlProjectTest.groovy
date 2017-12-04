package com.linkedin.gradle.azkaban.yaml;

import org.junit.Test;

import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_FLOW_VERSION;
import static org.junit.Assert.assertEquals;

class YamlProjectTest {
  @Test
  public void TestYamlProject() {
    Map yamlizedProject = new YamlProject().yamlize();

    // The third argument - 0 - refers to the possible difference between two doubles.
    // There should be no difference between AZK_FLOW_VERSION and the yamlProject flow version.
    assertEquals(AZK_FLOW_VERSION, (double) yamlizedProject["Azkaban-Flow-Version"], 0);
  }
}
