package com.linkedin.gradle.azkaban.yaml

import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.job.Job;
import org.junit.Test;

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class YamlJobTest {
  @Test
  public void TestSimpleYamlJob() {
    NamedScope mockNamedScope = mock(NamedScope.class);
    Job mockJob = mock(Job.class);
    when(mockJob.name).thenReturn("test");
    when(mockJob.jobProperties).thenReturn(["type": "testJobtype"]);
    when(mockJob.dependencyNames).thenReturn([].toSet());
    when(mockJob.buildProperties(mockNamedScope)).thenReturn([:]);

    Map yamlizedJob = new YamlJob(mockJob, mockNamedScope).yamlize();
    assertEquals("test", yamlizedJob["name"]);
    assertEquals("testJobtype", yamlizedJob["type"]);
    assertFalse(yamlizedJob.containsKey("dependsOn"));
    assertFalse(yamlizedJob.containsKey("config"));
  }

  @Test
  public void TestComplicatedYamlJob() {
    NamedScope mockNamedScope = mock(NamedScope.class);
    Job mockJob = mock(Job.class);
    when(mockJob.name).thenReturn("test");
    when(mockJob.jobProperties).thenReturn(["type": "testJobtype"]);
    when(mockJob.dependencyNames).thenReturn(["dependency1", "dependency2"].toSet());
    when(mockJob.buildProperties(mockNamedScope)).thenReturn(["configKey1" : "configVal1",
                                                              "configKey2": "configVal2"]);

    Map yamlizedJob = new YamlJob(mockJob, mockNamedScope).yamlize();
    assertEquals("test", yamlizedJob["name"]);
    assertEquals("testJobtype", yamlizedJob["type"]);
    assertEquals(["dependency1", "dependency2"], ((List) yamlizedJob["dependsOn"]).sort());
    assertEquals(["configKey1": "configVal1", "configKey2": "configVal2"], yamlizedJob["config"]);
  }
}
