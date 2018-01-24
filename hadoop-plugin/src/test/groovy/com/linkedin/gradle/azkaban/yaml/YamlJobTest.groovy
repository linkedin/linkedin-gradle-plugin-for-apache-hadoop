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
package com.linkedin.gradle.azkaban.yaml;

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
