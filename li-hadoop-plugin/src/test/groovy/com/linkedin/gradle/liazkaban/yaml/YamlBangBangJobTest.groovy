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
package com.linkedin.gradle.liazkaban.yaml;

import com.linkedin.gradle.hadoopdsl.NamedScope
import com.linkedin.gradle.liazkaban.LiAzkabanDslYamlCompiler;
import com.linkedin.gradle.lihadoopdsl.lijob.LiPigBangBangJob
import org.gradle.api.Project;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class YamlBangBangJobTest {
  @Test
  public void TestYamlBangBangJob() {
    Project mockProject = mock(Project.class);
    LiAzkabanDslYamlCompiler liYamlCompiler = new LiAzkabanDslYamlCompiler(mockProject);
    liYamlCompiler.parentDirectory = "build/tmp"
    NamedScope mockNamedScope = mock(NamedScope.class);
    liYamlCompiler.parentScope = mockNamedScope;
    LiPigBangBangJob mockLiBangBangJob = mock(LiPigBangBangJob.class);
    when(mockLiBangBangJob.name).thenReturn("test");
    when(mockLiBangBangJob.jobProperties).thenReturn(["type": "hadoopShell"]);
    when(mockLiBangBangJob.dependencyNames).thenReturn([].toSet());
    when(mockLiBangBangJob.condition).thenReturn("one_success");
    when(mockLiBangBangJob.buildProperties(mockNamedScope)).thenReturn([
        "type": "hadoopShell",
        "configKey2": "configValue2",
        "configKey1": "configValue1",
        "configKey3": "configValue3",
    ]);
    Map yamlizedJob = liYamlCompiler.yamlizeJob(mockLiBangBangJob);
    assertEquals("test", yamlizedJob["name"]);
    assertEquals("hadoopShell", yamlizedJob["type"]);
    assertFalse(yamlizedJob.containsKey("dependsOn"));
    assertEquals("one_success", yamlizedJob["condition"]);
    assertEquals("-Dazkaban.link.workflow.url=\${azkaban.link.workflow.url} " +
            "-Dazkaban.link.execution.url=\${azkaban.link.execution.url} " +
            "-Dazkaban.job.outnodes=\${azkaban.job.outnodes} " +
            "-Dazkaban.link.job.url=\${azkaban.link.job.url} " +
            "-Dazkaban.link.attempt.url=\${azkaban.link.attempt.url} " +
            "-Dazkaban.job.innodes=\${azkaban.job.innodes} ",
            yamlizedJob["config"]["env.PIG_JAVA_OPTS"]);

    assertNotNull(yamlizedJob["config"].get("env.PIG_JAVA_OPTS"))
    yamlizedJob["config"].remove("env.PIG_JAVA_OPTS")
    // Verify sorted older
    assertEquals(yamlizedJob["config"].toString(),
        "[configKey1:configValue1, configKey2:configValue2, configKey3:configValue3]")
  }
}
