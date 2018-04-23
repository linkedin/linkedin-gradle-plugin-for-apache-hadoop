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
import com.linkedin.gradle.liazkaban.LiYamlCompiler;
import com.linkedin.gradle.lihadoopdsl.lijob.LiPigBangBangJob
import org.gradle.api.Project;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class YamlBangBangJobTest {
  @Test
  public void TestYamlBangBangJob() {
    Project mockProject = mock(Project.class);
    LiYamlCompiler liYamlCompiler = new LiYamlCompiler(mockProject);
    liYamlCompiler.parentDirectory = "build/tmp"
    NamedScope mockNamedScope = mock(NamedScope.class);
    liYamlCompiler.parentScope = mockNamedScope;
    LiPigBangBangJob mockLiBangBangJob = mock(LiPigBangBangJob.class);
    when(mockLiBangBangJob.name).thenReturn("test");
    when(mockLiBangBangJob.jobProperties).thenReturn(["type": "hadoopShell"]);
    when(mockLiBangBangJob.dependencyNames).thenReturn([].toSet());
    when(mockLiBangBangJob.buildProperties(mockNamedScope)).thenReturn(["type": "hadoopShell"]);

    Map yamlizedJob = liYamlCompiler.yamlizeJob(mockLiBangBangJob);
    assertEquals("test", yamlizedJob["name"]);
    assertEquals("hadoopShell", yamlizedJob["type"]);
    assertFalse(yamlizedJob.containsKey("dependsOn"));
    assertEquals("-Dazkaban.link.workflow.url=\${azkaban.link.workflow.url} " +
            "-Dazkaban.link.execution.url=\${azkaban.link.execution.url} " +
            "-Dazkaban.job.outnodes=\${azkaban.job.outnodes} " +
            "-Dazkaban.link.job.url=\${azkaban.link.job.url} " +
            "-Dazkaban.link.attempt.url=\${azkaban.link.attempt.url} " +
            "-Dazkaban.job.innodes=\${azkaban.job.innodes} ",
            yamlizedJob["config"]["env.PIG_JAVA_OPTS"]);
  }
}
