/*
 * Copyright 2015 LinkedIn Corp.
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
package com.linkedin.gradle.lioozie;

import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.oozie.OozieDslCompiler;
import org.gradle.api.Project;

/**
 * LinkedIn specific customizations to the OozieDslCompiler
 */
class LiOozieDslCompiler extends OozieDslCompiler {

  LiOozieDslCompiler(Project project) {
    super(project);
  }

  /**
   * Adds linkedin specific properties to the jobProperties.
   *
   * @param props The Properties object to build
   */
  @Override
  void visitProperties(Properties props) {

    if(!props.jobProperties.containsKey("nameNode")) {
      props.jobProperties.put("nameNode","hdfs://eat1-nertznn01.grid.linkedin.com:9000");
    }

    if (!props.jobProperties.containsKey("jobTracker")) {
      props.jobProperties.put("jobTracker","eat1-nertzrm01.grid.linkedin.com:8032");
    }

    super.visitProperties(props);
  }

}
