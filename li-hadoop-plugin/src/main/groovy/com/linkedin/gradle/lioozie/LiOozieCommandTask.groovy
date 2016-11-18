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

import com.linkedin.gradle.liutil.LiKerberosUtil;
import com.linkedin.gradle.oozie.OozieCommandTask;
import com.linkedin.gradle.oozie.OozieService;

class LiOozieCommandTask extends OozieCommandTask {
  /**
   * Make OozieService object with kerberos authentication
   * @param url Url of the oozie system
   * @return OozieService object with kerberos authentication
   */
  @Override
  OozieService makeOozieService(String url) {
    return new OozieService(project, url, LiKerberosUtil.getKrb5File(project));
  }
}
