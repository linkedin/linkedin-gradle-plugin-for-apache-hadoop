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
package com.linkedin.gradle.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

class AzkabanHelperTest {

  @Test
  public void TestEpochToDate() {
    assertEquals(AzkabanClientUtil.epochToDate("-1"), "-");
  }

  @Test
  public void TestGetElapsedTime() {
    assertEquals(AzkabanClientUtil.getElapsedTime("-1", "anything"), "-");
    assertNotEquals(AzkabanClientUtil.getElapsedTime("1476786100", "-1"), "-");
    assertEquals(AzkabanClientUtil.getElapsedTime("1476786100000", "1476786100000"), "0 Sec");
    assertEquals(AzkabanClientUtil.getElapsedTime("1476786100000", "1476786238000"), "2 Min 18 Sec");
    assertEquals(AzkabanClientUtil.getElapsedTime("1476776100000", "1476786238000"), "2 Hr 48 Min 58 Sec");
  }
}
