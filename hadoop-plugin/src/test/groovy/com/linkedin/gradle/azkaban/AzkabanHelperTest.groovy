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
package com.linkedin.gradle.azkaban;

import org.gradle.api.GradleException;
import org.json.JSONObject
import org.junit.Rule;
import org.junit.Test
import org.junit.rules.ExpectedException

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*

class AzkabanHelperTest {
  @Test
  public void TestFetchSortedFlows() {
    assertEquals(AzkabanHelper.fetchSortedFlows(new JSONObject("{\"project\" : \"test-azkaban\",   \"projectId\" : 192,   \"flows\" : [ {     \"flowId\" : \"test\"   }, {     \"flowId\" : \"test2\"   } ] }")), ["test", "test2"]);
    assertEquals(AzkabanHelper.fetchSortedFlows(new JSONObject("{}")), []);
  }

  @Test(expected = GradleException.class)
  public void TestGradleException() {
    AzkabanHelper.fetchSortedFlows(new JSONObject("{\"project\" : \"test-azkaban\",   \"projectId\" : 192,   \"flows\" : [ ] }"));
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test(expected = Exception.class)
  public void TestGetConsoleException() throws Exception {
    expectedEx.expec(Exception.class);
    expectedEx.expectMessage(AzkabanHelper.CONSOLE_EXCEPTION_MESSAGE);
    AzkabanHelper.getSystemConsole();
  }

  // Since Console is final class, it requires `mock-maker-inline` extension to run below test.
  // More details: https://github.com/mockito/mockito/wiki/What's-new-in-Mockito-2#unmockable
  // Enabling this extension causes several other test cases failure. Keeping below test case commented for now.
//  @Test
//  void testConsoleSecretInput() {
//    String consoleInputMessage = "Enter test input: "
//    char[] nonEmptyInput = "test".toCharArray()
//    char[] emptyInput = "".toCharArray()
//    Console mockedConsole = mock(Console.class)
//
//    when(mockedConsole.readPassword()).thenReturn(nonEmptyInput)
//    assertEquals(AzkabanHelper.consoleSecretInput(mockedConsole, consoleInputMessage, false, false), nonEmptyInput)
//
//    when(mockedConsole.readPassword()).thenReturn(nonEmptyInput)
//    assertEquals(AzkabanHelper.consoleSecretInput(mockedConsole, consoleInputMessage, false, true), nonEmptyInput)
//
//    when(mockedConsole.readPassword()).thenReturn(emptyInput)
//    assertEquals(AzkabanHelper.consoleSecretInput(mockedConsole, consoleInputMessage, false, false), emptyInput)
//
//    when(mockedConsole.readPassword()).thenReturn(emptyInput, emptyInput, nonEmptyInput)
//    assertEquals(AzkabanHelper.consoleSecretInput(mockedConsole, consoleInputMessage, false, true), nonEmptyInput)
//  }
}
