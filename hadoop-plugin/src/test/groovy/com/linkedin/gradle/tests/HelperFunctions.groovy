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
package com.linkedin.gradle.tests;

import org.gradle.api.Project;
import org.gradle.api.tasks.bundling.Zip;

/**
 * Class for helper functions for unit tests.
 */
class HelperFunctions {
  /**
   * Helper function to check whether the contents of the zip contain the expected files.
   *
   * @param zipTaskName The name of the Hadoop zip task to execute
   * @param expected The set of file names expected in the zip
   * @return Whether or not the zip contains the expected files
   */
  static boolean checkExpectedZipFiles(Project project, String zipTaskName, Set<String> expected) {
    def zipTask = project.tasks.findByName(zipTaskName);
    zipTask.execute();

    Set<String> actual = new HashSet<String>();

    project.zipTree(((Zip)zipTask).archivePath).getFiles().each { file ->
      String pathName = file.path;
      int testIndex = pathName.indexOf(".zip");
      int rootIndex = pathName.substring(testIndex).indexOf("/") + testIndex;
      actual.add(pathName.substring(rootIndex + 1));
    }

    if (expected.equals(actual)) {
      return true;
    }

    println("Zip from ${zipTaskName} expected to contain: ${expected.sort()}, but actually contained: ${actual.sort()}");
    return false;
  }

  /**
   * Helper function to create sample test files.
   *
   * @param dir The directory in which to add the files
   * @param ext The file extension
   * @param number The number of files to create
   */
  static void createFilesForTesting(String dir, String ext, int number) {
    number.times { int i ->
      new File("${dir}/sample${i}.${ext}").withWriter { writer ->
        writer.print("blah");
      }
    }
  }  
}
