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
package com.linkedin.gradle.validator.pig

import org.junit.Test

import static org.junit.Assert.assertArrayEquals

class PigDependencyValidatorTest {
  @Test
  void testDependencyExtract() {
    File testFile = new File("src/test/pig/TestPigDependencyValidator.pig")

    ArrayList<Tuple> result = PigDependencyValidator.extractDependencies(testFile).collect { it ->
      new Tuple(it.filename, it.lineno, it.pathType, it.uri, it.reg_command)
    }

    ArrayList<Tuple> exp_result = new ArrayList<Tuple>([
        new Tuple("student_data.jar", 3, Dependency.PathType.HDFS, "webhdfs://abracadabra:50070","RegisTer hdfs://abracadabra:9000/student_data.jar"),
        new Tuple("com.linkedin.pig:pig:0.15.0.14", 3, Dependency.PathType.REPO, null, "register ivy://com.linkedin.pig:pig:0.15.0.14"),
        new Tuple("/abracadabra/student_data.jar", 5, Dependency.PathType.LOCAL_ABSOLUTE, null, "RegisTer file:/abracadabra/student_data.jar"),
        new Tuple("fooBar.py", 7, Dependency.PathType.LOCAL_RELATIVE, null, "regISTER fooBar.py")]
    )
    assertArrayEquals(exp_result.toArray(), result.toArray())
  }
}
