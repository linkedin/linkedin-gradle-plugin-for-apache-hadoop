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

class PigDataValidatorTest {
  @Test
  void testDataExtract() {
    File testFile = new File("src/test/pig/TestPigDataValidator1.pig")
    ArrayList<Tuple> result = PigDataValidator.extractData(testFile)
    ArrayList<Tuple> exp_result = new ArrayList<Tuple>([
        new Tuple('./student_data4.txt', 2),
        new Tuple('./student_data2.txt', 3)])
    assertArrayEquals(exp_result.toArray(), result.toArray())

    testFile = new File("src/test/pig/TestPigDataValidator2.pig")
    result = PigDataValidator.extractData(testFile)
    exp_result = new ArrayList<Tuple>([new Tuple('x.file', 20),
                                       new Tuple('x.file', 21),
                                       new Tuple('myfile.txt', 22),
                                       new Tuple('xxx', 23),
                                       new Tuple('myfile.txt', 24),
                                       new Tuple('/data/intermediate/pow/elcarobootstrap/account/full/weekly/data', 25),
                                       new Tuple('xxx', 26),
                                       new Tuple('x.file', 30),
                                       new Tuple('x', 43)])
    assertArrayEquals(exp_result.toArray(), result.toArray())
  }
}
