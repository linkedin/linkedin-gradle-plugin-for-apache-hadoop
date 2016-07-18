
package com.linkedin.gradle.hadoopValidator.PigValidator

import org.junit.Test
import static org.junit.Assert.assertArrayEquals;

public class PigDataValidatorTest {

  @Test
  public void testDataExtract() {

    File testFile = new File("src/test/pig/TestPigDataValidator1.pig")

    ArrayList<Tuple> result = PigDataValidator.extractData(testFile)
    ArrayList<Tuple> exp_result = new ArrayList<Tuple>([
        new Tuple('./student_data4.txt', 2),
        new Tuple('./student_data2.txt', 3)])

    assertArrayEquals(exp_result.toArray(),result.toArray())

    testFile = new File("src/test/pig/TestPigDataValidator2.pig")
    result = PigDataValidator.extractData(testFile)
    exp_result = new ArrayList<Tuple>([new Tuple('x.file', 20),
                                       new Tuple('x.file', 21),
                                       new Tuple('myfile.txt',22),
                                       new Tuple('xxx',23),
                                       new Tuple('myfile.txt',24),
                                       new Tuple('/data/intermediate/pow/elcarobootstrap/account/full/weekly/data',25),
                                       new Tuple('xxx',26),
                                       new Tuple('x.file',30),
                                       new Tuple('x',43)])

    assertArrayEquals(exp_result.toArray(),result.toArray())
  }
}
