package com.linkedin.gradle.hadoopValidator.PigValidator


import org.junit.Test
import static org.junit.Assert.assertArrayEquals;

class PigDependencyValidatorTest {

  @Test
  public void testDependencyExtract() {

    String fileText = "--D = load './student_data.txt' USING PigStorage(',') as (id : int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray);\n" +
        "D = load './student_data4.txt' USING PigStorage(',') as (id : int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray);\n" +
        "RegisTer hdfs://abracadabra:9000/student_data.jar ;register ivy://com.linkedin.pig:pig:0.15.0.14;\n" +
        "B = load './student_data2.txt' USING PigStorage(',') as (id:int, firstname:chararray, lastname :chararray, phone:chararray, city:chararray);\n" +
        "RegisTer file:/abracadabra/student_data.jar;\n" +
        "C = UNiON D , B;/*vsfvsfv*sfvsf*vfdvb*/\n" +
        "regISTER fooBar.py;";

    File testFile = new File(System.getProperty("java.io.tmpdir"), "testFoo.pig")
    if (testFile.exists()) {
      testFile.deleteOnExit()
    }
    testFile.write(fileText)

    ArrayList<Tuple> result = PigDependencyValidator.extractDependencies(testFile).collect {it
      new Tuple(it.filename,it.lineno,it.pathType,it.uri,it.reg_command)
    }

    ArrayList<Tuple> exp_result = new ArrayList<Tuple>([
        new Tuple("student_data.jar",3,Dependency.PathType.HDFS,"webhdfs://abracadabra:50070","RegisTer hdfs://abracadabra:9000/student_data.jar"),
        new Tuple("com.linkedin.pig:pig:0.15.0.14",3,Dependency.PathType.REPO,null,"register ivy://com.linkedin.pig:pig:0.15.0.14"),
        new Tuple("/abracadabra/student_data.jar",5,Dependency.PathType.LOCAL_ABSOLUTE,null,"RegisTer file:/abracadabra/student_data.jar"),
        new Tuple("fooBar.py",7,Dependency.PathType.LOCAL_RELATIVE,null,"regISTER fooBar.py")]
    )
    assertArrayEquals(exp_result.toArray(),result.toArray())
  }
}
