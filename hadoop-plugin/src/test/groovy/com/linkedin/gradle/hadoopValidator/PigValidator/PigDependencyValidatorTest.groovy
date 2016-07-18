package com.linkedin.gradle.hadoopValidator.PigValidator


import org.junit.Test
import static org.junit.Assert.assertArrayEquals;

class PigDependencyValidatorTest {

  @Test
  public void testDependencyExtract() {

    File testFile = new File("src/test/pig/TestPigDependencyValidator.pig")

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
