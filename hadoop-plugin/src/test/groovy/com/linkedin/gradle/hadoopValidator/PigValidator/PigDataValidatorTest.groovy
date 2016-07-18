
package com.linkedin.gradle.hadoopValidator.PigValidator

import org.junit.Test
import static org.junit.Assert.assertArrayEquals;

public class PigDataValidatorTest {

  @Test
  public void testDataExtract() {

    String fileText = "--D = load './student_data.txt' USING PigStorage(',') as (id : int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray);\n" +
        "D = load './student_data4.txt' USING PigStorage(',') as (id : int, firstname:chararray, lastname:chararray, phone:chararray, city:chararray);\n" +
        "B = load './student_data2.txt' USING PigStorage(',') as (id:int, firstname:chararray, lastname :chararray, phone:chararray, city:chararray);\n" +
        "C = UNiON D , B;"
    File testFile = new File(System.getProperty("java.io.tmpdir"), "testFoo.pig")
    if(testFile.exists()){
      testFile.deleteOnExit()
    }
    testFile.write(fileText)

    ArrayList<Tuple> result = PigDataValidator.extractData(testFile)
    ArrayList<Tuple> exp_result = new ArrayList<Tuple>([
        new Tuple('./student_data4.txt', 2, "load './student_data4.txt'"),
        new Tuple('./student_data2.txt', 3, "load './student_data2.txt'")])

    assertArrayEquals(exp_result.toArray(),result.toArray())


    fileText = "/*\n" + " * Licensed to the Apache Software Foundation (ASF) under one\n" +
        " * or more contributor license agreements.  See the NOTICE file\n" +
        " * distributed with this work for additional information\n" +
        " * regarding copyright ownership.  The ASF licenses this file\n" +
        " * to you under the Apache License, Version 2.0 (the\n" +
        " * \"License\"); you may not use this file except in compliance\n" +
        " * with the License.  You may obtain a copy of the License at\n" + " *\n" +
        " *     http://www.apache.org/licenses/LICENSE-2.0\n" + " *\n" +
        " * Unless required by applicable law or agreed to in writing, software\n" +
        " * distributed under the License is distributed on an \"AS IS\" BASIS,\n" +
        " * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n" +
        " * See the License for the specific language governing permissions and\n" +
        " * limitations under the License.\n" + " */\n" + "\n" + "--Load\n" +
        "A = load 'x.file' using org.apache.pig.PigStorage ( ',' ) as ( a:int, b:long );\n" +
        "A = load 'x.file' as c:int;\n" + "A=LoAD 'myfile.txt' using PigTextLoader() as ( a:int, b, c );\n" +
        "A = load 'xxx' as ( a : INT, b : LONG );\n" +
        "A = LOAD   'myfile.txt' USING PigTextLoader( 'arg1', 'arg2' ) as ( a:INT, b : long, c : bytearray, d : chararray, e : float, f : double);\n" +
        "aa = load '/data/intermediate/pow/elcarobootstrap/account/full/weekly/data' using org.apache.pig.PigStorage('\\n');\n" +
        "A = load 'xxx' as (a:int, b:long, c:bag{});\n" + "\n" + "--Filter\n" +
        "B = FILTER A by \$0 == 100 OR \$0 < 5 parallel 20;\n" + "B = FILTER ( load 'x.file' as c:int ) by c == 40;\n" +
        "bb = filter aa by \$4 eq '' or \$4 eq 'NULL' or \$4 eq 'ss' parallel 400;\n" +
        "B = filter A by NOT( ( a  > 5 ) OR ( b < 100 AND a < -1 ) AND d matches 'abc' );\n" +
        "inactiveAccounts = filter a by (\$1 neq '') and (\$1 == '2') parallel 400;\n" + "\n" + "--Distinct\n" +
        "C = DISTINCT B parallel 10;\n" + "C = DISTINCT B partition by org.apache.pig.RandomPartitioner;\n" + "\n" +
        "--Foreach\n" + "D = foreach bb { generate \$0,\$12,\$7; }\n" + "D = foreach bb { generate \$0,\$12,\$7; };\n" +
        "D = foreach C generate \$0;\n" +
        "D = foreach (load 'x' as (a:bag{}, b:chararray, c:int) ) { E = c; S = order a by \$0; generate \$1, COUNT( S ); }\n" +
        "countInactiveAcct = foreach grpInactiveAcct { generate COUNT( inactiveAccounts ); }\n" +
        "E = foreach A generate a as b:int;\n" + "I = foreach A generate flatten(c);\n" + "\n" + "--sample\n" +
        "E = sample D 0.9;\n" + "\n" + "--limit\n" + "F = limit E 100;\n" + "\n" + "--order by\n" +
        "G = ORDER F by \$2;\n" + "G = order F by * DESC;\n" + "E = order B by \$0 ASC;\n" + "\n" + "--define\n" +
        "define myudf org.apache.pig.TextLoader( 'test', 'data' );\n" + "define CMD `ls -l`;\n" + "\n" + "--group\n" +
        "D = cogroup A by \$0 inner, B by \$0 outer;\n" + "grpInactiveAcct = group inactiveAccounts all;\n" +
        "B = GROUP A ALL using 'collected';\n" + "\n" + "--cube\n" + "C = CUBE A BY CUBE(a, b);\n" +
        "CC = CUBE A BY ROLLUP(*);\n" + "\n" + "--join\n" + "E = join A by \$0, B by \$0 using 'replicated';\n" +
        "H = join A by u, B by u;\n" + "I = foreach H generate A::u, B::u;\n" + "\n" + "--croos\n" +
        "F = Cross A, B;\n" + "\n" + "--store\n" + "store C into 'output.txt';\n" +
        "store countInactiveAcct into '/user/kaleidoscope/pow_stats/20080228/acct_stats/InactiveAcctCount';\n" +
        "store inactiveAccounts into '/user/kaleidoscope/pow_stats/20080228/acct/InactiveAcct';\n" + "\n" +
        "--split\n" + "Split A into X if \$0 > 0, Y if \$0 == 0;\n" + "\n" + "--union\n" +
        "H = union onschema A, B;\n" + "\n" + "--stream\n" + "C = stream A through CMD;\n" + "\n" + "\n" + "--rank\n" +
        "\n" + "R = rank A;\n" + "R = rank A by a;\n" + "R = rank A by a DESC;\n" + "R = rank A by a DESC, b;"


    testFile.write(fileText)

    result = PigDataValidator.extractData(testFile)
    exp_result = new ArrayList<Tuple>([new Tuple('x.file', 20,"load 'x.file'"),
                                       new Tuple('x.file', 21,"load 'x.file'"),
                                       new Tuple('myfile.txt',22,"LoAD 'myfile.txt'"),
                                       new Tuple('xxx',23,"load 'xxx'"),
                                       new Tuple('myfile.txt',24,"LOAD   'myfile.txt'"),
                                       new Tuple('/data/intermediate/pow/elcarobootstrap/account/full/weekly/data',25,"load '/data/intermediate/pow/elcarobootstrap/account/full/weekly/data'"),
                                       new Tuple('xxx',26,"load 'xxx'"),
                                       new Tuple('x.file',30,"load 'x.file'"),
                                       new Tuple('x',43,"load 'x'")])

    assertArrayEquals(exp_result.toArray(),result.toArray())
  }
}