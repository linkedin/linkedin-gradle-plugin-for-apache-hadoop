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
package com.linkedin.hadoop.example;

import com.linkedin.hadoop.example.minicluster.MapReduceClusterTestBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Hadoop mini-cluster unit tests for the Java map-reduce word count job.
 */
public class WordCountJobTest extends MapReduceClusterTestBase {

  private static final Logger _logger = Logger.getLogger(WordCountJobTest.class);
  private Path _inputPath = new Path("/input");
  private Path _outputPath = new Path("/output");

  public WordCountJobTest() throws IOException {
    super();
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
  }

  @BeforeMethod
  public void beforeMethod(Method method) throws IOException {
    _logger.info("*** Cleaning input and output paths");
    getFileSystem().delete(_inputPath, true);
    getFileSystem().mkdirs(_inputPath);
    getFileSystem().delete(_outputPath, true);
  }

  /**
   * Helper method to read the word counts output file from the given path on HDFS.
   *
   * @param outputPath The path on HDFS to the word counts output file
   * @return The word counts as an ordered map of word to count
   * @throws Exception If there is a problem while reading the word counts
   */
  private Map<String, Long> readWordCounts(Path outputPath) throws Exception {
    FileSystem fileSystem = getFileSystem();
    BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(outputPath)));
    Map<String, Long> wordCounts = new LinkedHashMap<>();

    try {
      String line = reader.readLine();
      while (line != null) {
        int tab = line.indexOf('\t');
        if (tab == -1) {
          throw new Exception(String.format("No tab delimiter found in the line: %s", line));
        }

        String word = line.substring(0, tab);
        long count = Long.parseLong(line.substring(tab + 1));
        if (wordCounts.containsKey(word)) {
          throw new Exception(String.format("Found repeated instance of the word: %s", word));
        }

        wordCounts.put(word, count);
        line = reader.readLine();
      }
    } finally {
      reader.close();
    }

    return wordCounts;
  }

  /**
   * Helper method to execute the Hadoop job we are testing.
   *
   * @throws Exception If there is a problem executing the Hadoop job
   */
  private void runJob() throws Exception {
    Properties props = new Properties();
    props.setProperty("input.path", _inputPath.toString());
    props.setProperty("output.path", _outputPath.toString());
    props.setProperty("force.output.overwrite", "true");

    WordCountJob job = new WordCountJob("wordCountJobTest", props);
    job.setConf(getConfiguration());
    job.run();
  }

  /**
   * Simple unit test for this job. The purpose of this test is simply to demonstrate how to write
   * mini-cluster based unit tests.
   *
   * @throws Exception If there is a problem executing the Hadoop job
   */
  @Test
  public void wordCountMapReduceTest() throws Exception {
    // First, write some test data to the input path
    FileSystem fileSystem = getFileSystem();
    OutputStream outputStream = fileSystem.create(new Path(_inputPath, "testFile.txt"));
    outputStream.write("Hello world test hello\nHello again world\nTesting hello world".getBytes());
    outputStream.close();

    // Now run the WordCountJob
    runJob();

    // Then check the results
    Map<String, Long> wordCounts = readWordCounts(new Path(_outputPath, "part-r-00000"));
    Assert.assertEquals(wordCounts.get("Hello"), new Long(2));
    Assert.assertEquals(wordCounts.get("world"), new Long(3));
  }
}