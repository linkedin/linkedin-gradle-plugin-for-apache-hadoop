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
package com.linkedin.hadoop.jobs;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.apache.log4j.Logger;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Hadoop mini-cluster and regular unit testing for HdfsWaitJob.java.
 */
public class HdfsWaitJobTest {

  private static final Logger _log = Logger.getLogger(HdfsWaitJobTest.class);
  private Configuration _conf;
  private int _dataNodes;
  private MiniDFSCluster _dfsCluster;
  private FileSystem _dfsFileSystem;

  /**
   * Constructor for the HdfsWaitJobTest class that initializes the mini-cluster
   * with one HDFS data node.
   */
  public HdfsWaitJobTest() {
    _dataNodes = 1;
  }

  /**
   * Shuts down the mini-cluster after the unit tests are finished.
   *
   * @throws Exception If there is a problem shutting down the mini-cluster
   */
  @AfterClass
  public void afterClass() throws Exception {
    if (_dfsCluster != null) {
      _log.info("*** Shutting down Mini-DFS Cluster");
      _dfsCluster.shutdown();
      _dfsCluster = null;
    }
  }

  /**
   * Initializes the mini-cluster to use with unit tests.
   *
   * @throws Exception If there is a problem initializing the mini-cluster
   */
  @BeforeClass
  public void beforeClass() throws Exception {
    _conf = new Configuration();
    _conf.set("mapreduce.framework.name", "local");
 
    _log.info("*** Starting Mini DFS Cluster");
    _dfsCluster = new MiniDFSCluster.Builder(_conf).numDataNodes(_dataNodes).build();
    _dfsFileSystem = _dfsCluster.getFileSystem();
    _dfsFileSystem.mkdirs(new Path("/testJob/test"));
  }

  /**
   * Unit testing for HdfsWaitJob.java. Tests method checkDirectory to see if
   * it returns the expected values.
   *
   * @throws Exception If there is a problem executing the HdfsWaitJob method
   */
  @Test
  public void testCheckDirectory() throws Exception {
    HdfsWaitJob job = new HdfsWaitJob("job1", null);
    job.setConf(_conf);

    Assert.assertTrue(job.checkDirectory("/testJob", Long.MAX_VALUE, false));
    Assert.assertTrue(job.checkDirectory("/testJob", 60000, false));
    Assert.assertTrue(job.checkDirectory("/testJob", 5000, false));
    Assert.assertTrue(job.checkDirectory("/testJob", 150000, false));
    Assert.assertTrue(job.checkDirectory("/testJob", 0, true));
    Assert.assertTrue(job.checkDirectory("/testJob/test", Long.MAX_VALUE, true));

    Assert.assertFalse(job.checkDirectory("/testJob", 10, false));
    Assert.assertFalse(job.checkDirectory("/testJob", 1, false));
    Assert.assertFalse(job.checkDirectory("/testJob", 0, false));
    Assert.assertFalse(job.checkDirectory("/testJob/test", Long.MAX_VALUE, false));
    Assert.assertFalse(job.checkDirectory("/testJob/test/doesNotExist", Long.MAX_VALUE, true));
  }

  /**
   * Unit testing for hdfsWaitJob.java Test method parseFilePath to see if
   * it returns the expected values.
   *
   * @throws Exception If there is a problem executing the hdfsWaitJob method
   */
  @Test
  public void testParseFilePath() throws Exception {
    HdfsWaitJob job = new HdfsWaitJob("job1", new Properties());

    DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd");
    DateTimeZone timezone = DateTimeZone.forID("America/Los_Angeles");
    String date = dtf.print(DateTime.now(timezone));

    String datePath = "/foo/bar/" + date;
    String deeperDatePath = "/foo/bar/" + date.replace("-", "/");
    String regularPath = "/foo/bar";

    Assert.assertEquals(job.parseFilePath("/foo/bar/%Y-%m-%d"), datePath);
    Assert.assertEquals(job.parseFilePath("/foo/bar/%Y/%m/%d"), deeperDatePath);
    Assert.assertEquals(job.parseFilePath("/foo/bar"), regularPath);
  }

  /**
   * Unit testing for HdfsWaitJob.java. Tests method parseTime to see if
   * it returns the expected values.
   *
   * @throws Exception If there is a problem executing the HdfsWaitJob method
   */
  @Test
  public void testParseTime() throws Exception {
    HdfsWaitJob job = new HdfsWaitJob("job1", null);

    Assert.assertEquals(job.parseTime("4D"), 345600000);
    Assert.assertEquals(job.parseTime("3M 2S"), 182000);
    Assert.assertEquals(job.parseTime("0H"), 0);
    Assert.assertEquals(job.parseTime("17M 1D 5S 2H"), 94625000);
    Assert.assertEquals(job.parseTime("4M 0D 16S"), 256000);
    Assert.assertEquals(job.parseTime("0M 2M 18M 0M"), 1200000);
    Assert.assertEquals(job.parseTime("1S 7D"), 604801000);
    Assert.assertEquals(job.parseTime("99M 84H"), 308340000);
  }
}
