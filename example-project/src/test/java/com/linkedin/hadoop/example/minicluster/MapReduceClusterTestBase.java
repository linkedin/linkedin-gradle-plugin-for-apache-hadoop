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
package com.linkedin.hadoop.example.minicluster;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapred.MiniMRClientCluster;
import org.apache.hadoop.mapred.MiniMRClientClusterFactory;
import org.apache.log4j.Logger;


/**
 * Base class for testing MapReduce jobs using mini-cluster.
 */
public class MapReduceClusterTestBase {

  private static final Logger _logger = Logger.getLogger(MapReduceClusterTestBase.class);
  private int _dataNodes;
  private MiniDFSCluster _dfsCluster;
  private FileSystem _fileSystem;
  private MiniMRClientCluster _mrCluster;

  /**
   * Constructor for the MapReduceClusterTestBase class that initializes the mini-cluster with one
   * HDFS data node.
   */
  public MapReduceClusterTestBase() {
    this(1);
  }

  /**
   * Constructor for the MapReduceClusterTestBase class that initializes the mini-cluster with the
   * give number of HDFS data nodes.
   *
   * @param dataNodes The number of HDFS data nodes in the mini-cluster
   */
  public MapReduceClusterTestBase(int dataNodes) {
    if (dataNodes < 1) {
      throw new IllegalArgumentException("Invalid dataNodes value, must be greater than 0");
    }
    this._dataNodes = dataNodes;
  }

  /**
   * Shuts down the mini-cluster after the unit tests are finished.
   *
   * @throws Exception If there is a problem shutting down the mini-cluster
   */
  public void afterClass() throws Exception {
    if (_mrCluster != null) {
      _logger.info("*** Shutting down Mini MR Cluster");
      _mrCluster.stop();
      _mrCluster = null;
    }
    if (_dfsCluster != null) {
      _logger.info("*** Shutting down Mini DFS Cluster");
      _dfsCluster.shutdown();
      _dfsCluster = null;
    }
  }

  /**
   * Initializes the mini-cluster to use with unit tests.
   *
   * @throws Exception If there is a problem initializing the mini-cluster
   */
  public void beforeClass() throws Exception {
    // Make sure the log folder exists or your tests will fail
    new File("test-logs").mkdirs();
    System.setProperty("hadoop.log.dir", "test-logs");

    // Setup a new Configuration for the cluster
    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");

    _logger.info("*** Starting Mini DFS Cluster");
    _dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(_dataNodes).build();
    _fileSystem = _dfsCluster.getFileSystem();

    _logger.info("*** Starting Mini MR Cluster");
    _mrCluster = MiniMRClientClusterFactory.create(this.getClass(), _dataNodes, conf);
  }

  /**
   * Returns the job configuration for the mini-cluster.
   *
   * @return The job configuration for the mini-cluster
   */
  protected Configuration getConfiguration() throws IOException {
    return _mrCluster.getConfig();
  }

  /**
   * Returns the FileSystem instance in use by the mini-cluster.
   *
   * Test cases should use this FileSystem instance for any operations involving HDFS as it is
   * configured correctly for the mini-cluster.
   *
   * @return The FileSystem instance in use by the mini-cluster
   */
  protected FileSystem getFileSystem() {
    return _fileSystem;
  }
}