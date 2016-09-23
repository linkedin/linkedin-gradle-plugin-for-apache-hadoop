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
package com.linkedin.gradle.lihadoopValidator;

import com.linkedin.gradle.hdfs.HdfsFileSystem;

import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Path;
import org.gradle.api.Project;

/**
 * HDFS file system abstraction for the Hadoop Plugin that uses WebHDFS.
 */
class LiHdfsFileSystem extends HdfsFileSystem {

  // Designation to specify the latest subdirectory, as in /data/derived/member/summary/#LATEST.
  String LATEST = "#LATEST";

  LiHdfsFileSystem(Project project) {
    super(project);
  }

  LiHdfsFileSystem(Project project, File krb5Conf) {
    super(project, krb5Conf);
  }

  String getLatestPath(String inputPath) {

    assert (inputPath != null && !inputPath.isEmpty()): "The path to resolve is empty";
    def split = convertToSlashes(inputPath).split(LATEST);
    def retval = split[0];
    for (int i = 1; i < split.size(); i++) {
      retval = getLatestPathHelper(retval) + split[i];
    }
    if (inputPath.endsWith(LATEST)) {
      retval = getLatestPathHelper(retval);
    }
    return retval;
  }

  /**
   * For paths ending in #LATEST, determines the correct folder that should correspond to that
   * input path.
   *
   * @param inputPath The input path (that must end in #LATEST, such as /data/derived/member/summary/#LATEST)
   * @param conf The job configuration
   * @return The correct latest input path
   */
  String getLatestPathHelper(String inputPath) {
    def filter = new PathFilter() {
      @Override
      boolean accept(Path path) {
        return !(path.getName().startsWith("_") || path.getName().startsWith("."));
      }
    }

    def statuses = fs.listStatus(new Path(inputPath), filter).sort { a, b -> a.compareTo(b)
    }
    // if the directory is empty, then return the directory's path.
    if (statuses.size() == 0) {
      return inputPath;
    }
    String latestPath = inputPath + statuses[statuses.length - 1].getPath().getName();
    latestPath = latestPath.replaceAll("//", "/");
    return latestPath;
  }

  String convertToSlashes(String inputPath) {
    return inputPath.replaceAll(LATEST, "/" + LATEST).replaceAll("//", "/");
  }
}