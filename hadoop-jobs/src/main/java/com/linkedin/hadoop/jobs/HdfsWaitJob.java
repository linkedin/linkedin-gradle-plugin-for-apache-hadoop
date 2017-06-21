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

import java.io.IOException;
import java.lang.NullPointerException;
import java.lang.NumberFormatException;
import java.util.concurrent.TimeUnit;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.log4j.Logger;

/**
 * Hadoop Java job that is passed arguments from an hdfsWaitJob with type=hadoopJava.
 * This job will quit after it runs longer than the limit set by timeout, and will
 * either succeed or fail depending on the value of failOnTimeout passed in
 * from the Gradle file. Before timeout, if we can find a folder in the directory
 * specified by dirPath that is fresh enough, we cause the job to succeed.
 */
public class HdfsWaitJob extends Configured {

  private static final Logger log = Logger.getLogger(HdfsWaitJob.class);
  private String _name;
  private Properties _properties;

  /**
   * Constructor for HdfsWaitJob
   *
   * @param name The name of the job
   * @param properties The properties specified for this job
   */
  public HdfsWaitJob(String name, Properties properties) {
    super(new Configuration());
    _name = name;
    _properties = properties;
  }

  /**
   * Method run that keeps track of the time this program has been running. If
   * it surpasses the timeout limit, it causes the job to either fail or succeed,
   * depending on the value of 'failOnTimeout' specified in the DSL file. This
   * method continuously calls checkDirectory while it hasn't timed out, using
   * sleepTime as a buffer. If checkDirectory ever returns true on any of the
   * calls, the job succeeds.
   *
   * @throws Exception If there is an exception during the parameter setup
   */
  public void run() throws Exception {
    String dirPath = _properties.getProperty("pathToDirectory");
    long freshness = parseTime(_properties.getProperty("freshness"));
    long timeout = parseTime(_properties.getProperty("timeout"));
    long endTime = System.currentTimeMillis() + timeout;
    long sleepTime = parseTime(_properties.getProperty("sleepInterval", "1M"));
    boolean failOnTimeout = Boolean.valueOf(_properties.getProperty("forceJobToFail"));
    boolean folderFound = false;

    log.info("STATUS: Job started. Checking the directory at " + dirPath + " for fresh folders with a sleep interval of " + _properties.getProperty("sleepInterval", "1M"));

    while (System.currentTimeMillis() < endTime && !folderFound) {
      folderFound = checkDirectory(dirPath, freshness);
      if (!folderFound) {
        log.info("STATUS: No fresh folders found during latest polling. Now sleeping for " + _properties.getProperty("sleepInterval", "1M") + " before polling again.");
        log.info("REMINDER: Job will time out " + TimeUnit.MILLISECONDS.toMinutes(timeout) + " minutes after instantiation.");
        Thread.sleep(sleepTime);
      }
    }

    if (!folderFound) {
      log.info("WARNING: There were no folders found in " + dirPath + " that were fresh enough before reaching timeout.");
      log.info("RESULT: Job timing out with parameter failOnTimeout = " + failOnTimeout);
      if (failOnTimeout) {
        throw new Exception("Forcing job to fail after timeout. failOnTimeout = " + failOnTimeout);
      }
    }
  }

  /**
   * Method parseTime that takes in a string, either timeout, freshness, or sleepTime.
   * It splits the string up using whitespace as the delimiter. Then each time unit
   * and corresponding integer is converted to its millisecond representation. The total
   * millisecond value is then returned.
   *
   * @param prop The string value of either freshness, timeout or sleepTime
   * @throws NumberFormatException
   * @return The amount of milliseconds prop corresponds to as a long
   */
  public long parseTime(String prop) throws NumberFormatException {
    long totalTime = 0;
    String[] strArray = prop.split("\\s+");

    for (int i = 0; i < strArray.length; i++) {
      String str = strArray[i];
      long time = Long.valueOf(str.substring(0, str.length() - 1));
      char unit = str.charAt(str.length() - 1);

      if (unit == 'S') {
        totalTime += TimeUnit.SECONDS.toMillis(time);
      } else if (unit == 'M') {
        totalTime += TimeUnit.MINUTES.toMillis(time);
      } else if (unit == 'H') {
        totalTime += TimeUnit.HOURS.toMillis(time);
      } else if (unit == 'D') {
        totalTime += TimeUnit.DAYS.toMillis(time);
      } else {
        String errMessage = "ERROR: Invalid time specification: " + prop + " does not have units in seconds (S), minutes (M), hours (H), or days (D).";
        log.info(errMessage);
        throw new NumberFormatException(errMessage);
      }
    }
    return totalTime;
  }

  /**
   * Method checkDirectory loops through the folders pointed to by dirPath, and will
   * cause the job to succeed if any of the folders are fresh enough.
   *
   * @param dirPath The path to the directory we are searching for fresh folders
   * @param freshness The timeframe in which the folder has to have been modified by
   * @throws IOException
   * @return A boolean value corresponding to whether a fresh folder was found
   */
  public boolean checkDirectory(String dirPath, long freshness) throws IOException, NullPointerException {
    FileSystem fileSys = FileSystem.get(getConf());

    if (fileSys == null) {
      String errMessage = "ERROR: The file system trying to be accessed does not exist. JOB TERMINATED.";
      log.info(errMessage);
      throw new NullPointerException(errMessage);
    }

    FileStatus[] status = fileSys.listStatus(new Path(dirPath));

    if (status == null) {
      String errMessage = "ERROR: dirPath -> " + dirPath + " is empty or does not exist. JOB TERMINATED.";
      log.info(errMessage);
      throw new IOException(errMessage);
    }

    for (FileStatus file : status) {
      if (file.isDir()) {
        long timeModified = file.getModificationTime();
        if ((System.currentTimeMillis() - timeModified) <= freshness) {
          String fileName = file.getPath().toString();
          log.info("We found this fresh folder in the filePath: " + fileName.substring(fileName.lastIndexOf("/") + 1));
          log.info("SUCCESS: Program now quitting after successfully finding a fresh folder.");
          return true;
        }
      }
    }
    return false;
  }
}
