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

import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

/**
 * Enum for our custom Hadoop counters for this job.
 */
enum WordCountCounters {
  INPUT_WORDS
}

/**
 * Traditional "word count" Hadoop example that runs in Azkaban.
 * <p>
 * Note that for Azkaban Java map-reduce jobs you don't implement the Tool interface, as Azkaban
 * doesn't fully support it (which is too bad).
 */
public class WordCountJob extends Configured {

  private static final Logger _logger = Logger.getLogger(WordCountJob.class);
  private String _name;
  private Properties _properties;

  /**
   * Constructor for the WordCountJob class. Azkaban will pass in your job name and job properties.
   *
   * @param name The Azkaban job name
   * @param properties The Azkaban job properties
   */
  public WordCountJob(String name, Properties properties) {
    super(new Configuration());
    _name = name;
    _properties = properties;
  }

  /**
   * Azkaban will look for a method named `run` to start your job. Use this method to setup all the
   * Hadoop-related configuration for your job and submit it.
   *
   * @throws Exception If there is an exception during the configuration or submission of your job
   */
  public void run() throws Exception {
    _logger.info(String.format("Configuring job for the class %s", getClass().getSimpleName()));

    Job job = Job.getInstance(getConf());
    job.setJarByClass(WordCountJob.class);
    job.setJobName(_name);

    job.setMapperClass(WordCountMapper.class);
    job.setCombinerClass(WordCountCombiner.class);
    job.setReducerClass(WordCountReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    String inputPath = _properties.getProperty("input.path");
    String outputPath = _properties.getProperty("output.path");
    boolean forceOverwrite = Boolean.parseBoolean(_properties.getProperty("force.output.overwrite", "false"));

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    // Before we submit the job, remove the old the output directory
    if (forceOverwrite) {
      FileSystem fs = FileSystem.get(job.getConfiguration());
      fs.delete(FileOutputFormat.getOutputPath(job), true);
    }

    // Since we have Kerberos enabled at LinkedIn, we must add the token to our configuration. If
    // you don't use Kerberos security for your Hadoop cluster, you don't need this code.
    if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
      job.getConfiguration().set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"));
    }

    // Submit the job for execution
    _logger.info(String.format("About to submit the job named %s", _name));
    boolean succeeded = job.waitForCompletion(true);

    // Before we return, display our custom counters for the job in the Azkaban logs
    long inputWords = job.getCounters().findCounter(WordCountCounters.INPUT_WORDS).getValue();
    _logger.info(String.format("Read a total of %d input words", inputWords));

    // Azkaban will not realize the Hadoop job failed unless you specifically throw an exception
    if (!succeeded) {
      throw new Exception(String.format("Azkaban job %s failed", _name));
    }
  }

  public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private final static LongWritable oneWritable = new LongWritable(1);
    private long numRecords = 0;
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);

      while (tokenizer.hasMoreTokens()) {
        word.set(tokenizer.nextToken());
        context.write(word, oneWritable);
        context.getCounter(WordCountCounters.INPUT_WORDS).increment(1);
      }

      if ((++numRecords % 100) == 0) {
        context.setStatus(String.format("Read %d records from the input file so far", numRecords));
      }
    }
  }

  public static class WordCountCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable l : values) {
        sum += l.get();
      }
      context.write(key, new LongWritable(sum));
    }
  }

  public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable l : values) {
        sum += l.get();
      }
      context.write(key, new LongWritable(sum));
    }
  }
}