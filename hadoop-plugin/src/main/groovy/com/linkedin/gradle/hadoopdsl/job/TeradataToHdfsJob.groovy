package com.linkedin.gradle.hadoopdsl.job;

import com.linkedin.gradle.hadoopdsl.NamedScope;

/**
 * Job class for type=teradataToHdfsJob jobs.
 * <p>
 * According to the Azkaban documentation at http://azkaban.github.io/azkaban/docs/latest/#job-types,
 * this is the job type to move data from HDFS to Teradata
 * <p>
 * In the DSL, a TeradataToHdfsJob can be specified with:
 * <pre>
 *   teradataToHdfsJob('jobName') {
 *     hostName 'dw.foo.com'  // Required
 *     userId 'scott' //Required
 *     credentialName 'com.linkedin.teradata.scott' //Required
 *     sourceTable 'person' //Either sourceTable or sourceQuery is required
 *     sourceQuery 'select * from person;' //Either sourceTable or sourceQuery is required
 *     targetHdfsPath '/job/data/test/output' //Required
 *     avroSchemaPath '/job/data/src/avro.avsc'
 *     avroSchemaInline '{"type":"record","namespace":"com.example","name":"FullName","fields":[{"name":"first","type":"string"},{"name":"last","type":"string"}]}'
 *     set hadoopProperties: [
 *       'hadoopPropertyName1' : 'hadoopPropertyValue1',
 *       'hadoopPropertyName2' : 'hadoopPropertyValue2'
 *     ]
 *   }
 * </pre>
 */
class TeradataToHdfsJob extends Job {
  String hostName;
  String userId;
  String credentialName;
  String sourceTable;
  String sourceQuery;
  String targetHdfsPath;
  String avroSchemaPath;
  String avroSchemaInline;
  Map<String, Object> hadoopProperties;

  public TeradataToHdfsJob(String jobName) {
    super(jobName);
    setJobProperty("type", "teradataToHdfs");
    hadoopProperties = new LinkedHashMap<String, Object>();
  }

  @Override
  Map<String, String> buildProperties(NamedScope parentScope) {
    Map<String, String> allProperties = super.buildProperties(parentScope);

    if (hadoopProperties.size() > 0) {
      String hadoopConfig = hadoopProperties.collect() { String key, Object val -> return "-D${key}=${val.toString()}" }.join(" ");
      allProperties["hadoop.config"] = hadoopConfig;
    }

    return allProperties;
  }

  void hostName(String hostName) {
    this.hostName = hostName;
    setJobProperty("td.hostname", hostName);
  }

  void userId(String userId) {
    this.userId = userId;
    setJobProperty("td.userid", userId);
  }

  void credentialName(String credentialName) {
    this.credentialName = credentialName;
    setJobProperty("td.credentialName", credentialName);
  }

  void sourceTable(String sourceTable) {
    this.sourceTable = sourceTable;
    setJobProperty("source.td.tablename", sourceTable);
  }

  void sourceQuery(String sourceQuery) {
    this.sourceQuery = sourceQuery;
    setJobProperty("source.td.sourcequery", sourceQuery);
  }

  void targetHdfsPath(String targetHdfsPath) {
    this.targetHdfsPath = targetHdfsPath;
    setJobProperty("target.hdfs.path", targetHdfsPath);
  }

  void avroSchemaPath(String avroSchemaPath) {
    this.avroSchemaPath = avroSchemaPath;
    setJobProperty("avro.schema.path", avroSchemaPath);
  }

  void avroSchemaInline(String avroSchemaInline) {
    this.avroSchemaInline = avroSchemaInline;
    setJobProperty("avro.schema.inline", avroSchemaInline);
  }

  void setHadoopProperty(String name, Object value) {
    hadoopProperties.put(name, value);
  }

  @Override
  void set(Map args) {
    super.set(args);

    if (args.containsKey("hadoopProperties")) {
      Map<String, Object> hadoopProperties = args.hadoopProperties;
      hadoopProperties.each() { name, value ->
        setHadoopProperty(name, value);
      }
    }
  }

  /**
   * Clones the job.
   *
   * @return The cloned job
   */
  @Override
  TeradataToHdfsJob clone() {
    return clone(new TeradataToHdfsJob(name));
  }

  /**
   * Helper method to set the properties on a cloned job.
   *
   * @param cloneJob The job being cloned
   * @return The cloned job
   */
  TeradataToHdfsJob clone(TeradataToHdfsJob cloneJob) {
    cloneJob.hostName = hostName;
    cloneJob.userId = userId;
    cloneJob.credentialName = credentialName;
    cloneJob.sourceTable = sourceTable;
    cloneJob.sourceQuery = sourceQuery;
    cloneJob.targetHdfsPath = targetHdfsPath;
    cloneJob.avroSchemaPath = avroSchemaPath;
    cloneJob.avroSchemaInline = avroSchemaInline;
    cloneJob.hadoopProperties = new LinkedHashMap<String, String>(hadoopProperties);
    return super.clone(cloneJob);
  }
}
