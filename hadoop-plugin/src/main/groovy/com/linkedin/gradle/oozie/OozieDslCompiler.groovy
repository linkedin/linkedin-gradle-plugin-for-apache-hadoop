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
package com.linkedin.gradle.oozie;

import com.linkedin.gradle.hadoopdsl.BaseCompiler;
import com.linkedin.gradle.hadoopdsl.HadoopDslExtension;
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Namespace;
import com.linkedin.gradle.hadoopdsl.Properties
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.CommandJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopJavaJob;
import com.linkedin.gradle.hadoopdsl.job.HiveJob;
import com.linkedin.gradle.hadoopdsl.job.JavaProcessJob;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.LaunchJob;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.hadoopdsl.job.SparkJob;

import com.linkedin.gradle.oozie.xsd.workflow.ObjectFactory;
import com.linkedin.gradle.oozie.xsd.workflow.ACTION;
import com.linkedin.gradle.oozie.xsd.workflow.ACTIONTRANSITION;
import com.linkedin.gradle.oozie.xsd.workflow.CONFIGURATION;
import com.linkedin.gradle.oozie.xsd.workflow.DELETE;
import com.linkedin.gradle.oozie.xsd.workflow.END;
import com.linkedin.gradle.oozie.xsd.workflow.FLAG;
import com.linkedin.gradle.oozie.xsd.workflow.JAVA;
import com.linkedin.gradle.oozie.xsd.workflow.JOIN;
import com.linkedin.gradle.oozie.xsd.workflow.KILL;
import com.linkedin.gradle.oozie.xsd.workflow.MAPREDUCE;
import com.linkedin.gradle.oozie.xsd.workflow.PIG;
import com.linkedin.gradle.oozie.xsd.workflow.PREPARE;
import com.linkedin.gradle.oozie.xsd.workflow.START;
import com.linkedin.gradle.oozie.xsd.workflow.WORKFLOWAPP
import com.linkedin.gradle.oozie.xsd.hive.ObjectFactory as HiveObjectFactory;
import com.linkedin.gradle.oozie.xsd.hive.ACTION as Hive;
import com.linkedin.gradle.oozie.xsd.hive.CONFIGURATION as HIVE_CONFIGURATION;
import com.linkedin.gradle.oozie.xsd.hive.DELETE as HIVE_DELETE;
import com.linkedin.gradle.oozie.xsd.hive.PREPARE as HIVE_PREPARE;

import com.linkedin.gradle.oozie.xsd.spark.ObjectFactory as SparkObjectFactory;
import com.linkedin.gradle.oozie.xsd.spark.ACTION as Spark;
import com.linkedin.gradle.oozie.xsd.spark.CONFIGURATION as SPARK_CONFIGURATION;
import com.linkedin.gradle.oozie.xsd.spark.DELETE as SPARK_DELETE;
import com.linkedin.gradle.oozie.xsd.spark.PREPARE as SPARK_PREPARE;

import org.gradle.api.GradleException;

import javax.xml.bind.JAXB;

import org.gradle.api.Project;

/**
 * Hadoop DSL compiler for Apache Oozie.
 */
class OozieDslCompiler extends BaseCompiler {

  // Map of the Hadoop DSL job names to the ACTION Oozie jobs for the jobs
  Map<String, ACTION> actionMap;

  // Factory for creating new Oozie objects
  ObjectFactory objectFactory;

  // Current Oozie workflow we are building
  WORKFLOWAPP oozieWorkflow;

  /**
   * Constructor for the OozieDslCompiler.
   *
   * @param project The Gradle project
   */
  OozieDslCompiler(Project project) {
    super(project);
    actionMap = new HashMap<String, Job>();
    objectFactory = new ObjectFactory();
    oozieWorkflow = null;
  }

  /**
   * Cleans up generated files from the build directory.
   *
   * @param buildDirectoryFile Java File object representing the build directory
   */
  @Override
  void cleanBuildDirectory(File buildDirectoryFile) {
    buildDirectoryFile.eachFileRecurse(groovy.io.FileType.FILES) { f ->
      String fileName = f.getName().toLowerCase();
      if (fileName.endsWith(".xml")) {
        f.delete();
      }
    }
  }

  /**
   * Selects the appropriate build directory for the given compiler.
   *
   * @param hadoop The HadoopDslExtension object
   * @return The build directory for this compiler
   */
  @Override
  String getBuildDirectory(HadoopDslExtension hadoop) {
    return hadoop.oozieDirectory;
  }

  /**
   * Builds the namespace. Creates a subdirectory for everything under the namespace.
   *
   * @param namespace The namespace to build
   */
  @Override
  void visitNamespace(Namespace namespace) {
    // Save the last parent directory information
    String oldParentDirectory = this.parentDirectory;

    // Set the new parent directory information
    this.parentDirectory = "${this.parentDirectory}/${namespace.name}";

    // Build a directory for the namespace
    File file = new File(this.parentDirectory);
    if (file.exists()) {
      if (!file.isDirectory()) {
        throw new IOException("Directory ${this.parentDirectory} for the namespace ${namespace.name} must specify a directory");
      }
    }
    else {
      // Try to make the directory automatically if we can. For git users, this is convenient as
      // git will not push empty directories in the repository (and users will often add the
      // generated job files to their gitignore).
      if (!file.mkdirs()) {
        throw new IOException("Directory ${this.parentDirectory} for the namespace ${namespace.name} could not be created");
      }
    }

    // Visit the elements in the namespace
    visitScopeContainer(namespace);

    // Restore the last parent directory
    this.parentDirectory = oldParentDirectory;
  }

  /**
   * Builds the workflow.
   * <p>
   * NOTE: not all jobs in the workflow are built by default. Only those jobs that can be found
   * from a transitive walk starting from the jobs the workflow targets actually get built.
   *
   * @param workflow The workflow to build
   */
  @Override
  void visitWorkflow(Workflow workflow) {
    visitWorkflow(workflow, false);
  }

  /**
   * Builds the workflow. If the flow is a subflow, it will be constructed with a root FlowJob.
   * <p>
   * NOTE: not all jobs in the workflow are built by default. Only those jobs that can be found
   * from a transitive walk starting from the jobs the workflow targets actually get built.
   *
   * @param workflow The workflow to build
   * @param subflow Whether or not the workflow is a subflow
   */
  void visitWorkflow(Workflow workflow, boolean subflow) {
    // Save the last scope information and set the new parent scope information
    NamedScope oldParentScope = this.parentScope;
    this.parentScope = workflow.scope;

    // Start working on the new Oozie workflow. We'll create the workflow object and add the
    // start, kill and end nodes to it.
    if (!subflow) {
      oozieWorkflow = objectFactory.createWORKFLOWAPP();
      oozieWorkflow.name = workflow.name;

      // Add the start node
      START start = objectFactory.createSTART();
      oozieWorkflow.setStart(start);

      // Add the kill node
      KILL kill = objectFactory.createKILL();
      kill.setName("kill");
      kill.setMessage('Job failed, error message [${wf:errorMessage(wf:lastErrorNode())}]');
      oozieWorkflow.getDecisionOrForkOrJoin().add(kill);

      // Add the end node
      END end = objectFactory.createEND();
      end.setName("end");
      oozieWorkflow.setEnd(end);
    }
    else {
      // TODO Add subflow here
    }

    // Build the list of jobs and subflows to build for the workflow
    workflow.buildWorkflowTargets(subflow);

    // Visit each job to build in the workflow
    workflow.jobsToBuild.each() { Job job ->
      visitJobToBuild(job);
    }

    // Once we have added the jobs to build to the workflow, go back and add the job transitions
    workflow.jobsToBuild.each() { Job job ->
      visitJobTransitions(job);
    }

    // Visit each properties object in the workflow
    workflow.properties.each() { Properties props ->
      visitProperties(props);
    }

    // Visit each subflow to build in the workflow
    workflow.flowsToBuild.each() { Workflow flow ->
      // visitWorkflow(flow, true);
      // TODO
    }

    // Visit each child namespace in the workflow
    workflow.namespaces.each() { Namespace namespace ->
      visitNamespace(namespace);
    }

    // Write out the Oozie workflow XML
    File workflowFile = new File(this.parentDirectory, "${oozieWorkflow.name}.xml");
    workflowFile.createNewFile();

    JAXB.marshal(objectFactory.createWorkflowApp(oozieWorkflow), workflowFile);
    workflowFile.setWritable(false);

    // Restore the last parent scope
    this.parentScope = oldParentScope;
  }

  /**
   * Builds a job that has been found on a transitive walk starting from the jobs the workflow
   * targets. These are the jobs that should actually be built by the compiler.
   *
   * @param The job to build
   */
  void visitJobToBuild(Job job) {
  }

  /**
   * Visitor to build HiveJob
   * @param job The HiveJob to build
   */
  void visitJobToBuild(HiveJob job) {

    // Create Hive action
    HiveObjectFactory hiveObjectFactory = new HiveObjectFactory();
    Hive oozieJob = hiveObjectFactory.createACTION();

    // Set nameNode and jobTracker
    oozieJob.setNameNode('${nameNode}');
    oozieJob.setJobTracker('${jobTracker}');

    // The user should have this property defined in the job.properties. This should contain path of the
    // hive-site.xml or the settings file for the hive so that oozie can contact the hive metastore.
    if(job.jobProperties.containsKey("jobXml")) {
      oozieJob.getJobXml().add(job.jobProperties.get("jobXml"));
    }

    // Set script file for the job
    oozieJob.setScript(job.script);

    // By default, automatically delete any HDFS paths the job writes
    // TODO make this part of the "writing" method options
    if (job.writing.size() > 0) {
      HIVE_PREPARE prepare = hiveObjectFactory.createPREPARE();

      job.writing.each { String path ->
        HIVE_DELETE delete = hiveObjectFactory.createDELETE();
        delete.setPath(path);
        prepare.getDelete().add(delete);
      }

      oozieJob.setPrepare(prepare);
    }

    // Add the Hadooop job conf properties
    if (job.confProperties.size() > 0) {
      HIVE_CONFIGURATION conf = hiveObjectFactory.createCONFIGURATION();

      job.confProperties.each { String name, Object val ->
        HIVE_CONFIGURATION.Property prop = hiveObjectFactory.createCONFIGURATIONProperty();
        prop.setName(name);
        prop.setValue(val.toString());
        conf.getProperty().add(prop);
      }

      oozieJob.setConfiguration(conf);
    }

    // Add archives on HDFS to Distributed Cache
    job.cacheArchives.each { String name, String path ->
      oozieJob.getArchive().add("${path}#${name}".toString());
    }

    // Add files on HDFS to Distributed Cache
    job.cacheFiles.each { String name, String path ->
      oozieJob.getFile().add("${path}#${name}".toString());
    }

    // set parameters of the job. This should be done using <argument> according to new syntax
    job.parameters.each { String name, Object val ->
      oozieJob.getArgument().add("-hivevar");
      oozieJob.getArgument().add("${name}=${val.toString()}".toString());
    }

    // Add the action and add the job to the action
    ACTIONTRANSITION killTransition = objectFactory.createACTIONTRANSITION();
    killTransition.setTo("kill")

    // Don't specify the "Ok" transition for the action; we'll add the job transitions later
    String jobName = job.buildFileName(this.parentScope);
    ACTION action = objectFactory.createACTION();
    action.setError(killTransition);
    action.setName(jobName);
    action.setAny(hiveObjectFactory.createHive(oozieJob));

    oozieWorkflow.getDecisionOrForkOrJoin().add(action);

    // Remember the action so we can specify the job transitions later
    actionMap.put(job.name, action);
  }

  /**
   * Visitor to build SparkJob
   * @param The SparkJob to build
   */
  void visitJobToBuild(SparkJob job) {

    // Set of all spark options
    Set<String> allSparkOptions = [
      "master",
      "deploy-mode",
      "py-files",
      "properties-file",
      "driver-memory",
      "driver-java-options",
      "driver-library-path",
      "driver-class-path",
      "executor-memory",
      "driver-cores",
      "total-executor-cores",
      "executor-cores",
      "queue",
      "num-executors",
      "archives",
      "principal",
      "keytab"
    ];

    // Create Spark action
    SparkObjectFactory sparkObjectFactory = new SparkObjectFactory();
    Spark oozieJob = sparkObjectFactory.createACTION();

    // Set nameNode and jobTracker
    oozieJob.setNameNode('${nameNode}');
    oozieJob.setJobTracker('${jobTracker}');

    // Jars are added from jobProperties. The execution-jar is also added to jars.
    StringBuilder buildJars = new StringBuilder();
    buildJars.append(job.executionTarget);
    if (job.jobProperties.containsKey("jars")) {
      buildJars.append(",");
      buildJars.append(job.jobProperties.get("jars"));
    }
    oozieJob.setJar(buildJars.toString());

    // Add the spark flags and configurations
    StringBuilder builder = new StringBuilder();
    builder.append(" ").append(job.flags.collect() { flag -> return "--$flag" }.join(" "));
    builder.append(" ").append(job.sparkConfs.collect() {
      key, value -> "--conf $key=$value";
    }.join(" "));

    // Set the name of the job
    oozieJob.setName(job.name);

    // Set master and deploy-mode from the properties. Set other properties as spark options.
    job.jobProperties.each() { key, value ->
      if (allSparkOptions.contains(key)) {
        switch (key) {
          case "master":
            oozieJob.setMaster(value);
            break;
          case "deploy-mode":
            oozieJob.setMode(value);
            break;
          default:
            builder.append(" ").append("--$key $value");
        }
      }
    }

    // Set spark options
    oozieJob.setSparkOpts(builder.toString());

    // Set execution class
    oozieJob.setClazz(job.appClass);

    // Set application parameters
    job.appParams.each { param -> oozieJob.getArg().add(param.toString())}

    // By default, automatically delete any HDFS paths the job writes
    // TODO make this part of the "writing" method options
    if (job.writing.size() > 0) {
      SPARK_PREPARE prepare = sparkObjectFactory.createPREPARE();

      job.writing.each { String path ->
        SPARK_DELETE delete = sparkObjectFactory.createDELETE();
        delete.setPath(path);
        prepare.getDelete().add(delete);
      }

      oozieJob.setPrepare(prepare);
    }

    // Add the Hadooop job conf properties
    if (job.confProperties.size() > 0) {
      SPARK_CONFIGURATION conf = sparkObjectFactory.createCONFIGURATION();

      job.confProperties.each { String name, Object val ->
        SPARK_CONFIGURATION.Property prop = sparkObjectFactory.createCONFIGURATIONProperty();
        prop.setName(name);
        prop.setValue(val.toString());
        conf.getProperty().add(prop);
      }

      oozieJob.setConfiguration(conf);
    }

    // Add the action and add the job to the action
    ACTIONTRANSITION killTransition = objectFactory.createACTIONTRANSITION();
    killTransition.setTo("kill")

    // Don't specify the "Ok" transition for the action; we'll add the job transitions later
    String jobName = job.buildFileName(this.parentScope);
    ACTION action = objectFactory.createACTION();
    action.setError(killTransition);
    action.setName(jobName);
    action.setAny(sparkObjectFactory.createSpark(oozieJob));

    oozieWorkflow.getDecisionOrForkOrJoin().add(action);

    // Remember the action so we can specify the job transitions later
    actionMap.put(job.name, action);
  }

  void visitJobToBuild(CommandJob commandJob) {
  // TODO
  }

  /**
   * Visitor to build HadoopJavaJob
   * Currently DSL doesn't support separate syntax for map and reduce class.
   * we can use something like "uses "map:com.linkedin.hello.mapper,reduce:com.linkedin.hello.reducer"
   * @param job The HadoopJavaJob to build
   */
  void visitJobToBuild(HadoopJavaJob job) {

    // Create Mapreduce action
    MAPREDUCE oozieJob = objectFactory.createMAPREDUCE();

    // Set nameNode and jobTracker
    oozieJob.setNameNode('${nameNode}');
    oozieJob.setJobTracker('${jobTracker}');

    // set mapper
    if(job.mapClass!=null && !job.mapClass.isEmpty()) {
      job.confProperties.put("mapred.mapper.class", job.mapClass);
    }

    // set reducer
    if(job.reduceClass!=null && !job.reduceClass.isEmpty()) {
      job.confProperties.put("mapred.reducer.class", job.reduceClass);
    }

    // By default, automatically delete any HDFS paths the job writes
    // TODO make this part of the "writing" method options
    if (job.writing.size() > 0) {
      PREPARE prepare = objectFactory.createPREPARE();

      job.writing.each { String path ->
        DELETE delete = objectFactory.createDELETE();
        delete.setPath(path);
        prepare.getDelete().add(delete);
      }

      oozieJob.setPrepare(prepare);
    }

    // Add the Hadooop job conf properties
    if (job.confProperties.size() > 0) {
      CONFIGURATION conf = objectFactory.createCONFIGURATION();

      job.confProperties.each { String name, Object val ->
        CONFIGURATION.Property prop = objectFactory.createCONFIGURATIONProperty();
        prop.setName(name);
        prop.setValue(val.toString());
        conf.getProperty().add(prop);
      }
      oozieJob.setConfiguration(conf);
    }

    // Add archives on HDFS to Distributed Cache
    job.cacheArchives.each { String name, String path ->
      oozieJob.getArchive().add("${path}#${name}".toString());
    }

    // Add files on HDFS to Distributed Cache
    job.cacheFiles.each { String name, String path ->
      oozieJob.getFile().add("${path}#${name}".toString());
    }

    // Add the action and add the job to the action
    ACTIONTRANSITION killTransition = objectFactory.createACTIONTRANSITION();
    killTransition.setTo("kill")

    // Don't specify the "Ok" transition for the action; we'll add the job transitions later
    String jobName = job.buildFileName(this.parentScope);
    ACTION action = objectFactory.createACTION();
    action.setError(killTransition);
    action.setName(jobName);
    action.setMapReduce(oozieJob);
    oozieWorkflow.getDecisionOrForkOrJoin().add(action);

    // Remember the action so we can specify the job transitions later
    actionMap.put(job.name, action);
  }

  /**
   * Visitor to build JavaProcessJob
   * We currently don't support parameters in the
   * dsl for a java job. For now the user should be able to pass the paramters in the
   * properties as "'params':'param1,param2,param3'"
   * @param job The JavaProcessJob to build
   */
  void visitJobToBuild(JavaProcessJob job) {

    // Create Java action
    JAVA oozieJob = objectFactory.createJAVA();

    // Set nameNode and jobTracker
    oozieJob.setNameNode('${nameNode}');
    oozieJob.setJobTracker('${jobTracker}');

    // Add the main class
    oozieJob.setMainClass(job.javaClass);

    // Add java options
    oozieJob.setJavaOpts(job.jvmProperties.collect() { key, val -> return "-D${key}=${val.toString()}" }.join(" "));

    // We currently don't support parameters in the java process job, user can pass parameters as "params:'param1,param2,param3".
    if (job.jobProperties.containsKey("params")) {
      String[] params = job.jobProperties.get("params").toString().split(",");
      params.each {
        oozieJob.getArg().add(it);
      }
    }

    // If the captureOutput is set in the jobProperties. Enable it.
    if (job.jobProperties.containsKey("captureOutput")) {
      if (job.jobProperties.get("captureOutput").equals("true")) {
        oozieJob.setCaptureOutput(new FLAG());
      }
    }

    // Add the action and add the job to the action
    ACTIONTRANSITION killTransition = objectFactory.createACTIONTRANSITION();
    killTransition.setTo("kill")

    // Don't specify the "Ok" transition for the action; we'll add the job transitions later
    String jobName = job.buildFileName(this.parentScope);
    ACTION action = objectFactory.createACTION();
    action.setError(killTransition);
    action.setName(jobName);
    action.setJava(oozieJob);
    oozieWorkflow.getDecisionOrForkOrJoin().add(action);

    // Remember the action so we can specify the job transitions later
    actionMap.put(job.name, action);
  }

  /**
   * Visitor to build PigJob
   * @param job The PigJob to build
   */
  void visitJobToBuild(PigJob job) {

    // Create Pig action
    PIG oozieJob = objectFactory.createPIG();

    // Set nameNode and jobTracker
    oozieJob.setNameNode('${nameNode}');
    oozieJob.setJobTracker('${jobTracker}');

    // Set pig scrip to execute
    oozieJob.setScript(job.script);

    // By default, automatically delete any HDFS paths the job writes
    // TODO make this part of the "writing" method options
    if (job.writing.size() > 0) {
      PREPARE prepare = objectFactory.createPREPARE();

      job.writing.each { String path ->
        DELETE delete = objectFactory.createDELETE();
        delete.setPath(path);
        prepare.getDelete().add(delete);
      }

      oozieJob.setPrepare(prepare);
    }

    // Add the Hadooop job conf properties
    if (job.confProperties.size() > 0) {
      CONFIGURATION conf = objectFactory.createCONFIGURATION();

      job.confProperties.each { String name, Object val ->
        CONFIGURATION.Property prop = objectFactory.createCONFIGURATIONProperty();
        prop.setName(name);
        prop.setValue(val.toString());
        conf.getProperty().add(prop);
      }

      oozieJob.setConfiguration(conf);
    }

    // Add archives on HDFS to Distributed Cache
    job.cacheArchives.each { String name, String path ->
      oozieJob.getArchive().add("${path}#${name}".toString());
    }

    // Add files on HDFS to Distributed Cache
    job.cacheFiles.each { String name, String path ->
      oozieJob.getFile().add("${path}#${name}".toString());
    }

    // Add the Pig parameters, they must be added via <argument>-param</argument><argument>name=value</argument> according to the newer version
    job.parameters.each { String name, Object val ->
      oozieJob.getArgument().add("-param");
      oozieJob.getArgument().add("${name}=${val.toString()}".toString());
    }

    // Add the action and add the job to the action
    ACTIONTRANSITION killTransition = objectFactory.createACTIONTRANSITION();
    killTransition.setTo("kill")

    // Don't specify the "Ok" transition for the action; we'll add the job transitions later
    String jobName = job.buildFileName(this.parentScope);
    ACTION action = objectFactory.createACTION();
    action.setError(killTransition);
    action.setName(jobName);
    action.setPig(oozieJob);
    oozieWorkflow.getDecisionOrForkOrJoin().add(action);

    // Remember the action so we can specify the job transitions later
    actionMap.put(job.name, action);
  }


  /**
   * Builds a properties file.
   *
   * @param props The Properties object to build
   */
  @Override
  void visitProperties(Properties props) {
    Map<String, String> allProperties = props.buildProperties(this.parentScope);
    if (allProperties.size() == 0) {
      return;
    }
    checkRequiredProperties(allProperties.keySet());
    String fileName = props.buildFileName(this.parentScope);
    File file = new File(this.parentDirectory, "${fileName}.properties");
    file.withWriter { out ->
      out.writeLine("# This file generated from the Hadoop DSL. Do not edit by hand.");
      allProperties.each { key,value ->
        out.writeLine("${key}=${value}");
      }
    }
  }

  void visitJobTransitions(Job job) {
    String jobName = job.buildFileName(this.parentScope);
    visitJobTransitionsHelper(job, jobName);
  }

  void visitJobTransitions(LaunchJob job) {
    String endName = oozieWorkflow.getEnd().getName();
    visitJobTransitionsHelper(job, endName);
  }

  void visitJobTransitionsHelper(Job job, String toName) {
    if (job.dependencyNames.size() == 0) {
      oozieWorkflow.getStart().setTo(toName);
    }
    else if (job.dependencyNames.size() == 1) {
      // Transition the dependent job to this job
      ACTIONTRANSITION okTransition = objectFactory.createACTIONTRANSITION();
      okTransition.setTo(toName);

      String dependencyName = job.dependencyNames.toList().get(0);
      ACTION action = actionMap.get(dependencyName);
      action.setOk(okTransition);
    }
    else {
      // Transition all the dependent jobs to a JOIN node, and transition the JOIN to this job
      JOIN join = objectFactory.createJOIN();
      join.setName("${toName}_JOIN".toString());
      join.setTo(toName);

      job.dependencyNames.each { String dependencyName ->
        ACTIONTRANSITION okTransition = objectFactory.createACTIONTRANSITION();
        okTransition.setTo(toName);

        ACTION action = actionMap.get(dependencyName);
        action.setOk(okTransition);
      }

      oozieWorkflow.getDecisionOrForkOrJoin().add(join);
    }
  }

  /**
   * Method to check if the required properties have been defined
   * @param propertyNames The set of all properties
   */
  void checkRequiredProperties(Set<String> propertyNames) {

    if(!propertyNames.contains("nameNode")) {
      throw new GradleException("Property 'nameNode' is required")
    }
    if(!propertyNames.contains("jobTracker")) {
      throw new GradleException("Property 'jobTracker' is required")
    }
    if(!propertyNames.contains("oozie.wf.application.path")) {
      throw new GradleException("Property 'oozie.wf.application.path' is required")
    }
  }
}
