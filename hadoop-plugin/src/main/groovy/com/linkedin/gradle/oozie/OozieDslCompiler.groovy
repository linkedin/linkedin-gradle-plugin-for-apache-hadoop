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
import com.linkedin.gradle.hadoopdsl.HadoopDslExtension
import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.Namespace;
import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.PropertySet;
import com.linkedin.gradle.hadoopdsl.Workflow;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.LaunchJob
import com.linkedin.gradle.hadoopdsl.job.PigJob;

import com.linkedin.gradle.oozie.xsd.JOIN
import com.linkedin.gradle.oozie.xsd.ObjectFactory;
import com.linkedin.gradle.oozie.xsd.ACTION;
import com.linkedin.gradle.oozie.xsd.ACTIONTRANSITION;
import com.linkedin.gradle.oozie.xsd.CONFIGURATION;
import com.linkedin.gradle.oozie.xsd.DELETE;
import com.linkedin.gradle.oozie.xsd.END;
import com.linkedin.gradle.oozie.xsd.KILL;
import com.linkedin.gradle.oozie.xsd.PIG;
import com.linkedin.gradle.oozie.xsd.PREPARE;
import com.linkedin.gradle.oozie.xsd.START;
import com.linkedin.gradle.oozie.xsd.WORKFLOWAPP;

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
      // visitProperties(props);
      // TODO
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
    // TODO fill out the rest of the job types
  }

  void visitJobToBuild(PigJob job) {
    PIG oozieJob = objectFactory.createPIG();
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

    // Add the Pig parameters
    job.parameters.each { String name, Object val ->
      oozieJob.getParam().add("${name}=${val.toString()}".toString());
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
}