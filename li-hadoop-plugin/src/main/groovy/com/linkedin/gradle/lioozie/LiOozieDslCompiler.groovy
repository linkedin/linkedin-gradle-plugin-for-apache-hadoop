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
package com.linkedin.gradle.lioozie;

import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.job.HiveJob;
import com.linkedin.gradle.oozie.OozieDslCompiler;
import org.gradle.api.Project;

import com.linkedin.gradle.oozie.xsd.hive.ACTION as Hive;
import com.linkedin.gradle.oozie.xsd.hive.ObjectFactory as HiveObjectFactory;
import com.linkedin.gradle.oozie.xsd.hive.CONFIGURATION as HIVE_CONFIGURATION;
import com.linkedin.gradle.oozie.xsd.hive.DELETE as HIVE_DELETE;
import com.linkedin.gradle.oozie.xsd.hive.PREPARE as HIVE_PREPARE;

import com.linkedin.gradle.oozie.xsd.workflow.ACTION;
import com.linkedin.gradle.oozie.xsd.workflow.ACTIONTRANSITION;
import com.linkedin.gradle.oozie.xsd.workflow.CREDENTIAL;
import com.linkedin.gradle.oozie.xsd.workflow.CREDENTIALS;

/**
 * LinkedIn specific customizations to the OozieDslCompiler
 */
class LiOozieDslCompiler extends OozieDslCompiler {

  LiOozieDslCompiler(Project project) {
    super(project);
  }

  /**
   * Adds linkedin specific properties to the jobProperties.
   *
   * @param props The Properties object to build
   */
  @Override
  void visitProperties(Properties props) {

    if(!props.jobProperties.containsKey("nameNode")) {
      props.jobProperties.put("nameNode","hdfs://eat1-nertznn01.grid.linkedin.com:9000");
    }

    if (!props.jobProperties.containsKey("jobTracker")) {
      props.jobProperties.put("jobTracker","eat1-nertzrm01.grid.linkedin.com:8032");
    }

    super.visitProperties(props);
  }

  /**
   * Linkedin specific visitor for the hive job. We need to provide credentials to contact the metastore
   * with kerberos. We'll support credentials separately in the dsl later.
   * @param job The HiveJob to build;
   */
  @Override
  void visitJobToBuild(HiveJob job) {

    // Create credentials to contact hive metastore with kerberos.
    CREDENTIALS credAction = objectFactory.createCREDENTIALS();
    CREDENTIAL credential = new CREDENTIAL();
    credential.setType('hive_metastore');
    CREDENTIAL.Property metastoreURI = new CREDENTIAL.Property();
    CREDENTIAL.Property metastorePrincipal = new CREDENTIAL.Property();

    metastoreURI.setName("hcat.metastore.uri");
    // if job properties contains the key hcat.metastore.uri, then set it else use default
    if(job.jobProperties.containsKey("hcat.metastore.uri")) {
      metastoreURI.setValue(job.jobProperties.get("hcat.metastore.uri"));
    }
    else {
      metastoreURI.setValue("thrift://eat1-nertzhcat01.grid.linkedin.com:7552");
    }

    metastorePrincipal.setName("hcat.metastore.principal");
    // if job properties contains the key hcat.metastore.principal then set it else use default
    if(job.jobProperties.containsKey("hcat.metastore.principal")) {
      metastorePrincipal.setValue(job.jobProperties.get("hcat.metastore.principal"));
    }
    else {
      metastorePrincipal.setValue("hcat/_HOST@GRID.LINKEDIN.COM");
    }

    credential.getProperty().add(metastoreURI);
    credential.getProperty().add(metastorePrincipal);
    credAction.getCredential().add(credential);

    credential.setName(job.getName() + "_cred");
    oozieWorkflow.setCredentials(credAction);



    // Create hive action
    HiveObjectFactory hiveObjectFactory = new HiveObjectFactory();
    Hive oozieJob = hiveObjectFactory.createACTION();

    // The user should have this property defined in the job.properties. This should contain path of the
    // hive-site.xml or the settings file for the hive so that oozie can contact the hive metastore.
    if(job.jobProperties.containsKey("jobXml")) {
      oozieJob.getJobXml().add(job.jobProperties.get("jobXml"));
    }

    // Set nameNode and jobTracker
    oozieJob.setNameNode('${nameNode}');
    oozieJob.setJobTracker('${jobTracker}');

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
    // Set credentials defined before
    action.setCred(job.getName() + "_cred");
    action.setAny(hiveObjectFactory.createHive(oozieJob));

    oozieWorkflow.getDecisionOrForkOrJoin().add(action);

    // Remember the action so we can specify the job transitions later
    actionMap.put(job.name, action);
  }
}
