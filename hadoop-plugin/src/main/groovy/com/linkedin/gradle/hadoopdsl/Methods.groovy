/*
 * Copyright 2014 LinkedIn Corp.
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
package com.linkedin.gradle.hadoopdsl;

import com.linkedin.gradle.hadoopdsl.job.CommandJob;
import com.linkedin.gradle.hadoopdsl.job.HadoopJavaJob;
import com.linkedin.gradle.hadoopdsl.job.HiveJob;
import com.linkedin.gradle.hadoopdsl.job.JavaJob;
import com.linkedin.gradle.hadoopdsl.job.JavaProcessJob;
import com.linkedin.gradle.hadoopdsl.job.Job;
import com.linkedin.gradle.hadoopdsl.job.KafkaPushJob;
import com.linkedin.gradle.hadoopdsl.job.NoOpJob;
import com.linkedin.gradle.hadoopdsl.job.PigJob;
import com.linkedin.gradle.hadoopdsl.job.VoldemortBuildPushJob;

import org.gradle.api.Project;

/**
 * Static helper methods used to implement the Hadoop DSL.
 * <p>
 * People extending the Hadoop DSL by subclassing should generally not need to call methods in
 * this class, as they should already be called appropriately by HadoopDslPlugin,
 * HadoopDslExtension and Workflow classes, but you can call them if you need to do so.
 */
class Methods {

  static Job cloneJob(String name, NamedScope scope) {
    Job job = scope.lookup(name);
    if (job == null) {
      throw new Exception("Could not find job ${name} from scope ${scope.levelName}");
    }
    return job.clone();
  }

  static Job cloneJob(String name, String rename, NamedScope scope) {
    Job job = cloneJob(name, scope);
    job.name = rename;
    return job;
  }

  static Properties clonePropertyFile(String name, NamedScope scope) {
    Properties props = scope.lookup(name);
    if (props == null) {
      throw new Exception("Could not find propertyFile ${name} from scope ${scope.levelName}");
    }
    return props.clone();
  }

  static Properties clonePropertyFile(String name, String rename, NamedScope scope) {
    Properties props = clonePropertyFile(name, scope)
    props.name = rename;
    return props;
  }

  static PropertySet clonePropertySet(String name, NamedScope scope) {
    PropertySet propertySet = scope.lookup(name);
    if (propertySet == null) {
      throw new Exception("Could not find PropertySet ${name} from scope ${scope.levelName}");
    }
    return propertySet.clone(scope);
  }

  static PropertySet clonePropertySet(String name, String rename, NamedScope scope) {
    PropertySet propertySet = clonePropertySet(name, scope)
    propertySet.name = rename;
    return propertySet;
  }

  static Workflow cloneWorkflow(String name, NamedScope scope) {
    Workflow workflow = scope.lookup(name);
    if (workflow == null) {
      throw new Exception("Could not find workflow ${name} from scope ${scope.levelName}");
    }
    return workflow.clone(scope);
  }

  static Workflow cloneWorkflow(String name, String rename, NamedScope scope) {
    Workflow workflow = cloneWorkflow(name, scope);
    workflow.name = rename;
    workflow.scope.levelName = rename;
    return workflow;
  }

  static Job configureJob(Project project, Job job, Closure configure, NamedScope scope) {
    scope.bind(job.name, job);
    project.configure(job, configure);
    return job;
  }

  static Properties configureProperties(Project project, Properties props, Closure configure, NamedScope scope) {
    scope.bind(props.name, props);
    project.configure(props, configure);
    return props;
  }

  static PropertySet configurePropertySet(Project project, PropertySet propertySet, Closure configure, NamedScope scope) {
    scope.bind(propertySet.name, propertySet);
    project.configure(propertySet, configure);
    return propertySet;
  }

  static Workflow configureWorkflow(Project project, Workflow workflow, Closure configure, NamedScope scope) {
    scope.bind(workflow.name, workflow);
    project.configure(workflow, configure);
    return workflow;
  }

  static Object global(Object object, NamedScope scope) {
    if (scope.contains(object.name)) {
      throw new Exception("An object with name ${object.name} requested to be global is already bound in scope ${scope.levelName}");
    }
    scope.bind(object.name, object);
    return object;
  }

  static Object lookup(String name, NamedScope scope) {
    return scope.lookup(name);
  }

  static Object lookup(Project project, String name, NamedScope scope, Closure configure) {
    Object boundObject = lookup(name, scope);
    if (boundObject == null) {
      return null;
    }
    project.configure(boundObject, configure);
    return boundObject;
  }
}