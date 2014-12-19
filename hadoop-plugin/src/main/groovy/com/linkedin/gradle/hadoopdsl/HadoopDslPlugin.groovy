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

import org.gradle.api.Plugin;
import org.gradle.api.Project;

/**
 * HadoopDslPlugin implements features for the Hadoop DSL.
 */
class HadoopDslPlugin extends BaseNamedScopeContainer implements Plugin<Project> {
  HadoopDslExtension extension;

  /**
   * Constructor for the Hadoop DSL Plugin.
   */
  HadoopDslPlugin() {
    super(null, "");
  }

  /**
   * Applies the Hadoop DSL Plugin, which sets up the extensions and methods necessary to use the
   * Hadoop DSL.
   *
   * @param project The Gradle project
   */
  @Override
  void apply(Project project) {
    this.factory = makeFactory();
    this.project = project;

    project.extensions.add("hadoopDslFactory", factory);
    project.extensions.add("hadoopDslPlugin", this);

    // You must have the hadoopDslFactory extension set before you can make the HadoopDslExtension
    this.extension = factory.makeExtension(project, scope);
    project.extensions.add("hadoop", extension);

    // Expose the DSL global method, which is only implemented by the HadoopDslPlugin class.
    project.extensions.add("global", this.&global);

    // Add the extensions that expose the DSL to users. Specifically, expose all of the DSL
    // functions on the NamedScopeContainer interface.
    project.extensions.add("addJob", this.&addJob);
    project.extensions.add("addPropertyFile", this.&addPropertyFile);
    project.extensions.add("addPropertySet", this.&addPropertySet);
    project.extensions.add("addWorkflow", this.&addWorkflow);
    project.extensions.add("lookup", this.&lookup);
    project.extensions.add("propertyFile", this.&propertyFile);
    project.extensions.add("propertySet", this.&propertySet);
    project.extensions.add("workflow", this.&workflow);
    project.extensions.add("commandJob", this.&commandJob);
    project.extensions.add("hadoopJavaJob", this.&hadoopJavaJob);
    project.extensions.add("hiveJob", this.&hiveJob);
    project.extensions.add("javaJob", this.&javaJob);
    project.extensions.add("javaProcessJob", this.&javaProcessJob);
    project.extensions.add("job", this.&job);
    project.extensions.add("kafkaPushJob", this.&kafkaPushJob);
    project.extensions.add("noOpJob", this.&noOpJob);
    project.extensions.add("pigJob", this.&pigJob);
    project.extensions.add("voldemortBuildPushJob", this.&voldemortBuildPushJob);
  }

  /**
   * Clones the scope container given its new parent scope.
   *
   * @param parentScope The new parent scope
   * @return The cloned scope container
   */
  @Override
  HadoopDslPlugin clone(NamedScope parentScope) {
    throw new Exception("The Hadoop DSL Plugin is a singleton and cannot be cloned.")
  }

  /**
   * DSL global method. Binds the object in global scope.
   *
   * @param object The object to bind in global scope
   * @return The object, now bound in global scope
   */
  Object global(Object object) {
    return Methods.global(object, scope);
  }

  /**
   * Factory method to return the Hadoop DSL Factory. Can be overridden by subclasses that wish to
   * provide their own factory.
   *
   * @return The factory to use
   */
  HadoopDslFactory makeFactory() {
    return new HadoopDslFactory();
  }
}