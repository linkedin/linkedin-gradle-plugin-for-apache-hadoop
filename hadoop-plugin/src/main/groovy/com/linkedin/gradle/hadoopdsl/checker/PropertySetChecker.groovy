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
package com.linkedin.gradle.hadoopdsl.checker;

import com.linkedin.gradle.hadoopdsl.BaseStaticChecker;
import com.linkedin.gradle.hadoopdsl.Properties;
import com.linkedin.gradle.hadoopdsl.PropertySet;
import com.linkedin.gradle.hadoopdsl.job.Job;

import org.gradle.api.Project;

/**
 * The PropertySetChecker makes the following checks:
 * <ul>
 *   <li>ERROR if a Job has a value for baseProperties that is not in scope or is not bound to a PropertySet</li>
 *   <li>ERROR if a Properties object has a value for baseProperties that is not in scope or is not bound to a PropertySet</li>
 *   <li>ERROR if a PropertySet has a value for baseProperties, but has a null parent scope</li>
 *   <li>ERROR if a PropertySet has a value for baseProperties that equals its own name</li>
 *   <li>ERROR if a PropertySet has a cycle among its baseProperties</li>
 *   <li>ERROR if a PropertySet has a value for baseProperties that is not in scope or is not bound to a PropertySet</li>
 * </ul>
 */
class PropertySetChecker extends BaseStaticChecker {
  Set<PropertySet> propertySetsChecked;

  /**
   * Constructor for the PropertySetChecker.
   *
   * @param project The Gradle project
   */
  PropertySetChecker(Project project) {
    super(project);
    propertySetsChecked = new LinkedHashSet<PropertySet>();
  }

  @Override
  void visitJob(Job job) {
    String basePropertySetName = job.basePropertySetName;
    if (basePropertySetName == null)
      return;

    Object object = parentScope.lookup(basePropertySetName);

    // ERROR if a Job has a value for baseProperties that is not in scope
    if (object == null) {
      project.logger.lifecycle("PropertySetChecker ERROR: job ${job.name} declares baseProperties ${basePropertySetName} that is not bound in scope");
      foundError = true;
      return;
    }

    // ERROR if a Job has a value for baseProperties that is not bound to a PropertySet
    if (!(object instanceof PropertySet)) {
      project.logger.lifecycle("PropertySetChecker ERROR: job ${job.name} declares baseProperties ${basePropertySetName} that refers to an object that is not a PropertySet");
      foundError = true;
      return;
    }

    visitPropertySet((PropertySet) object);
  }

  @Override
  void visitProperties(Properties props) {
    String basePropertySetName = props.basePropertySetName;
    if (basePropertySetName == null)
      return;

    Object object = parentScope.lookup(basePropertySetName);

    // ERROR if a Properties object has a value for baseProperties that is not in scope
    if (object == null) {
      project.logger.lifecycle("PropertySetChecker ERROR: properties ${props.name} declares baseProperties ${basePropertySetName} that is not bound in scope");
      foundError = true;
      return;
    }

    // ERROR if a Properties object has a value for baseProperties that is not bound to a PropertySet
    if (!(object instanceof PropertySet)) {
      project.logger.lifecycle("PropertySetChecker ERROR: properties ${props.name} declares baseProperties ${basePropertySetName} that refers to an object that is not a PropertySet");
      foundError = true;
      return;
    }

    visitPropertySet((PropertySet) object);
  }

  @Override
  void visitPropertySet(PropertySet propertySet) {
    if (!propertySetsChecked.contains(propertySet)) {
      visitPropertySet(propertySet, new LinkedHashSet<String>());
      propertySetsChecked.add(propertySet);
    }
  }

  void visitPropertySet(PropertySet propertySet, Set<String> propertySetNames) {
    if (propertySet.basePropertySetName == null) {
      propertySetsChecked.add(propertySet);
      return;
    }

    String basePropertySetName = propertySet.basePropertySetName;

    // ERROR if a PropertySet has a value for baseProperties, but has a null parent scope
    if (propertySet.parentScope == null) {
      project.logger.lifecycle("PropertySetChecker ERROR: ${propertySet.name} declares baseProperties ${basePropertySetName}, but has a null parent scope value");
      foundError = true;
      return;
    }

    // ERROR if a PropertySet has a value for baseProperties that equals its own name
    if (basePropertySetName.equals(propertySet.name)) {
      project.logger.lifecycle("PropertySetChecker ERROR: ${propertySet.name} declares baseProperties ${basePropertySetName} with the same name as itself");
      foundError = true;
      return;
    }

    // ERROR if a PropertySet has a cycle among its baseProperties
    if (propertySetNames.contains(propertySet.name)) {
      String cycleText = buildCyclesText(propertySetNames, propertySet.name);
      project.logger.lifecycle("PropertySetChecker ERROR: ${propertySet.name} has a baseProperties cycle: ${cycleText}");
      foundError = true;
      return;
    }

    propertySetNames.add(propertySet.name);

    // Be sure to lookup the baseProperties in the scope in which the property set was declared
    Object object = propertySet.parentScope.lookup(basePropertySetName);

    // ERROR if a PropertySet has a value for baseProperties that is not in scope
    if (object == null) {
      project.logger.lifecycle("PropertySetChecker ERROR: ${propertySet.name} declares baseProperties ${basePropertySetName} that is not bound in scope");
      foundError = true;
      return;
    }

    // ERROR if a PropertySet has a value for baseProperties that is not bound to a PropertySet
    if (!(object instanceof PropertySet)) {
      project.logger.lifecycle("PropertySetChecker ERROR: ${propertySet.name} declares baseProperties ${basePropertySetName} that refers to an object that is not a PropertySet");
      foundError = true;
      return;
    }

    PropertySet basePropertySet = (PropertySet) object;

    if (!propertySetsChecked.contains(basePropertySet)) {
      visitPropertySet(basePropertySet, propertySetNames);
      propertySetsChecked.add(basePropertySet);
    }
  }
}