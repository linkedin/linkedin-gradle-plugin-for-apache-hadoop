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

/**
 * The PropertySet class represents an in-memory only set of properties. To write a property file
 * instead, use the Properties class. In the DSL, a PropertySet can be specified with:
 * <pre>
 *   propertySet('common') {
 *     set confProperties: [
 *       'mapred.foo.bar' : 'foobar',
 *       'mapred.foo.bazz' : 'foobazz'
 *     ]
 *     set jvmProperties: [
 *       'jvmPropertyName1' : 'jvmPropertyValue1',
 *       'jvmPropertyName2' : 'jvmPropertyValue2'
 *     ]
 *     set properties: [
 *       myPropertyA : 'valA',
 *       myPropertyB : 'valB'
 *     ]
 *   }
 * </pre>
 */
class PropertySet extends BasePropertySet {
  NamedScope parentScope;

  /**
   * Constructor for a PropertySet.
   *
   * @param name The PropertySet name
   * @param parentScope The parent scope
   */
  PropertySet(String name, NamedScope parentScope) {
    super(name);
    this.parentScope = parentScope;
  }

  /**
   * Clones the PropertySet given its new parent scope.
   *
   * @param parentScope The parent scope
   * @return The cloned PropertySet
   */
  PropertySet clone(NamedScope parentScope) {
    return clone(new PropertySet(name, parentScope));
  }

  /**
   * Helper method to set the properties on a cloned PropertySet object.
   *
   * @param clonePropertySet The PropertySet being cloned
   * @return The cloned PropertySet
   */
  @Override
  PropertySet clone(PropertySet clonePropertySet) {
    return super.clone(clonePropertySet);
  }

  /**
   * Fills out the complete set of properties for this PropertySet by recursively filling out its
   * base properties and then unioning them to the current PropertySet.
   */
  void fillProperties() {
    if (basePropertySetName != null) {
      PropertySet propertySet = (PropertySet) parentScope.lookup(basePropertySetName);
      propertySet.fillProperties();    // The base property set looks up its base properties in its own scope
      unionProperties(propertySet);
    }
  }

  /**
   * Returns a string representation of the PropertySet.
   *
   * @return A string representation of the PropertySet
   */
  @Override
  String toString() {
    return "(PropertySet: name = ${name}, basePropertySetName = ${basePropertySetName}, confProperties = ${confProperties.toString()}, jobProperties = ${jobProperties.toString()}, jvmProperties = ${jvmProperties.toString()})";
  }
}