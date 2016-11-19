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
package com.linkedin.gradle.hadoopdsl;

/**
 * The Properties class represents property files. In the DSL, properties can be specified with:
 * <pre>
 *   propertySet('global') {
 *     set properties: [
 *       myPropertyA : 'valA'
 *     ]
 *   }
 *
 *   propertyFile('common') {
 *     baseProperties 'global'
 *     set confProperties: [
 *       'mapred.foo.bar' : 'foobar',
 *       'mapred.foo.bazz' : 'foobazz'
 *     ]
 *     set jvmProperties: [
 *       'jvmPropertyName1' : 'jvmPropertyValue1',
 *       'jvmPropertyName2' : 'jvmPropertyValue2'
 *     ]
 *     set properties: [
 *       myPropertyB : 'valB',
 *       myPropertyC : 'valC'
 *     ]
 *   }
 * </pre>
 */
class Properties extends BasePropertySet {
  /**
   * Base constructor for Properties.
   *
   * @param name The properties name
   */
  Properties(String name) {
    super(name);
  }

  /**
   * Method to construct the file name to use for the properties file.
   * <p>
   * We'll use the fully-qualified name to help us form the file name. However, to make the file
   * name more readable, we'll use underscores and drop the hadoop scope prefix from the file name.
   * <p>
   * As an example, if the properties object named "properties1" is nested inside the workflow
   * "testWorkflow" in Hadoop scope, the file name will be "testWorkflow_properties1".
   *
   * @param parentScope The parent scope in which the properties object is bound
   * @return The name to use when generating the properties file
   */
  String buildFileName(NamedScope parentScope) {
    return getQualifiedName(parentScope).replaceFirst("hadoop.", "").replace('.', '_');
  }

  /**
   * Builds the properties that go into the generated properties file.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and properties correctly.
   *
   * @param parentScope The parent scope in which to lookup the base properties
   * @return The map that holds all the properties that will go into the built properties file
   */
  Map<String, String> buildProperties(NamedScope parentScope) {
    if (basePropertySetName != null) {
      PropertySet propertySet = (PropertySet)parentScope.lookup(basePropertySetName);
      propertySet.fillProperties();    // The base property set looks up its base properties in its own scope
      unionProperties(propertySet);
    }

    Map<String, String> allProperties = new LinkedHashMap<String, String>();

    jobProperties.each { String key, Object val ->
      allProperties.put(key, val.toString());
    }

    if (jvmProperties.size() > 0) {
      String jvmArgs = jvmProperties.collect { key, val -> return "-D${key}=${val.toString()}" }.join(" ");
      allProperties["jvm.args"] = jvmArgs;
    }

    return allProperties;
  }

  /**
   * Clones the properties given its new parent scope.
   *
   * @param parentScope The new parent scope
   * @return The cloned properties
   */
  @Override
  Properties clone() {
    return clone(new Properties(name));
  }

  /**
   * Helper method to set the properties on a cloned Properties object.
   *
   * @param cloneProperties The properties object being cloned
   * @return The cloned properties
   */
  protected Properties clone(Properties cloneProperties) {
    return ((Properties)super.clone(cloneProperties));
  }

  /**
   * Sets the given Hadoop job configuration property.
   *
   * @param name The Hadoop job configuration property to set
   * @param value The Hadoop job configuration property value
   */
  @HadoopDslMethod
  @Override
  void setConfProperty(String name, Object value) {
    super.setConfProperty(name, value);
    setJobProperty("hadoop-inject.${name}", value);
  }

  /**
   * Returns a string representation of the properties.
   *
   * @return A string representation of the properties
   */
  @Override
  String toString() {
    return "(Properties: name = ${name}, basePropertySetName = ${basePropertySetName}, confProperties = ${confProperties.toString()}, jobProperties = ${jobProperties.toString()}, jvmProperties = ${jvmProperties.toString()})";
  }
}