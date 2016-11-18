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
 * The BasePropertySet class is a common base class for PropertySet and Properties.
 */
abstract class BasePropertySet {
  String basePropertySetName;
  String name;
  Map<String, Object> confProperties;
  Map<String, Object> jobProperties;
  Map<String, Object> jvmProperties;

  /**
   * Base constructor for BasePropertySet.
   *
   * @param name The BasePropertySet name
   */
  BasePropertySet(String name) {
    this.basePropertySetName = null;
    this.name = name;
    this.confProperties = new LinkedHashMap<String, Object>();
    this.jobProperties = new LinkedHashMap<String, Object>();
    this.jvmProperties = new LinkedHashMap<String, Object>();
  }

  /**
   * DSL baseProperties method. Sets the name of the BasePropertySet from which to get the base
   * properties.
   *
   * @param propertySetName The name of the base PropertySet
   */
  void baseProperties(String propertySetName) {
    this.basePropertySetName = propertySetName;
  }

  /**
   * Helper method to set the properties on a cloned BasePropertySet object.
   *
   * @param clonePropertySet The BasePropertySet being cloned
   * @return The cloned BasePropertySet
   */
  protected BasePropertySet clone(BasePropertySet clonePropertySet) {
    clonePropertySet.basePropertySetName = this.basePropertySetName;
    clonePropertySet.confProperties.putAll(this.confProperties);
    clonePropertySet.jobProperties.putAll(this.jobProperties);
    clonePropertySet.jvmProperties.putAll(this.jvmProperties);
    return clonePropertySet;
  }

  /**
   * Returns the fully-qualified name for the BasePropertySet.
   *
   * @param parentScope The parent scope in which the BasePropertySet is bound
   * @return The fully-qualified name for the BasePropertySet
   */
  String getQualifiedName(NamedScope parentScope) {
    return (parentScope == null) ? name : "${parentScope.getQualifiedName()}.${name}";
  }

  /**
   * DSL method to specify properties for the BasePropertySet.
   * <p>
   * You can specify job properties by using the syntax "set properties: [ ... ]", which causes
   * causes lines of the form key=val to be written to the properties file.
   * <p>
   * You can specify JVM properties by using the syntax "set jvmProperties: [ ... ]", which causes
   * a line of the form jvm.args=-Dkey1=val1 ... -DkeyN=valN to be written to the properties file.
   * <p>
   * You can specify Hadoop job configuration properties by using the syntax
   * "set confProperties: [ ... ]", which causes lines of the form hadoop-inject.key=val to be
   * written to the properties file. To make this more clear, we've added the syntax
   * "set hadoopProperties: [ ... ]" as a synonym for setting confProperties.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set;
   *   or a key 'jvmProperties' with a map value that specifies the JVM properties to set;
   *   or a key 'confProperties' with a map value that specifies the Hadoop job configuration properties to set;
   *   or a key 'hadoopProperties' that is a synonym for 'confProperties';
   *   or a key 'parameters' with a map value that specifies the Hive parameters to set
   */
  void set(Map args) {
    if (args.containsKey("confProperties")) {
      Map<String, Object> confProperties = args.confProperties;
      confProperties.each { String name, Object value ->
        setConfProperty(name, value);
      }
    }
    if (args.containsKey("hadoopProperties")) {
      Map<String, Object> hadoopProperties = args.hadoopProperties;
      hadoopProperties.each { String name, Object value ->
        setConfProperty(name, value);
      }
    }
    if (args.containsKey("jvmProperties")) {
      Map<String, Object> jvmProperties = args.jvmProperties;
      jvmProperties.each { String name, Object value ->
        setJvmProperty(name, value);
      }
    }
    if (args.containsKey("properties")) {
      Map<String, Object> properties = args.properties;
      properties.each { String name, Object value ->
        setJobProperty(name, value);
      }
    }
  }

  /**
   * Sets the given Hadoop job configuration property.
   *
   * @param name The Hadoop job configuration property to set
   * @param value The Hadoop job configuration property value
   */
  void setConfProperty(String name, Object value) {
    confProperties.put(name, value);
  }

  /**
   * Sets the given job property.
   *
   * @param name The job property to set
   * @param value The job property value
   */
  void setJobProperty(String name, Object value) {
    jobProperties.put(name, value);
  }

  /**
   * Sets the given JVM property.
   *
   * @param name The JVM property name to set
   * @param value The JVM property value
   */
  void setJvmProperty(String name, Object value) {
    jvmProperties.put(name, value);
  }

  /**
   * Returns a string representation of the properties.
   *
   * @return A string representation of the properties
   */
  @Override
  String toString() {
    return "(BasePropertySet: name = ${name}, basePropertySetName = ${basePropertySetName}, confProperties = ${confProperties.toString()}, jobProperties = ${jobProperties.toString()}, jvmProperties = ${jvmProperties.toString()})";
  }

  /**
   * Adds any properties set on the given BasePropertySet (that are not already set), so that the
   * final set of properties set on the current object represents the union of the properties.
   *
   * @param propertySet The BasePropertySet to union to the current object
   */
  void unionProperties(BasePropertySet propertySet) {
    propertySet.confProperties.each { String name, Object value ->
      if (!confProperties.containsKey(name)) {
        setConfProperty(name, value);
      }
    }
    propertySet.jvmProperties.each { String name, Object value ->
      if (!jvmProperties.containsKey(name)) {
        setJvmProperty(name, value);
      }
    }
    propertySet.jobProperties.each { String name, Object value ->
      if (!jobProperties.containsKey(name)) {
        setJobProperty(name, value);
      }
    }
  }
}