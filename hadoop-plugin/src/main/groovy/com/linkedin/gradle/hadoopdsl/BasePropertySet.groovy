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
 * The BasePropertySet class is a common base class for PropertySet and Properties.
 */
abstract class BasePropertySet {
  String name;
  Map<String, String> confProperties;
  Map<String, String> jobProperties;
  Map<String, String> jvmProperties;

  /**
   * Base constructor for BasePropertySet.
   *
   * @param name The BasePropertySet name
   */
  BasePropertySet(String name) {
    this.name = name;
    this.confProperties = new LinkedHashMap<String, String>();
    this.jobProperties = new LinkedHashMap<String, String>();
    this.jvmProperties = new LinkedHashMap<String, String>();
  }

  /**
   * Builds the properties that go into the generated properties file.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and properties correctly.
   *
   * @param allProperties The map that holds all the properties that will go into the built properties file
   * @return The input properties map with all the properties added
   */
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties.putAll(jobProperties);

    if (jvmProperties.size() > 0) {
      String jvmArgs = jvmProperties.collect() { key, val -> return "-D${key}=${val}" }.join(" ");
      allProperties["jvm.args"] = jvmArgs;
    }

    return allProperties;
  }

  /**
   * Clones the BasePropertySet.
   *
   * @return The cloned BasePropertySet
   */
  abstract BasePropertySet clone();

  /**
   * Helper method to set the properties on a cloned BasePropertySet object.
   *
   * @param The BasePropertySet being cloned
   * @return The cloned BasePropertySet
   */
  BasePropertySet clone(BasePropertySet clonePropertySet) {
    clonePropertySet.confProperties.putAll(this.confProperties);
    clonePropertySet.jobProperties.putAll(this.jobProperties);
    clonePropertySet.jvmProperties.putAll(this.jvmProperties);
    return clonePropertySet;
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
   * "set confProperties: [ ... ]", which causes lines of the form hadoop-conf.key=val to be
   * written to the properties file.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set;
   *   or a key 'jvmProperties' with a map value that specifies the JVM properties to set;
   *   or a key 'confProperties' with a map value that specifies the Hadoop job configuration properties to set;
   *   or a key 'parameters' with a map value that specifies the Hive parameters to set
   */
  void set(Map args) {
    if (args.containsKey("confProperties")) {
      Map<String, String> confProperties = args.confProperties;
      confProperties.each() { String name, String value ->
        setConfProperty(name, value);
      }
    }
    if (args.containsKey("jvmProperties")) {
      Map<String, String> jvmProperties = args.jvmProperties;
      jvmProperties.each() { name, value ->
        setJvmProperty(name, value);
      }
    }
    if (args.containsKey("properties")) {
      Map<String, String> properties = args.properties;
      properties.each() { String name, String value ->
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
  void setConfProperty(String name, String value) {
    confProperties.put(name, value);
    setJobProperty("hadoop-conf.${name}", value);
  }

  /**
   * Sets the given job property.
   *
   * @param name The job property to set
   * @param value The job property value
   */
  void setJobProperty(String name, String value) {
    jobProperties.put(name, value);
  }

  /**
   * Sets the given JVM property.
   *
   * @param name The JVM property name to set
   * @param value The JVM property value
   */
  void setJvmProperty(String name, String value) {
    jvmProperties.put(name, value);
  }

  /**
   * Returns a string representation of the properties.
   *
   * @return A string representation of the properties
   */
  @Override
  String toString() {
    return "(BasePropertySet: name = ${name}, confProperties = ${confProperties.toString()}, jobProperties = ${jobProperties.toString()}, jvmProperties = ${jvmProperties.toString()})";
  }

  /**
   * Adds any properties set on the given BasePropertySet (that are not already set), so that the
   * final set of properties set on the current object represents the union of the properties.
   *
   * @param propertySet The BasePropertySet to union to the current object
   */
  void unionProperties(BasePropertySet propertySet) {
    propertySet.confProperties.each() { String name, String value ->
      if (!confProperties.containsKey(name)) {
        setConfProperty(name, value);
      }
    }
    propertySet.jvmProperties.each() { name, value ->
      if (!jvmProperties.containsKey(name)) {
        setJvmProperty(name, value);
      }
    }
    propertySet.jobProperties.each() { String name, String value ->
      if (!jobProperties.containsKey(name)) {
        setJobProperty(name, value);
      }
    }
  }
}
