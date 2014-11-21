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
 * The Properties class represents property files. In the DSL, properties can be specified with:
 * <pre>
 *   propertyFile('common') {
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
class Properties {
  String name;
  Map<String, String> confProperties;
  Map<String, String> jvmProperties;
  Map<String, String> properties;

  /**
   * Base constructor for Properties.
   *
   * @param name The properties name
   */
  Properties(String name) {
    this.name = name;
    this.confProperties = new LinkedHashMap<String, String>();
    this.jvmProperties = new LinkedHashMap<String, String>();
    this.properties = new LinkedHashMap<String, String>();
  }

  /**
   * Helper method to construct the name to use with the properties file. By default, the name
   * constructed is "${parentScope}_${name}", but subclasses can override this method if they need
   * to customize how the name is constructed.
   *
   * @param name The job name
   * @param parentScope The fully-qualified name of the scope in which the properties object is bound
   * @return The name to use when generating the properties file
   */
  String buildFileName(String name, String parentScope) {
    return parentScope == null ? name : "${parentScope}_${name}";
  }

  /**
   * Builds the properties that go into the generated properties file.
   * <p>
   * Subclasses can override this method to add their own properties, and are recommended to
   * additionally call this base class method to add the jvmProperties and properties correctly.
   *
   * @param allProperties The map that holds all the properties that will go into the built properties file
   * @return The input properties map, with jvmProperties and properties added
   */
  Map<String, String> buildProperties(Map<String, String> allProperties) {
    allProperties.putAll(properties);

    if (jvmProperties.size() > 0) {
      String jvmArgs = jvmProperties.collect() { key, val -> return "-D${key}=${val}" }.join(" ");
      allProperties["jvm.args"] = jvmArgs;
    }

    return allProperties;
  }

  /**
   * Clones the properties.
   *
   * @return The cloned properties
   */
  Properties clone() {
    return clone(new Properties(name));
  }

  /**
   * Helper method to set the properties on a cloned Properties object.
   *
   * @param The properties object being cloned
   * @return The cloned properties
   */
  Properties clone(Properties cloneProps) {
    cloneProps.confProperties.putAll(this.confProperties);
    cloneProps.jvmProperties.putAll(this.jvmProperties);
    cloneProps.properties.putAll(this.properties);
    return cloneProps;
  }

  /**
   * DSL method to specify job properties for the property file. Specifying job properties causes
   * lines of the form key=val to be written to the properties file.
   * <p>
   * You can specify JVM properties by using the syntax "set jvmProperties: [ ... ]", which causes
   * a line of the form jvm.args=-Dkey1=val1 ... -DkeyN=valN to be written to the properties file.
   * <p>
   * You can specify Hadoop job configuration properties by using the syntax "set confProperties: [ ... ]",
   * which causes lines of the form hadoop-conf.key=val to be written to the properties file.
   *
   * @param args Args whose key 'properties' has a map value specifying the job properties to set;
   *   or a key 'jvmProperties' with a map value that specifies the JVM properties to set;
   *   or a key 'confProperties' with a map value that specifies the Hadoop job configuration properties to set
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
   * Sets the given Hadoop job configuration property. For a given key and value, this method
   * causes the line hadoop-conf.key=val to be added to the properties file.
   *
   * @param name The Hadoop job configuration property to set
   * @param value The Hadoop job configuration property value
   */
  void setConfProperty(String name, String value) {
    confProperties.put(name, value);
    setJobProperty("hadoop-conf.${name}", value);
  }

  /**
   * Sets the given property. Setting a property causes a line of the form key=val to be written to
   * the properties file.
   *
   * @param name The job property to set
   * @param value The job property value
   */
  void setJobProperty(String name, String value) {
    properties.put(name, value);
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
    return "(Properties: name = ${name}, confProperties = ${confProperties.toString()}, jvmProperties = ${jvmProperties.toString()}, properties = ${properties.toString()})";
  }
}