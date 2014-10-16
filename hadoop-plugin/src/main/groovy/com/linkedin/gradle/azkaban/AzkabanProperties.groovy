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
package com.linkedin.gradle.azkaban;

/**
 * The AzkabanProperties class represents Azkaban property files. In the DSL, properties can be
 * specified with:
 * <pre>
 *   propertyFile('common') {
 *     set properties: [
 *       myPropertyA : 'valA',
 *       myPropertyB : 'valB'
 *     ]
 *   }
 * </pre>
 */
class AzkabanProperties {
  String name;
  Map<String, String> properties;

  /**
   * Base constructor for AzkabanProperties.
   *
   * @param name The properties name
   */
  AzkabanProperties(String name) {
    this.name = name;
    this.properties = new LinkedHashMap<String, String>();
  }

  /**
   * Builds an Azkaban properties file.
   *
   * @param directory The directory in which to build the property file
   * @param parentScope The fully-qualified name of the scope in which the properties object is bound
   */
  void build(String directory, String parentScope) throws IOException {
    if (this.properties.keySet().size() == 0) {
      return;
    }

    String fileName = buildFileName(name, parentScope);
    File file = new File(directory, "${fileName}.properties");

    file.withWriter { out ->
      out.writeLine("# This file generated from the Azkaban DSL. Do not edit by hand.");
      properties.each() { key, value ->
        out.writeLine("${key}=${value}");
      }
    }

    // Set to read-only to remind people that they should not be editing the job files.
    file.setWritable(false);
  }

  /**
   * Helper method to construct the name to use with the Azkaban properties file. By default, the name
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
   * Clones the properties.
   *
   * @return The cloned properties
   */
  AzkabanProperties clone() {
    return clone(new AzkabanProperties(name));
  }

  /**
   * Helper method to set the properties on a cloned AzkabanProperties object.
   *
   * @param The properties object being cloned
   * @return The cloned properties
   */
  AzkabanProperties clone(AzkabanProperties cloneProps) {
    cloneProps.properties.putAll(this.properties);
    return cloneProps;
  }

  /**
   * DSL method to set Azkaban properties.
   *
   * @param args Args whose key 'properties' has a map value containing the properties to set
   */
  void set(Map args) {
    Map<String, String> props = args.properties;
    properties.putAll(props);
  }

  /**
   * Returns a string representation of the properties.
   *
   * @return A string representation of the properties
   */
  String toString() {
    return "(AzkabanProperties: name = ${name}, properties = ${properties.toString()})";
  }
}
