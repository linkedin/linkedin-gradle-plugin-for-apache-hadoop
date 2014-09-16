package com.linkedin.gradle.azkaban;

class AzkabanProperties {
  String name;
  Map<String, String> properties;

  AzkabanProperties(String name) {
    this.name = name;
    this.properties = new LinkedHashMap<String, String>();
  }

  void build(String directory, String parentName) throws IOException {
    if (this.properties.keySet().size() == 0) {
      return;
    }

    String fileName = parentName == null ? name : "${parentName}-${name}";
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

  AzkabanProperties clone() {
    AzkabanProperties props = new AzkabanProperties(name);
    props.properties.putAll(this.properties);
    return props;
  }

  void set(Map args) {
    Map<String, String> props = args.properties;
    properties.putAll(props);
  }

  String toString() {
    return "(AzkabanProperties: name = ${name}, properties = ${properties.toString()})";
  }
}