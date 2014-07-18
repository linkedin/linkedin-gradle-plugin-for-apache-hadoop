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
      properties.each() { key, value ->
        out.writeLine("${key}=${value}");
      }
    }
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