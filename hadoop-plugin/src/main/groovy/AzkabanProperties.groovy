class AzkabanProperties extends LinkedHashMap<String, String> {
  String name;

  AzkabanProperties(String name) {
    super();
    this.name = name;
    if (name.toLowerCase().endsWith(".properties")) {
      throw new Exception("Do not add the .properties extension as it will be added automatically");
    }
  }

  void build(String directory) throws IOException {
    if (this.keySet().size() == 0) {
      return;
    }

    File file = new File(directory, "${name}.properties");
    file.withWriter { out ->
      this.each() { key, value ->
        out.writeLine("${key}=${value}");
      }
    }
  }

  AzkabanProperties clone() {
    AzkabanProperties props = new AzkabanProperties(name);
    props.putAll(this);
    return props;
  }
}