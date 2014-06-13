class AzkabanProperties extends LinkedHashMap<String, String> {
  String name;

  AzkabanProperties(String name) {
    super();
    this.name = name;
    if (name.toLowerCase().endsWith(".properties")) {
      throw new Exception("Do not add the .properties extension as it will be added automatically");
    }
  }

  void build(String directory, String parentName) throws IOException {
    if (this.keySet().size() == 0) {
      return;
    }

    String fileName = parentName == null ? name : "${parentName}-${name}";
    File file = new File(directory, "${fileName}.properties");

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

  void set(Map args) {
    Map<String, String> properties = args.properties;
    this.putAll(properties);
  }
}