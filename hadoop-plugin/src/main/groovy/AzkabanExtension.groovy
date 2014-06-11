import org.gradle.api.Project;

/**
 * AzkabanExtension will be the class that exposes the DSL to users. To use
 * the DSL, users should add an
 *
 * azkaban {
 *   ...
 * }
 *
 * configuration block to their build.gradle file.
 */
class AzkabanExtension {
  NamedScope azkabanScope;
  String jobConfDir;
  Project project;
  List<AzkabanProperties> properties;
  List<AzkabanWorkflow> workflows;

  AzkabanExtension(Project project) {
    this(project, null);
  }

  AzkabanExtension(Project project, NamedScope globalScope) {
    this.azkabanScope = new NamedScope("azkaban", globalScope);
    this.jobConfDir = null;
    this.project = project;
    this.properties = new ArrayList<AzkabanProperties>();
    this.workflows = new ArrayList<AzkabanJob>();
  }

  void build() throws IOException {
    if (jobConfDir == null || jobConfDir.isEmpty()) {
      throw new IOException("You must set the property jobConfDir to use the Azkaban DSL");
    }

    File file = new File(jobConfDir);
    if (!file.isDirectory() || !file.exists()) {
      throw new IOException("Directory ${jobConfDir} does not exist or is not a directory");
    }

    workflows.each() { workflow ->
      workflow.build(jobConfDir);
    }

    properties.each() { props ->
      props.build(jobConfDir);
    }
  }

  AzkabanProperties addPropertySet(String name, Closure configure) {
    AzkabanProperties props = azkabanScope.lookup(name);
    if (props == null) {
      throw new Exception("Could not find property set ${name} in call to addProperty");
    }
    AzkabanProperties clone = props.clone();
    azkabanScope.bind(name, clone);
    project.configure(clone, configure);
    properties.add(clone);
    return clone;
  }

  AzkabanProperties addPropertySet(String name, String rename, Closure configure) {
    AzkabanProperties props = azkabanScope.lookup(name);
    if (props == null) {
      throw new Exception("Could not find property set ${name} in call to addProperty");
    }
    AzkabanProperties clone = props.clone();
    clone.name = rename;
    azkabanScope.bind(rename, clone);
    project.configure(clone, configure);
    properties.add(clone);
    return clone;
  }

  AzkabanWorkflow addWorkflow(String name, Closure configure) {
    AzkabanWorkflow workflow = azkabanScope.lookup(name);
    if (workflow == null) {
      throw new Exception("Could not find workflow ${name} in call to addWorkflow");
    }
    AzkabanWorkflow clone = workflow.clone();
    clone.workflowScope.nextLevel = azkabanScope;
    azkabanScope.bind(name, clone);
    project.configure(clone, configure);
    workflows.add(clone);
    return clone;
  }

  AzkabanWorkflow addWorkflow(String name, String rename, Closure configure) {
    AzkabanWorkflow workflow = azkabanScope.lookup(name);
    if (workflow == null) {
      throw new Exception("Could not find workflow ${name} in call to addWorkflow");
    }
    AzkabanWorkflow clone = workflow.clone();
    clone.name = rename;
    clone.workflowScope.nextLevel = azkabanScope;
    azkabanScope.bind(rename, clone);
    project.configure(clone, configure);
    workflows.add(clone);
    return clone;
  }

  Object local(Object object) {
    if (azkabanScope.contains(object.name)) {
      throw new Exception("An object with name ${object.name} requested to be local is already bound in azkaban scope");
    }
    azkabanScope.bind(object.name, object);
    return object;
  }

  Object lookup(String name) {
    return azkabanScope.lookup(name);
  }

  Object lookup(String name, Closure configure) {
    Object boundObject = azkabanScope.lookup(name);
    if (boundObject == null) {
      return null;
    }
    project.configure(boundObject, configure);
    return boundObject;
  }

  AzkabanProperties propertySet(String name, Closure configure) {
    AzkabanProperties props = new AzkabanProperties(name);
    azkabanScope.bind(name, props);
    project.configure(props, configure);
    properties.add(props);
    return props;
  }

  AzkabanWorkflow workflow(String name, Closure configure) {
    AzkabanWorkflow flow = new AzkabanWorkflow(name, project, azkabanScope);
    azkabanScope.bind(name, flow);
    project.configure(flow, configure);
    workflows.add(flow);
    return flow;
  }
}