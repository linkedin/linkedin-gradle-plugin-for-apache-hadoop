package com.linkedin.gradle.hadoop;

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
class AzkabanExtension implements NamedScopeContainer {
  AzkabanFactory azkabanFactory;
  NamedScope azkabanScope;
  boolean cleanFirst;
  String jobConfDir;
  Project project;
  List<AzkabanProperties> properties;
  List<AzkabanWorkflow> workflows;

  AzkabanExtension(Project project) {
    this(project, null);
  }

  AzkabanExtension(Project project, NamedScope globalScope) {
    this.azkabanFactory = project.extensions.azkabanFactory;
    this.azkabanScope = new NamedScope("azkaban", globalScope);
    this.cleanFirst = true;
    this.jobConfDir = null;
    this.project = project;
    this.properties = new ArrayList<AzkabanProperties>();
    this.workflows = new ArrayList<AzkabanJob>();

    // Bind the name azkaban in the global scope so that we can do fully-qualified name lookups
    // starting from the global scope.
    globalScope.bind("azkaban", this);
  }

  @Override
  public NamedScope getScope() {
    return azkabanScope;
  }

  void build() throws IOException {
    if (jobConfDir == null || jobConfDir.isEmpty()) {
      throw new IOException("You must set the property jobConfDir to use the Azkaban DSL");
    }

    File file = new File(jobConfDir);
    if (!file.isDirectory() || !file.exists()) {
      throw new IOException("Directory ${jobConfDir} does not exist or is not a directory");
    }

    if (cleanFirst) {
      file.eachFileRecurse(groovy.io.FileType.FILES) { f ->
        String fileName = f.getName().toLowerCase();
        if (fileName.endsWith(".job") || fileName.endsWith(".properties")) {
          f.delete();
        }
      }
    }

    workflows.each() { workflow ->
      workflow.build(jobConfDir);
    }

    properties.each() { props ->
      props.build(jobConfDir, null);
    }
  }

  void buildPath(String buildDir) {
    if (buildDir.startsWith("/")) {
      jobConfDir = buildDir;
    }
    else {
      jobConfDir = new File("${project.projectDir}", buildDir).getPath();
    }
  }

  void cleanPath(boolean cleanFirst) {
    this.cleanFirst = cleanFirst;
  }

  // Helper method to configure AzkabanProperties in the DSL. Can be called by subclasses to
  // configure custom AzkabanProperties subclass types.
  AzkabanProperties configureProperties(AzkabanProperties props, Closure configure) {
    azkabanScope.bind(props.name, props);
    project.configure(props, configure);
    properties.add(props);
    return props;
  }

  // Helper method to configure AzkabanWorkflow in the DSL. Can be called by subclasses to
  // configure custom AzkabanWorkflow subclass types.
  AzkabanWorkflow configureWorkflow(AzkabanWorkflow workflow, Closure configure) {
    azkabanScope.bind(workflow.name, workflow);
    project.configure(workflow, configure);
    workflows.add(workflow);
    return workflow;
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

  AzkabanProperties addPropertyFile(String name, Closure configure) {
    AzkabanProperties props = azkabanScope.lookup(name);
    if (props == null) {
      throw new Exception("Could not find propertyFile ${name} in call to addPropertyFile");
    }
    AzkabanProperties clone = props.clone();
    return configureProperties(clone, configure);
  }

  AzkabanProperties addPropertyFile(String name, String rename, Closure configure) {
    AzkabanProperties props = azkabanScope.lookup(name);
    if (props == null) {
      throw new Exception("Could not find propertyFile ${name} in call to addPropertyFile");
    }
    AzkabanProperties clone = props.clone();
    clone.name = rename;
    return configureProperties(clone, configure);
  }

  AzkabanWorkflow addWorkflow(String name, Closure configure) {
    AzkabanWorkflow workflow = azkabanScope.lookup(name);
    if (workflow == null) {
      throw new Exception("Could not find workflow ${name} in call to addWorkflow");
    }
    AzkabanWorkflow clone = workflow.clone();
    clone.workflowScope.nextLevel = azkabanScope;
    return configureWorkflow(clone, configure);
  }

  AzkabanWorkflow addWorkflow(String name, String rename, Closure configure) {
    AzkabanWorkflow workflow = azkabanScope.lookup(name);
    if (workflow == null) {
      throw new Exception("Could not find workflow ${name} in call to addWorkflow");
    }
    AzkabanWorkflow clone = workflow.clone();
    clone.name = rename;
    clone.workflowScope.nextLevel = azkabanScope;
    return configureWorkflow(clone, configure);
  }

  AzkabanProperties propertyFile(String name, Closure configure) {
    AzkabanProperties props = azkabanFactory.makeAzkabanProperties(name);
    return configureProperties(props, configure);
  }

  AzkabanWorkflow workflow(String name, Closure configure) {
    AzkabanWorkflow flow = azkabanFactory.makeAzkabanWorkflow(name, project, azkabanScope);
    return configureWorkflow(flow, configure);
  }
}