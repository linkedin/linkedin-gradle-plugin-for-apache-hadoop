package com.linkedin.gradle.azkaban;

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
    AzkabanMethods.configureProperties(project, props, configure, azkabanScope);
    properties.add(props);
    return props;
  }

  // Helper method to configure AzkabanWorkflow in the DSL. Can be called by subclasses to
  // configure custom AzkabanWorkflow subclass types.
  AzkabanWorkflow configureWorkflow(AzkabanWorkflow workflow, Closure configure) {
    AzkabanMethods.configureWorkflow(project, workflow, configure, azkabanScope);
    workflows.add(workflow);
    return workflow;
  }

  AzkabanProperties addPropertyFile(String name, Closure configure) {
    return configureProperties(AzkabanMethods.clonePropertyFile(name, azkabanScope), configure);
  }

  AzkabanProperties addPropertyFile(String name, String rename, Closure configure) {
    return configureProperties(AzkabanMethods.clonePropertyFile(name, rename, azkabanScope), configure);
  }

  AzkabanWorkflow addWorkflow(String name, Closure configure) {
    return configureWorkflow(AzkabanMethods.cloneWorkflow(name, azkabanScope), configure);
  }

  AzkabanWorkflow addWorkflow(String name, String rename, Closure configure) {
    return configureWorkflow(AzkabanMethods.cloneWorkflow(name, rename, azkabanScope), configure);
  }

  Object lookup(String name) {
    return AzkabanMethods.lookup(name, azkabanScope);
  }

  Object lookup(String name, Closure configure) {
    return AzkabanMethods.lookup(project, name, azkabanScope, configure);
  }

  AzkabanProperties propertyFile(String name, Closure configure) {
    return configureProperties(azkabanFactory.makeAzkabanProperties(name), configure);
  }

  AzkabanWorkflow workflow(String name, Closure configure) {
    return configureWorkflow(azkabanFactory.makeAzkabanWorkflow(name, project, azkabanScope), configure);
  }
}