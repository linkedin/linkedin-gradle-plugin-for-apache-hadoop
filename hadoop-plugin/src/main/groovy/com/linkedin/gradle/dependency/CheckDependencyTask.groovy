/*
 * Copyright 2015 LinkedIn Corp.
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
package com.linkedin.gradle.dependency;

import groovy.json.JsonSlurper;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.tasks.TaskAction;

class CheckDependencyTask extends DefaultTask {
  String DEPENDENCY_PATTERN_FILE = "sample-dependency-pattern.json";

  boolean errorFound = false;
  boolean warningFound = false;

  @TaskAction
  void checkDependencies() {
    Map<Dependency, List<DependencyPattern>> matchedDependencyPatterns;

    // Get all the dependencies used in the project
    Set<Dependency> dependencies = getDependencies(project);
    project.logger.debug("Dependencies found : ${dependencies.toString()}")

    // Get the List of DependencyPattern against which the dependencies should be checked
    List<DependencyPattern> dependencyPatterns = getDependencyPatterns(project);
    project.logger.debug("DependencyPatterns found : ${dependencyPatterns.toString()}")

    matchedDependencyPatterns = fillMatchedDependencyPatterns(dependencies, dependencyPatterns);
    matchedDependencyPatterns.each { dependency, matchedPatterns ->
      matchedPatterns.each { matchedPattern -> handleDependencyCheck(project, dependency, matchedPattern); }
    }
  }

  /**
   * Fills matchedDependencyPatterns.
   *
   * @param dependencies The set of dependencies
   * @param dependencyPatterns The list of dependency patterns.
   */
  Map<Dependency, List<DependencyPattern>> fillMatchedDependencyPatterns(Set<Dependency> dependencies, List<DependencyPattern> dependencyPatterns) {
    Map<Dependency, List<DependencyPattern>> matchedDependencyPatterns = new HashMap<Dependency, List<DependencyPattern>>();

    dependencies.each { dependency ->
      dependencyPatterns.each { dependencyPattern ->
        // If any dependency matches dependencyPattern then put it in matchedDependencyPatterns
        if (dependencyMatchesPattern(project, dependency, dependencyPattern)) {
          if (!matchedDependencyPatterns.containsKey(dependency)) {
            matchedDependencyPatterns.put(dependency, new LinkedList<DependencyPattern>());
          }
          matchedDependencyPatterns.get(dependency).add(dependencyPattern);
        }
      }
    }

    return matchedDependencyPatterns;
  }

  /**
   * This method handles the check of dependencies and calls other handlers based on the severity.
   *
   * @param project The Gradle project
   * @param dependency The dependency to check
   * @param matchedPattern The matchedPattern against which dependency should be checked
   */
  void handleDependencyCheck(Project project, Dependency dependency, DependencyPattern matchedPattern) {
    switch (matchedPattern.severity) {
      case SEVERITY.ERROR:
        errorFound = true;
        handleError(project, dependency, matchedPattern);
        break;
      case [SEVERITY.INFO, null]:
        handleInfo(project, dependency, matchedPattern);
        break;
      case SEVERITY.WARN:
        warningFound = true;
        handleWarn(project, dependency, matchedPattern);
        break;
    }
  }

  /**
   * Method to handle error. Subclasses can override this method to handle error in their own way.
   *
   * @param project The Gradle project
   * @param dependency The dependency to check
   * @param matchedPattern The matchedPattern against which dependency should be checked
   */
  void handleError(Project project, Dependency dependency, DependencyPattern matchedPattern) {
    project.logger.error("ERROR: ($dependency.group:$dependency.name:$dependency.version), ${matchedPattern.getMessage()}");
  }

  /**
   * Method to handle info. Subclasses can override this method to handle info in their own way.
   *
   * @param project The Gradle project
   * @param dependency The dependency to check
   * @param matchedPattern The matchedPattern against which dependency should be checked
   */
  void handleInfo(Project project, Dependency dependency, DependencyPattern matchedPattern) {
    project.logger.lifecycle("INFO: ($dependency.group:$dependency.name:$dependency.version), ${matchedPattern.getMessage()}");
  }

  /**
   * Method to handle warning. Subclasses can override this method to handle warning in their own way.
   *
   * @param project The Gradle project
   * @param dependency The dependency to check
   * @param matchedPattern The matchedPattern against which dependency should be checked
   */
  void handleWarn(Project project, Dependency dependency, DependencyPattern matchedPattern) {
    project.logger.warn("WARN: ($dependency.group:$dependency.name:$dependency.version), ${matchedPattern.getMessage()}");
  }

  /**
   * Returns true if the dependency matches the dependencyPattern.
   *
   * @param dependency The dependency to match
   * @param dependencyPattern The dependencyPattern against which dependency should be matched
   * @return true or false based on whether match is found or not
   */
  Boolean dependencyMatchesPattern(Project project, Dependency dependency, DependencyPattern dependencyPattern) {
    // If any of the group, name and version are not specified then return false
    if (dependency.group == null || dependency.name == null || dependency.version == null) {
      return false;
    }

    Boolean matchFound = true;

    if (dependencyPattern.getGroupPattern() != null) {
      matchFound = matchFound && dependency.getGroup().matches(dependencyPattern.getGroupPattern());
    }
    if (dependencyPattern.getNamePattern() != null) {
      matchFound = matchFound && dependency.getName().matches(dependencyPattern.getNamePattern());
    }
    if (dependencyPattern.getVersionPattern() != null) {
      matchFound = matchFound && dependency.getVersion().matches(dependencyPattern.getVersionPattern());
    }

    return matchFound;
  }

  /**
   * Returns the set of all the dependencies which should be checked.
   * <p>
   * Subclasses can override this method to return their own set of dependencies. By default we
   * return all the dependencies.
   *
   * @param project The Gradle project
   * @return All dependencies of the project
   */
  Set<Dependency> getDependencies(Project project) {
    Set<Dependency> dependencies = new HashSet<Dependency>();

    project.getConfigurations().each {
      configuration -> dependencies.addAll(configuration.getAllDependencies())
    }

    return dependencies;
  }

  /**
   * Creates and returns the list of DependencyPatterns.
   *
   * @param project The Gradle project
   * @return List of dependency patterns
   */
  List<DependencyPattern> getDependencyPatterns(Project project) {
    List<DependencyPattern> dependencyPatterns = new ArrayList<DependencyPattern>();
    String jsonTextToParse = getDependencyPatternsJsonText(project);

    new JsonSlurper().parseText(jsonTextToParse).dependencyPatterns.each {
      dependencyPatterns.add(new DependencyPattern(it.groupPattern, it.namePattern, it.versionPattern, (SEVERITY)it.severity, it.message));
    }

    return dependencyPatterns;
  }

  /**
   * Gets the text from the json file.
   *
   * @param project The Gradle project
   * @return Text from the dependency patterns json file
   */
  String getDependencyPatternsJsonText(Project project) {
    String dependencyPatternFile = getDependencyPatternFile(project);
    String jsonTextToParse = null;

    if (Thread.currentThread().getContextClassLoader().getResource(dependencyPatternFile) != null) {
      jsonTextToParse = Thread.currentThread().getContextClassLoader().getResource(getDependencyPatternFile(project)).text;
    } else if (new File(dependencyPatternFile).exists()) {
      jsonTextToParse = new File(dependencyPatternFile).text;
    } else {
      throw new GradleException("${dependencyPatternFile} was not found.");
    }
    return jsonTextToParse;
  }

  /**
   * This returns the name of the json file which contains the DependencyPatterns.
   * <p>
   * Subclasses can override this method to return their own dependencyPatternJson file.
   *
   * @param project The Gradle project
   * @return The path or name of the json file which contains dependencyPattern information
   */
  String getDependencyPatternFile(Project project) {
    return DEPENDENCY_PATTERN_FILE;
  }
}
