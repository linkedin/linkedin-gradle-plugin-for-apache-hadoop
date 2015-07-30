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
package com.linkedin.gradle.scm;

import org.gradle.api.Project;
import org.gradle.api.file.CopySpec;

/**
 * A hadoopZip makes it convenient for the user to make specific choices about how their content
 * goes inside the zip. In the hadoopZip, the user can specify files and folders which should go
 * inside which folder in the zip.
 * <p>
 * The hadoopZip can be specified with:
 * <pre>
 *    hadoopZip {
 *      libPath = "."
 *      main {
 *        from ("src/") {
 *          into "src"
 *        }
 *      }
 *    }
 * </pre>
 */
public class HadoopZipExtension {
  CopySpec baseCopySpec;
  String libPath;
  Project project;
  Map<String, CopySpec> zipMap;

  /**
   * Constructor for the HadoopZipExtension.
   *
   * @param project The Gradle project
   */
  HadoopZipExtension(Project project) {
    this.project = project;
    libPath = ".";
    zipMap = new HashMap<String, CopySpec>();
  }

  /**
   * The files specified by the base copySpec will be added to all the zips including main.
   * The base spec is added as a child of the specific zip specs.
   * <pre>
   *   hadoopZip {
   *     base {
   *       from("common resources") {  // add the files common to all the zips.
   *         into "common"
   *       }
   *     }
   *
   *     zip("magic") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *
   *     zip("canasta") {
   *       from ("azkaban/") {
   *         into "."
   *       }
   *     }
   *   }
   * </pre>
   * The DSL inside the {@code base\{} } block is the same DSL used for Copy tasks.
   */
  void base(Closure closure) {
    if (baseCopySpec != null) {
      throw new RuntimeException("base is already defined");
    }
    baseCopySpec = project.copySpec(closure);
  }

  /**
   * <pre>
   *   hadoopZip {
   *     main {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *   }
   * </pre>
   * The DSL inside the {@code main\{} } block is the same DSL used for Copy tasks.
   */
  void main(Closure closure) {
    zip("main", closure);
  }

  /**
   * <pre>
   *   hadoopZip {
   *     zip("magic") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *
   *     zip("canasta") {
   *       from ("src/") {
   *         into "src"
   *       }
   *     }
   *   }
   * </pre>
   * The DSL inside the {@code zip(zipName)\{} } block is the same DSL used for Copy tasks.
   */
  void zip(String zipName, Closure closure) {
    if (zipMap.containsKey(zipName)){
      throw new RuntimeException("${zipName} is already defined");
    }
    zipMap.put(zipName, project.copySpec(closure));
  }

  /**
   * Utility method to return baseCopySpec.
   *
   * @return baseCopySpec
   */
  CopySpec getBaseCopySpec() {
    return baseCopySpec;
  }

  /**
   * Utility method to return the CopySpec for the given named zip.
   *
   * @param zipName
   * @return Returns the CopySpec for the given zip name
   */
  CopySpec getZipCopySpec(String zipName) {
    if (zipMap.containsKey(zipName)) {
      return zipMap.get(zipName);
    }
    return null;
  }

  /**
   * Utility method to return the zipMap.
   *
   * @return Returns the zipMap
   */
  Map<String, CopySpec> getZipMap() {
    return zipMap;
  }
}