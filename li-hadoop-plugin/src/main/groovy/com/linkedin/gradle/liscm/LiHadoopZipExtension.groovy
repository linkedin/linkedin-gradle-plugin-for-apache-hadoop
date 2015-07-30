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
package com.linkedin.gradle.liscm;

import com.linkedin.gradle.scm.HadoopZipExtension;

import org.gradle.api.Project;
import org.gradle.api.file.CopySpec

/**
 * LinkedIn-specific customizations to the HadoopZipExtension class. In particular, this class
 * enables users to declare that they should generate a CRT deployment zip that contains their .crt
 * file. This is particular to LinkedIn and required for LinkedIn's one-click deployment system.
 */
class LiHadoopZipExtension extends HadoopZipExtension {
  boolean declaresCRT = false;
  boolean declaresMain = false;

  /**
   * Constructor for the LiHadoopZipExtension.
   *
   * @param project The Gradle project
   */
  LiHadoopZipExtension(Project project) {
    super(project);
  }

  /**
   * <pre>
   *   hadoopZip {
   *     CRT { }
   *   }
   * </pre>
   */
  void CRT(Closure closure) {
    if (declaresMain) {
      throw new Exception("You cannot declare both a main zip and a CRT zip. Refer to go/HadoopCRT for more information.");
    }
    declaresCRT = true;
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
  @Override
  void main(Closure closure) {
    if (declaresCRT) {
      throw new Exception("You cannot declare both a main zip and a CRT zip. Refer to go/HadoopCRT for more information.");
    }
    declaresMain = true;
    super.main(closure);
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
  @Override
  void zip(String zipName, Closure closure) {
    if ("CRT".equals(zipName)) {
      throw new Exception("You cannot declare a zip called CRT. Use the CRT method to declare that your project uses CRT. Refer to go/HadoopCRT for more information.");
    }
    super.zip(zipName, closure);
  }
}