/*
 * Copyright 2014 LinkedIn Corp.
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
 *
 * <p>
 * the hadoopZip can be specified with:
 * <pre>
 *     hadoopZip {
 *         main {
 *             from ("src/") {
 *                 into "src"
 *             }
 *         }
 *     }
 * </pre>
 */
public class HadoopZipExtension {
    Project project;
    Map<String,CopySpec> clusterMap;
    CopySpec baseCopySpec;
    /**
     * Constructor for the HadoopZipExtension
     * @param project
     */
    HadoopZipExtension(Project project) {
        this.project = project;
        clusterMap = new HashMap<String,CopySpec>();
    }

    /**
     * <pre>
     * hadoopZip {
     *
     *     main {
     *             from ("src/") {
     *                 into "src"
     *             }
     *     }
     * }
     * </pre>
     * The DSL inside the {@code main\{} } block is the same DSL used for Copy tasks.
     */
    void main(Closure closure){
        cluster("main",closure);
    }

    /**
     * The files specified by the base copySpec will be added to all the zips including main.
     * The base spec is added as a child of the specific zip specs.
     *
     * <pre>
     * hadoopZip {
     *
     *     base {
     *           from("common resources") {  // add the files common to all the zips.
     *                  into "common"
     *         }
     *     }
     *
     *     cluster("magic") {
     *             from ("src/") {
     *                 into "src"
     *             }
     *     }
     *
     *     cluster("canasta") {
     *         from ("azkaban/") {
     *             into "."
     *         }
     *     }
     * }
     * </pre>
     * The DSL inside the {@code base\{} } block is the same DSL used for Copy tasks.
     */
    void base(Closure closure) {
        if(baseCopySpec!=null) {
            throw new RuntimeException("base is already defined");
        }
        baseCopySpec = project.copySpec(closure);
    }

    /**
     * Utility method to return baseCopySpec.
     * @return baseCopySpec
     */
    CopySpec getBaseCopySpec() {
        return baseCopySpec;
    }

    /**
     * <pre>
     * hadoopZip {
     *
     *     cluster("magic") {
     *             from ("src/") {
     *                 into "src"
     *             }
     *     }
     *
     *     cluster("canasta") {
     *         from ("src/") {
     *             into "src"
     *         }
     *     }
     *
     * }
     * </pre>
     * The DSL inside the {@code cluster(clustername)\{} } block is the same DSL used for Copy tasks.
     */
    void cluster(String name, Closure closure){
        if(clusterMap.containsKey(name)){
            throw new RuntimeException("${name} is already defined");
        }
        clusterMap.put(name,project.copySpec(closure));
    }

    /**
     * Utility method to return the list of CopySpec for a given clustername
     * @param clusterName
     * @return Returns the list of CopySpec for clustername
     */
    CopySpec getClusterCopySpec(String clusterName){
       if(clusterMap.containsKey(clusterName)) {
           return clusterMap.get(clusterName);
       }
        return null;
    }

    /**
     * Utility method to return the clusterMap.
     * @return Returns the clusterMap
     */
    Map<String,CopySpec> getClusterMap(){
       return clusterMap;
    }
}