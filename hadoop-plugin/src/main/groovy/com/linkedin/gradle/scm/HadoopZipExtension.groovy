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
 *             from "src/" {
 *                 into "src"
 *             }
 *         }
 *     }
 * </pre>
 */
public class HadoopZipExtension {
    Project project;
    Map<String,List<CopySpec>> clusterMap;

    /**
     * Constructor for the HadoopZipExtension
     * @param project
     */
    HadoopZipExtension(Project project) {
        this.project = project;
        clusterMap = new HashMap<String,List<CopySpec>>();
    }

    /**
     * <pre>
     * hadoopZip {
     *
     *     main {
     *             from "src/" {
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
     * <pre>
     * hadoopZip {
     *
     *
     *     cluster("magic") {
     *             from "src/" {
     *                 into "src"
     *             }
     *     }
     *
     *     cluster("canasta") {
     *         from "src/" {
     *             into "src"
     *         }
     *     }
     *
     *     cluster("magic") {  // add files to existing cluster magic.
     *         from "resources"{}
     *     }
     * }
     * </pre>
     * The DSL inside the {@code cluster(clustername)\{} } block is the same DSL used for Copy tasks.
     */
    void cluster(String name, Closure closure){
        if(clusterMap.containsKey(name)){
           clusterMap.get(name).add(project.copySpec(closure));
           return;
        }
        List<CopySpec> copySpecList = new ArrayList<CopySpec>();
        copySpecList.add(project.copySpec(closure));
        clusterMap.put(name,copySpecList);
    }

    /**
     * Utility method to return the list of CopySpecs for a given clustername
     * @param clusterName
     * @return Returns the list of CopySpecs for clustername
     */
    List<CopySpec> getContentList(String clusterName){
       if(clusterMap.containsKey(clusterName)) {
           return clusterMap.get(clusterName);
       }
        return null;
    }

    /**
     * Utility method to return the clusterMap.
     * @return Returns the clusterMap
     */
    Map<String,List<CopySpec>> getClusterMap(){
       return clusterMap;
    }
}