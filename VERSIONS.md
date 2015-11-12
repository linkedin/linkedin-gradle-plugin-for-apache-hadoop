<!--
Copyright 2015 LinkedIn Corp.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

Note that the LinkedIn build system occasionally requires that we skip a version bump, so you will
see a few skipped version numbers in the list below.

0.7.3

* Refactor Hadoop zip functionality into separate ZipPlugin class and establish a clear place in the HadoopPlugin class where task dependencies between plugins are set

0.7.2

* Refactor Hadoop zip functionality into separate ZipPlugin class and establish a clear place in the HadoopPlugin class where task dependencies between plugins are set
* LIHADOOP-16016 Minor bug fix for li-hadoop-plugin
* Fix hadoop-plugin subproject unit tests
* Add generic GatewayCommand helper class for running remote commands through a gateway

0.7.1

* LIHADOOP-15728 Update dependency pattern blacklist rules for li-hadoop-plugin
* Make LinkedIn subclass for the Spark Plugin use eat1-nertzgw03 instead of eat1-nertzgw01
* Expose lookupRef as a DSL method

0.7.0

* Cleanup the implementation of scope lookup

0.6.10

* LIHADOOP-15501 checkDependencies task fails if any of groupID, name or version is null
* Added applyProfile method that applies an external Gradle script, but only if it exists

0.6.9

* LIHADOOP-15077 Hadoop plugin should have a feature to control and monitor dependencies specified by the users

0.6.8

* Fixups on HadoopJavaJob and HiveJob
* LIHADOOP-14506 Hadoop plugin should directly submit jobs to oozie
* LIHADOOP-13682 Oozie Hadoop DSL Compiler support property files
* LIHADOOP-13681 Oozie Hadoop DSL Compiler support all job types
* Added xsd schemas for different job types.

0.6.7

* LIHADOOP-14566 SparkPlugin does not read application parameters

0.6.6

* LIHADOOP-12557 Hadoop Plugin should support launching Spark scripts from a gateway machine

0.6.5

* LIHADOOP-14099 Create spark job type in hadoop plugin dsl.

0.6.4

* The oozieUpload task should depend on the buildHadoopZips task but does not seem to do so. Also update the NOTICE file.
* Usability improvements and minor refactorings to Oozie-related tasks
* LIHADOOP-13660 Hadoop Plugin oozieUpload task should upload the contents of the hadoopZip task

0.6.3

* Update jaxb versions to final 2.2.7 version
* LIHADOOP-13173 Initial work for Hadoop DSL Oozie compiler

0.6.2

* Minor fixes on OozieUploadTask and LiKerberosUtil
* Fix property to get the current user name in the writeAzkabanPluginJson task
* Update the li-hadoop-plugin override of the writeAzkabanPluginJson method to fill in some common property values
* LIHADOOP-13353 Hadoop Plugin oozieUpload task should upload to versioned directory.
* LIHADOOP-13457 Fix Stackoverflow exception thrown by OozieUploadTask.

0.6.1

* Enable Hadoop Plugin tests to work wth LinkedIn mint snapshot builds
* Version bump back to the 0.6.x series

0.5.17

* LIHADOOP-12945 Hadoop Plugin azkabanUpload enhancement to print project URL
* Fix unit tests for hadoop-plugin module and fix how cross-plugin task dependencies are added
* LIHADOOP-12771 Prototype Hadoop Plugin Upload Task for Oozie

0.5.16

* TOOLS-67363 Hadoop Plugin intermittent errors building sources zip file

0.5.15

* HADOOP-12795 Complete Tasks for Open-Sourcing Hadoop Plugin
* HADOOP-12846 Hadoop DSL Make KafkaPushJob nameNode property not required again
* HADOOP-12837 Set file permissions correctly for new Hadoop Plugin azkabanUpload task

0.5.14

* HADOOP-12773 Minor refactoring to new Azkaban upload task

0.5.13

* HADOOP-12726 Style cleanups for Hadoop zip tests

0.5.12

* HADOOP-12724 Minor refactoring to new Hadoop zip tasks

0.5.11

* HADOOP-12243 Fix Hadoop Plugin PCL Breakage

0.5.10

* HADOOP-10773: Rewrite li-azkaban2 zip upload tasks for the Hadoop Plugin

0.5.8

* HADOOP-11178 Hadoop DSL Closures Language Feature

0.5.7

* HADOOP-12029 Prevent users from declaring elements directly under a workflow with the same name as the workflow

0.5.6

* HADOOP-11814 Base configuration for Hadoop plugin zip task
* HADOOP-11658 Rename Hadoop Plugin and DSL appropriately to respect trademarks

0.5.5

* HADOOP-10772 Port li-azkaban2 zip-building tasks to the Hadoop Plugin
* HADOOP-10914 Add Apache License Files to Hadoop Plugin - Minor Updates Part 2
* HADOOP-10914 Add Apache License Files to Hadoop Plugin - Minor Updates
* HADOOP-10914 Add Apache License Files to Hadoop Plugin

0.5.3

* HADOOP-11177 Hadoop DSL Definition Sets Language Feature
* HADOOP-11332 Hadoop DSL Kafka job type should require nameNode property
