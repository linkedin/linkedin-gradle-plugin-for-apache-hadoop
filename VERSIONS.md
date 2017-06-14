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

Note that the LinkedIn build system occasionally requires that we skip a
version bump, so you will see a few skipped version numbers in the list below.

0.12.4

* Add ability for Hadoop DSL to understand the HdfsWait job type
* Add subdirectory hadoop-jobs
* Minor fixes to the Azkaban CLI tasks
* Minor style and code fixes

0.12.3

* Minor documentation and style fixes

0.12.1

* Add TableauJob job type support
* Fix issue with missing isGrouping property on cloned workflows

0.12.0

* Add initial Hadoop Plugin example project
* Fix broken links to example-project subproject in README.md
* Refactor namespaces for Hadoop Validator code to be more consistent and switch to jcenter

0.11.25

* Downgrade from com.jfrog.bintray plugin 1.7.3 to 1.7 since the latest version is not working for me

0.11.24

* After evaluating a Hadoop DSL `hadoopClosure` we should restore the previous current definition set

0.11.23

* Updates to use external resource files

0.11.22

* LIHADOOP-28747 Hadoop plugin's test plugin should have functionality to add assertions on tests.

0.11.20

* Minor cosmetic changes to the project setup
* Copy resource files from external directory

0.11.19

* Prevent `core-site.xml` from polluting the IDE classpath

0.11.18

* Add support for a `required parameters` check

0.11.17

* Remove unfinished Oozie Hadoop DSL elements to simplify the long-term maintenance overhead
* Rename conflicting Azkaban test extension and task names to not collide

0.11.16

* Minor updates to naming scheme for grouping jobs into subflows in the Azkaban UI
* Add PinotBuildAndPushJob job type support

0.11.15

* Incrementing version to 0.11.15 for LinkedIn internal builds

0.11.14

* LIHADOOP-25519 ligradle tasks to test the flow on Azkaban
* LIHADOOP-25518 Prototype deploy tests for the workflows

0.11.13

* Improvement of visual presentation of generated flows in Azkaban - Add ability to group job nodes in a subflow 

0.11.12

* Expand on the list of file types automatically excluded from the sources zip to exclude archive types

0.11.11

* LIHADOOP-26251 Include check for classpath property in javadoc when setting cluster provided configuration

0.11.10

* LIHADOOP-26059 Change in azkabanUpload task error message and azkabanCancelFlow task console output.

0.11.9

* LIHADOOP-25566 Update Dr.Elephant URL link in azkabanFlowStatus task

0.11.8

* LIHADOOP-25637 Check for trailing slash in AzkabanUrl when running azkabanUpload task

0.11.7

* Added possibility to have Azkaban password in configuration JSON.

0.11.6

* Hadoop DSL IDE Syntax Completion for IntelliJ - Add annotation to help expose top-level Hadoop DSL methods

0.11.5

* Hadoop DSL IDE Syntax Completion for IntelliJ - Further improvements to Hadoop DSL code for Intellij
    
0.11.4

* Hadoop DSL IDE Syntax Completion for IntelliJ - Further improvements to Hadoop DSL code for Intellij
* Minor build setup cleanups and improvements - Apply eclipse and idea plugins to all projects

0.11.3

* Hadoop DSL IDE Syntax Completion for IntelliJ - Minor fixes to Hadoop DSL code and initial gdsl file (#122)

0.11.2

* LIHADOOP-24729 Addressed improper console output for azkabanExecuteFlow task
* Fix jline dependency override in hadoop-plugin from azkaban-client subproject

0.11.1

* LIHADOOP-24274 Checks for empty fields in .azkabanPlugin.json

0.11.0

* LIHADOOP-24299 Add support in the Hadoop Plugin for choosing the default artifact for Hadoop CRT deployments

0.10.15

* Bump to Gradle 2.13

0.10.14

* LIHADOOP-21680 Azkaban CLI Features Create Project, Flow Status, Execute Flow and Cancel Flow

0.10.13

* Display custom message after azkabanUpload task.

0.10.12

* Enable properly redeclaring the paths your Hadoop DSL job reads and writes

0.10.10

* LIHADOOP-23413: Changes to make Hadoop Validator work. Automates creating .hadoopValidatorProperties file and properly sets krb5.conf

0.10.9

* LIHADOOP-23255: Add azkaban jvm properties to the jobConf for bangbang

0.10.8

* LIHADOOP-22850 Handle null console and minor changes in upload bar's dispaly

0.10.7

* Minor touch-ups to the azkabanUpload task console messsages

0.10.6

* LIHADOOP-22822 The azkabanUpload task should automatically go into edit mode if there are missing required fields

0.10.5

* LIHADOOP-22809 Displaying configured Hadoop zips during azkabanUpload task

0.10.4

* Cause the azkabanUpload task to prompt the user for their password as part of the progress line

0.10.3

* LIHADOOP-22655 Automate creation of Azkaban Project during azkabanUpload task
* LIHADOOP-22654 Enable azkabanUpload task to validate session before uploading

0.10.2

* Misc update on Java doc and test cases

0.10.1

* Integrate bangbang with li-hadoop-plugin

0.10.0

* Add HadoopValidatorPlugin with PigValidatorPlugin in its stack

0.9.17

* Add additional Hadoop DSL workflow flowDepends override that matches the override for the targets method

0.9.16

* Minor fixes to how the upload status of the Azkaban zip is displayed
* LIHADOOP-22222 Display Upload Status of Zip when running azkabanUpload task

0.9.15

* Added Hive ORC table as a source in hdfsToTeradata job type

0.9.14

* Added python Spark support for runSparkJob task

0.9.13

* Minor fixes to the automation process to the .azkabanPlugin.json file

0.9.12

* LIHADOOP-21797 Automate the creation of the .azkabanPlugin.json file

0.9.11

* Added support for submitting python Spark applications with Spark job type

0.9.10

* LIHADOOP-21658 Refactor AzkabanUploadTask in Hadoop Plugin by creating AzkabanHelper class

0.9.9

* Correct subtle error in evalHadoopClosure that can occur if the hadoopClosure is not declared in global scope
* Correct minor error with runSparkJob task for li-hadoop-plugin override

0.9.8

* Work around for the gradle issue where output to console is overwritten by status line.

0.9.7

* Modified Spark plugin so that Spark job run on YARN cluster unless explicitly specified otherwise.

0.9.6

* Made multipart file upload to be compliant with httpclient 4.3.1 version.
* Upgraded httpclient and httpmime libraries to 4.3.1 version.

0.9.5

* Added SQL job type support.
* Added encrypted credential for Teradata job types.

0.9.4

* Fixes for Apache Spark job type

0.9.3

* Update LinkedIn-specific overrides for tasks

0.9.2

* Large number of minor fixes to reduce LinkedIn codestyle and codenarc check failures
* Increase build robustness for LinkedIn ligradle builds, which keep changing how SNAPSHOT versions work

0.9.1

* LIHADOOP-18499 Add extra typed methods for Hadoop DSL SparkJob properties

0.8.8

* Updated return types of Hadoop DSL definitionSet and hadoopClosure methods

0.8.6

* Add Maven publishing configuration so Bintray uploads can be linked to JCenter

0.8.4

* Various minor improvements for the Hadoop DSL from my backlog

0.8.3

* Despite my best efforts, we're having issues with Gradle 2.10 in our LinkedIn
  internal builds. Re-reverting back to 2.7 for now.

0.8.2

* Gradle version bumped to 2.10

0.8.1

* LIHADOOP-6730: Add clusterProvided configuration for dependency management

0.8.0

* Improved hadoopClosure and evalHadoopClosure Hadoop DSL methods that are way more expressive

0.7.11

* DSS-3870 Hadoop DSL for new Azkaban job type. Teradata <-> HDFS, HDFS -> Espresso, and Gobblin

0.7.10

* Fix for LinkedIn PCX builds
* LIHADOOP-17034 Create HadoopShell job type in Hadoop DSL

0.7.9

* Updates to README.md to announce publishing on plugins.gradle.org
* Fixes for publishing to plugins.gradle.org
* Update build system to publish to plugins.gradle.org
* Update VERSIONS.md and CONTRIBUTORS.md for recent contributions

0.7.8

* LIHADOOP-16591: ligradle azkabanUpload fails with "ClassNotFoundException: org.apache.http.client.methods.HttpPost"
* TOOLS-67569 Add task to disallow local dependencies which are not generated at build time

0.7.7

* Fix to restore improper source task name

0.7.6

* Fix to restore adding the sources zip and scm metadata files automatically
* Fix for LiScmPlugin bug affecting LinkedIn li-azkaban2 zip artifacts

0.7.5

* Dropping Gradle version 2.7 to match most LinkedIn multiproducts
* Fix test output string filter for LinkedIn builds
* Gradle version bumped to 2.8

0.7.4

* Add Hadoop zip tasks directly instead of through project.afterEvaluate
Clean up hadoop-plugin unit tests and start to add tests in li-hadoop-plugin-test
Start removing ability to disable sub-plugins
Enable the Gradle daemon - tests run much faster

0.7.3

* Refactor Hadoop zip functionality into separate ZipPlugin class
Establish a clear place in the HadoopPlugin class where task dependencies are set

0.7.2

* LIHADOOP-16016 Minor bug fix for li-hadoop-plugin
* Fix hadoop-plugin subproject unit tests
* Add generic GatewayCommand helper class for running remote commands through a gateway

0.7.1

* LIHADOOP-15728 Update dependency pattern blacklist rules for li-hadoop-plugin
* Make LinkedIn subclass for the Spark Plugin use the designated Spark gateway host
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
