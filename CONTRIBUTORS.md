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

### Author

The LinkedIn Gradle Plugin for Apache Hadoop was designed and created by Alex Bain. The design of
the LinkedIn Gradle DSL for Apache Hadoop was influenced by
[azkaban-rb](https://github.com/matthayes/azkaban-rb) from Matthew Hayes and by input from Will
Vaughan.

### Contributors

The following were contributed by Jun "Tony" Zhou. Thanks, Tony!
* `Added application running mode check in Spark plugin. Spark job now run on YARN cluster unless explicitly specified otherwise.`

The following were contributed by Jin Hyuk Chang. Thanks, Jin Hyuk!
* `DSS-4743: Hadoop DSL for SQL job type. Adding encrypted password support on Teradata job type in Hadoop DSL.`
* `DSS-3870: Hadoop DSL for new Azkaban job type. Teradata <-> HDFS, HDFS -> Espresso, and Gobblin`

The following were contributed by Anthony Hsu. Thanks, Anthony!
* `LIHADOOP-16591: ligradle azkabanUpload fails with "ClassNotFoundException: org.apache.http.client.methods.HttpPost"`

The following were contributed by Jonathan Hung. Thanks, Jonathan!
* `TOOLS-67569 Block Hadoop Client Teams from checking in Hadoop Jars in their MP`

The following were contributed by Anant Nag. Thanks, Anant!
* `LIHADOOP-18499 Add extra typed methods for Hadoop DSL SparkJob properties`
* `LIHADOOP-6730: Add clusterProvided configuration for dependency management`
* `LIHADOOP-17034 Create HadoopShell job type in Hadoop DSL`
* `LIHADOOP-16016 Minor bug fix for li-hadoop-plugin`
* `LIHADOOP-15728 Update dependency pattern blacklist rules for li-hadoop-plugin`
* `LIHADOOP-15501 checkDependencies task fails if any of groupID, name or version is null`
* `LIHADOOP-15077 Hadoop plugin should have a feature to control and monitor dependencies specified by the users`
* `Fixups on HadoopJavaJob and HiveJob`
* `LIHADOOP-14506 Hadoop plugin should directly submit jobs to oozie`
* `LIHADOOP-13682 Oozie Hadoop DSL Compiler support property files`
* `LIHADOOP-13681 Oozie Hadoop DSL Compiler support all job types`
* `Added xsd schemas for different job types`
* `LIHADOOP-14566 SparkPlugin does not read application parameters`
* `LIHADOOP-12557 Hadoop Plugin should support launching Spark scripts from a gateway machine`
* `LIHADOOP-14099 Create spark job type in hadoop dsl`
* `LIHADOOP-13660 Hadoop Plugin oozieUpload task should upload the contents of the hadoopZip task`
* `Minor fixes on OozieUploadTask and LiKerberosUtil`
* `LIHADOOP-13353 Hadoop Plugin oozieUpload task should upload to versioned directory.`
* `LIHADOOP-13457 Fix Stackoverflow exception thrown by OozieUploadTask.`
* `LIHADOOP-12771 Prototype Hadoop Plugin Upload Task for Oozie`
* `HADOOP-12726 Style cleanups for Hadoop zip tests`
* `HADOOP-12243 Fix Hadoop Plugin PCL Breakage`
* `HADOOP-10148 Support exclusion list for Hadoop Plugin sources Zip`

The following were contributed by Akshay Rai. Thanks, Akshay!
* `LIHADOOP-12945 Hadoop Plugin azkabanUpload enhancement to print project URL`
* `HADOOP-12243 Fix Hadoop Plugin PCL Breakage`
* `HADOOP-10773 Rewrite li-azkaban2 zip upload tasks for the Hadoop Plugin`
* `HADOOP-8251 Hadoop DSL doesn't use -p flag for mkdir`

The following were contributed by Keith Dsouza. Thanks, Keith!
* `Added support to the KafkaPushJob to accept the disable.auditing property`

The following were contributed by Ragesh Rajagopalan. Thanks, Ragesh!
* `LIHADOOP-20985 azkabanUpload throwing 500 error`
* `DI-584 Login prompt missing when uploading to Azkaban`

The following were contributed by Pranay Hasan Yerra. Thanks, Pranay!
* 'LIHADOOP-21658 Refactor AzkabanUploadTask in Hadoop Plugin by creating AzkabanHelper class'

The following were contributed by Pratik Satish. Thanks, Pratik!
* `LIHADOOP-21429 Add HadoopValidatorPlugin with PigValidatorPlugin in its stack
