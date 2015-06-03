<!--
Copyright 2014 LinkedIn Corp.

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

### Hadoop Gradle Plugin

The Hadoop Plugin will help you more effectively build, test and deploy Hadoop applications.

In particular, the Plugin will help you easily work with Hadoop applications like Apache Pig and
build workflows for Hadoop workflow schedulers like Azkaban and Apache Oozie.

The Plugin includes the Hadoop DSL, a language for specifying jobs and workflows for Hadoop workflow schedulers like Azkaban and Apache Oozie. Go directly to the [Hadoop DSL Language Reference](https://github.com/convexquad/hadoop-plugin/wiki/Hadoop-DSL-Language-Reference).

#### Project Structure

The project structure is setup as follows:

  * hadoop-plugin: Code for the various plugins that comprise the Hadoop Plugin
  * hadoop-plugin-test: Test cases for the Hadoop Plugin
  * li-hadoop-plugin: LinkedIn-specific extensions to the Hadoop Plugin

Although the li-hadoop-plugin code is generally specific to LinkedIn, it is included in the project
to show you how to use subclassing to extend the core functionality of the Hadoop Plugin.

#### Building and Running Test Cases

To build the Plugin and run the test cases, run "./gradlew build" from the top-level project directory.

To see all the test tasks, run "./gradlew tasks" from the top-level project directory. You can
run an individual test with "./gradlew test_testName". You can also run multiple tests by running
"./gradlew test_testName1 ... test_testNameN".

### User Documentation

#### Hadoop Plugin User Guide
The Hadoop Plugin User Guide is available at [Hadoop Plugin User Guide]
(https://github.com/convexquad/hadoop-plugin/wiki/Hadoop-Plugin-User-Guide).

#### Hadoop DSL Language Reference
The Hadoop DSL Language Reference is available at [Hadoop DSL Language Reference]
(https://github.com/convexquad/hadoop-plugin/wiki/Hadoop-DSL-Language-Reference).
