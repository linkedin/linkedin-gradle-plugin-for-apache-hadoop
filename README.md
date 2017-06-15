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

[![Build Status](https://travis-ci.org/linkedin/linkedin-gradle-plugin-for-apache-hadoop.svg?branch=master)](https://travis-ci.org/linkedin/linkedin-gradle-plugin-for-apache-hadoop) [![Download](https://api.bintray.com/packages/convexquad/maven/linkedin-gradle-plugin-for-apache-hadoop/images/download.svg)](https://bintray.com/convexquad/maven/linkedin-gradle-plugin-for-apache-hadoop/_latestVersion)

### LinkedIn Gradle Plugin for Apache Hadoop

The **LinkedIn Gradle Plugin for Apache Hadoop** (which we shall refer to as simply the
"Hadoop Plugin" for brevity) will help you more effectively build, test and deploy Hadoop
applications.

In particular, the Plugin will help you easily work with Hadoop applications like Apache Pig and
build workflows for Hadoop workflow schedulers such as Azkaban and Apache Oozie.

The Plugin includes the **LinkedIn Gradle DSL for Apache Hadoop** (which we shall refer to as simply
the "Hadoop DSL" for brevity), a language for specifying jobs and workflows for Azkaban.

#### Hadoop Plugin User Guide

The Hadoop Plugin User Guide is available at [User Guide](https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/wiki/User-Guide).

#### Hadoop DSL Language Reference

The Hadoop DSL Language Reference is available at [Hadoop DSL Language Reference](https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/wiki/Hadoop-DSL-Language-Reference).

#### Getting the Hadoop Plugin

The Hadoop Plugin is now published at [plugins.gradle.org](https://plugins.gradle.org/plugin/com.linkedin.gradle.hadoop.HadoopPlugin).
Click on the link for a short snippet to add to your `build.gradle` file to start using the Hadoop
Plugin.

#### Can I Benefit from the Hadoop Plugin and Hadoop DSL?

You must use Gradle as your build system to use the Hadoop Plugin. If you are using Azkaban, you
should start using the Hadoop Plugin immediately and you should use the Hadoop DSL to develop all
of your Azkaban workflows.

If you are using Apache Pig, the Plugin includes features that will statically validate your Pig
scripts, saving you time by finding errors at build time instead of when you run your Pig script.

If you run Apache Pig or Apache Spark on a Hadoop cluster through a gateway node, the Plugin
includes tasks that will automate the process of launching your Pig or Spark jobs on the gateway
without you having to manually download your code and dependencies there first.

If you are using Gradle and you feel that you might benefit from any of the above features,
consider using the Hadoop Plugin and the Hadoop DSL.

#### Example Project

We have added an [Example Project](https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/tree/master/example-project)
that uses the Hadoop Plugin and DSL to build an example Azkaban workflow consisting of Apache Pig,
Apache Hive and Java Map-Reduce jobs.

#### Apache Oozie Status

The Hadoop Plugin includes Gradle tasks for Apache Oozie, including the ability to upload versioned
directories to HDFS, as well as Gradle tasks for issuing Oozie commands. If you are using Gradle as
your build system and Apache Oozie as your Hadoop workflow scheduler, you might find the Hadoop
Plugin useful. However, we would like to mention the fact that since we are no longer actively using
Oozie at LinkedIn, it is possible that the Oozie tasks may fall into a non-working state.

Although we started on a Hadoop DSL compiler for Oozie, we did not complete it, and it is currently
not in a usable form. We are not currently working on it and it is unlikely to be completed.

#### Recent News

  * `May 2017` We have added an [Example Project](https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/tree/master/example-project) that uses the Hadoop Plugin and DSL
  * `April 2016` We have made a refresh of the [User Guide](https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/wiki/User-Guide) and [Hadoop DSL Language Reference](https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/wiki/Hadoop-DSL-Language-Reference) Wiki pages
  * `January 2016` The Hadoop Plugin is now published on [plugins.gradle.org](https://plugins.gradle.org/plugin/com.linkedin.gradle.hadoop.HadoopPlugin)
  * `November 2015` Gradle version bumped to 2.7 and the Gradle daemon enabled - tests run much, much faster
  * `August 2015` Initial pull requests for Oozie versioned deployments and the Oozie Hadoop DSL compiler have been merged
  * `August 2015` The Hadoop Plugin and Hadoop DSL were released on Github! See the [LinkedIn Engineering Blog post](https://engineering.linkedin.com/hadoop/open-sourcing-linkedin-gradle-plugin-and-dsl-apache-hadoop) for the announcement!
  * `July 2015` See our talk at the [Gradle Summit](https://www.youtube.com/watch?v=51NzDgxHr4I)

#### Project Structure

The project structure is setup as follows:

  * `azkaban-client`: Code to work with Azkaban via the Azkaban REST API
  * `example-project`: Example project that uses the Hadoop Plugin and DSL to build an example Azkaban workflow
  * `hadoop-jobs`: Code for re-usable Hadoop jobs and implementations of Hadoop DSL job types 
  * `hadoop-plugin`: Code for the various plugins that comprise the Hadoop Plugin
  * `hadoop-plugin-test`: Test cases for the Hadoop Plugin
  * `li-hadoop-plugin`: LinkedIn-specific extensions to the Hadoop Plugin
  * `li-hadoop-plugin-test`: Test cases for the LinkedIn-specific extensions to the Hadoop Plugin

Although the `li-hadoop-plugin` code is generally specific to LinkedIn, it is included in the
project to show you how to use subclassing to extend the core functionality of the Hadoop Plugin for your
organization (and to make sure our open-source contributions don't break the LinkedIn customizations).

#### Building and Running Test Cases

To build the Plugin and run the test cases, run `./gradlew build` from the top-level project directory.

To see all the test tasks, run `./gradlew tasks` from the top-level project directory. You can run
an individual test with `./gradlew test_testName`. You can also run multiple tests by running
`./gradlew test_testName1 ... test_testNameN`.
