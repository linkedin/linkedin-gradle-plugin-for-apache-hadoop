## Hadoop Gradle Plugin

The Hadoop Plugin will help you more effectively build, test and deploy Hadoop applications. In
particular, the first release of the Plugin will help you quickly debug Apache Pig scripts and
build Hadoop data workflows with the Hadoop DSL.

### Project Structure

The project structure is setup as follows:

  * hadoop-plugin: Code for the various plugins that comprise the Hadoop Plugin
  * hadoop-plugin-test: Test cases for the Hadoop Plugin
  * li-hadoop-plugin: LinkedIn-specific extensions to the Hadoop Plugin

Although the li-hadoop-plugin code is generally specific to LinkedIn, it is included in the project
to show you how to use subclassing to extend the core functionality of the Hadoop Plugin.

### Test Cases

To run the test cases, just "gradle build" from either the top-level directory or the
hadoop-plugin-test directory.

To see all the tests you can run, "gradle tasks" from the hadoop-plugin-test directory. You can run
an individual test with "gradle test_<testName>". You can also run multiple tests by running
"gradle test_<testName1> ... test_<testNameN>".
