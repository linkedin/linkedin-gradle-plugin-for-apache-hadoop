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

### LinkedIn Gradle Plugin for Apache Hadoop - Example Project

This is an example project that uses the **LinkedIn Gradle Plugin for Apache
Hadoop** (i.e. the "Hadoop Plugin") to build an Azkaban workflow that includes
Hadoop shell command jobs, an Apache Pig job, an Apache Hive job, and a Java
Map-Reduce job. To learn more about the Hadoop Plugin, see the
[User Guide](https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/wiki/User-Guide).

All the example Hadoop jobs implement their own variant of the traditional
"word count" example. In addition, there are Hadoop mini-cluster based unit
tests for the Java Map-Reduce jobs.

The Azkaban workflow is specified using the **LinkedIn Gradle DSL for Apache
Hadoop** (i.e. the "Hadoop DSL"). To learn more about The Hadoop DSL, see the
[Hadoop DSL Language Reference](https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/wiki/Hadoop-DSL-Language-Reference).

#### Instructions
If you use Eclipse or IDEA as your IDE, start of by running `./gradlew eclipse`
or `./gradlew idea` and then importing the project into your IDE.

Next, you need to customize the Hadoop DSL workflow for your own user name and
email address. Edit the Hadoop DSL `definitionSet` that appears at the top of
the file `src/main/definitions/dev.gradle` and provide your own values.

Since this example assumes you have separate `dev` and `prod` Hadoop grids (and
that you want to run slightly different versions of the workflow in each grid),
you will notice that there is also a `prod.gradle` file in
`src/main/definitions`. The Hadoop DSL Automatic Build process will configure
the Hadoop DSL separately for both these files.

Alternatively, you can add your own Hadoop DSL profile script at
`src/main/profiles/<yourUserName>.gradle` and provide your own `definitionSet`
overrides there. See the file `src/main/profiles/abain.gradle` for an example.

Now build the project by running `./gradlew build`. This will run the
mini-cluster based unit tests, compile the Hadoop DSL, and build your Azkaban
zip files.

Last, you should upload the project to your Azkaban instance by running
`./gradlew azkabanUpload --no-daemon`. Follow the on-screen prompts to tell the
Hadoop Plugin about your Azkaban instance and upload the Azkaban zip file to
Azkaban.
