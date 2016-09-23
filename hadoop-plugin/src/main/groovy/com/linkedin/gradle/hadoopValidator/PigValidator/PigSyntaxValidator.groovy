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
package com.linkedin.gradle.hadoopValidator.PigValidator;

import com.linkedin.gradle.hadoopdsl.NamedScope;
import com.linkedin.gradle.hadoopdsl.job.PigJob;

import org.apache.pig.Main;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;

/**
 * PigSyntaxValidator is the class that provides the Task for validation of syntax mentioned in the Apache Pig Scripts
 * in the project.
 */
class PigSyntaxValidator extends DefaultTask implements PigValidator {

  public enum PigArgs {
    //Execution mode Local
    EXEC_MODE('local', 'This arg sets the pig execution mode to local'),

    //command line option to set exec mode
        CL_OPTION_EXECTYPE('-x', 'command line option to set execution mode'),

    //Command line option for parameter substitution
        CL_OPTION_PARAMETERS('-p', 'Command line option for specifying parameters'),

    //Command line option to perform parameter substitution
        CL_OPTION_DRYRUN('-dryrun', 'Command line option to perform parameter substitution'),

    //Command line parameter to initiate syntax checking
        CL_OPTS_C('-c', 'Command line parameter to initiate syntax checking')

    private String value;
    private String description;

    private PigArgs(String value, String description) {
      this.value = value;
      this.description = description;
    }
  }

  Map<PigJob, NamedScope> jobMap;
  Properties properties;

  /**
   * Validates the syntax the Apache Pig Scripts in the project. This is the Task Action Function
   */
  @TaskAction
  void validate() {

    ArrayList<String> _args = new ArrayList<String>();
    File script;

    InputStream krbInputStream = this.getClass().getClassLoader().getResourceAsStream("krb5.conf");
    File krb5 = new File(System.getProperty("java.io.tmpdir"), "krb5.conf");
    OutputStream krbOutputStream = new FileOutputStream(krb5);

    int read;
    byte[] bytes = new byte[1024];
    while ((read = krbInputStream.read(bytes)) != -1) {
      krbOutputStream.write(bytes, 0, read);
    }
    System.setProperty("java.security.krb5.conf", krb5.getAbsolutePath());

    jobMap.each { PigJob pigJob, NamedScope parentScope ->
      script = new File(pigJob.script);
      if (script.name.endsWith(".pig")) {

        _args.addAll(pigJob.parameters.collect { key, value -> "${PigArgs.CL_OPTION_PARAMETERS.value} $key=$value" }.
            join(" ").
            split())
        _args.add(PigArgs.CL_OPTION_DRYRUN.value);

        project.logger.lifecycle("Checking file: \t $script");
        _args.add("$script");

        File subst_file = new File("${script}.substituted");
        subst_file.deleteOnExit();

        String[] args = _args.toArray();
        int returnValue = Main.run(args, null);
        if (returnValue) {
          throw new GradleException('Syntax checker found errors');
        }
        _args.clear();
      }
    }
  }
}
