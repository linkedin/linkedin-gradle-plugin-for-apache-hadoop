package com.linkedin.gradle

import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification
import spock.lang.Unroll

class HadoopDslTestCase extends Specification {
    @Rule
    TemporaryFolder tmp = new TemporaryFolder()
    def buildDotGradle
    def settingsDotGradle
    def testProfile

    def setup() {
        tmp.create()
        buildDotGradle = tmp.newFile('build.gradle')
        settingsDotGradle = tmp.newFile('settings.gradle')
        def profilesDir = tmp.newFolder('src', 'main', 'profiles')
        testProfile = new File(profilesDir, 'testProfile.gradle')
    }

    @Unroll
    def "Hadoop DSL test case for the file #filename"() {
        given:
        testProfile << this.class.classLoader.getResource('gradle/testProfile.gradle').text
        def resourcePath = "gradle/${shouldPass ? 'positive' : 'negative'}/${filename}.gradle"
        buildDotGradle << this.class.classLoader.getResource(resourcePath).text
        def projectName = 'hadoop-plugin-test'
        settingsDotGradle << """rootProject.name='${projectName}'"""

        when:
        GradleRunner runner = GradleRunner.create()
                .withProjectDir(tmp.root)
                .withPluginClasspath()
                .withArguments('buildAzkabanFlows', '-is')

        BuildResult result = shouldPass ? runner.build() : runner.buildAndFail()

        then:
        def expectedOutputLines = this.class.classLoader.getResource("expectedOutput/${shouldPass ? 'positive' : 'negative'}/${filename}.out").readLines()
        expectedOutputLines.each { line ->
            assert result.output.contains(line)
        }
        if (shouldPass) {
            result.task(':buildAzkabanFlows').outcome == TaskOutcome.SUCCESS
            def jobsFolder = new File(tmp.root, "jobs/${filename}")
            if (jobsFolder.exists()) {
                def expectedJobsPath = "expectedJobs/${filename}"
                jobsFolder.eachFile { actualFile ->
                    def expectedFile = this.class.classLoader.getResource("${expectedJobsPath}/${actualFile.name}")
                    // operations inside closure require explicit assert statements
                    assert expectedFile != null
                    if (actualFile.isFile()) {
                        assert actualFile.text == expectedFile.text
                    }
                }
            }
        }

        where:
        filename                         | shouldPass
        'applyProfile'                   | true
        'applyUserProfile'               | true
        'azkabanZip'                     | true
        'basicFlow'                      | true
        'basicFlowMultiple'              | true
        'basicFlowNotYaml'               | true
        'classes1'                       | true
        'cloneJobWithCondition'          | true
        'cloneLookup'                    | true
        'cloneSubflows'                  | true
        'closures'                       | true
        'conditionalFlow'                | true
        'definitionSet'                  | true
        'embeddedConditionalFlow'        | true
        'embeddedFlow'                   | true
        'embeddedFlowWithBaseProperties' | true
        'emergentFlow'                   | true
        'flowWithVariableSubstitution'   | true
        'fullyQualifiedLookup'           | true
        'generateYamlOutputTwice'        | true
        'groovy1'                        | true
        'groupingWorkflows'              | true
        'jobs1'                          | true
        'loadPropsFlow'                  | true
        'lookups'                        | true
        'multiQuery'                     | true
        'names'                          | true
        'namespacedFlows'                | true
        'namespaces'                     | true
        'propertyFiles1'                 | true
        'propertySet1'                   | true
        'readWriteRace1'                 | true
        'readWriteRace2'                 | true
        'scope'                          | true
        'subflowReadWriteRace1'          | true
        'subflows1'                      | true
        'triangleDependencies'           | true
        'triggerFlow'                    | true
        'triggerFlowWithoutDep'          | true
        'workflows'                      | true
        'cycles2'                        | false
        'cycles1'                        | false
        'invalidFields'                  | false
        'invalidNames'                   | false
        'missingFields'                  | false
        'missingRequiredParameters'      | false
        'propertySetChecks'              | false
        'propertySetCycles'              | false
        'scope1'                         | false
        'scope2'                         | false
        'scope3'                         | false
        'scope4'                         | false
        'scope5'                         | false
        'scope6'                         | false
        'subflowChecks1'                 | false
        'subflowCycles1'                 | false
        'triggerCheck'                   | false
        'workflowChecks'                 | false
    }

}
