package com.linkedin.gradle

import org.gradle.testkit.runner.BuildResult
import org.gradle.testkit.runner.GradleRunner
import org.gradle.testkit.runner.TaskOutcome
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification

import java.util.zip.ZipFile

class BuildHadoopZips extends Specification {

    @Rule
    TemporaryFolder tmp = new TemporaryFolder()
    def buildDotGradle
    def settingsDotGradle
    def gradleDotProperties

    def setup() {
        tmp.create()
        buildDotGradle = tmp.newFile('build.gradle')
        settingsDotGradle = tmp.newFile('settings.gradle')
        gradleDotProperties = tmp.newFile('gradle.properties')
    }

    /**
     * Integration test for including the sources zip and SCM metadata file in the Hadoop zip
     */
    def 'verify zip contents'() {
        given:
        def projectName = 'build-hadoop-zips'
        def version = '1.0.0'
        buildDotGradle << this.class.classLoader.getResource('buildZips/buildZips.gradle').text
        settingsDotGradle << """rootProject.name='${projectName}'"""
        gradleDotProperties << """version=${version}"""
        GradleRunner runner = GradleRunner.create()
                .withProjectDir(tmp.root)
                .withPluginClasspath()
                .withArguments('buildHadoopZips', '-is')

        when:
        BuildResult result = runner.build()

        then:
        result.task(':azkabanHadoopZip').outcome == TaskOutcome.SUCCESS
        def azkabanZip = new File(tmp.root, "build/distributions/${projectName}-${version}-azkaban.zip")
        azkabanZip.exists()
        def zipFileContents = new ZipFile(azkabanZip)
        zipFileContents.getEntry('build.gradle') != null
        def buildMetadataJson = zipFileContents.getEntry('buildMetadata.json')
        buildMetadataJson != null
        new Scanner(zipFileContents.getInputStream(buildMetadataJson)).useDelimiter("\\A").next().contains("\"projectDirectory\": \"\"")
        zipFileContents.getEntry("${projectName}-${version}-sources.zip") != null
    }
}
