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
    def dotCrt

    def setup() {
        tmp.create()
        buildDotGradle = tmp.newFile('build.gradle')
        settingsDotGradle = tmp.newFile('settings.gradle')
        gradleDotProperties = tmp.newFile('gradle.properties')
        dotCrt = tmp.newFile('.crt')
    }

    /**
     * Integration test for including the sources zip and SCM metadata file in the Hadoop zip
     */
    def 'verify zip contents'() {
        given:
        def projectName = 'build-hadoop-zips'
        def version = '1.0.0'
        buildDotGradle << this.class.classLoader.getResource('buildZips/buildZipsCRT.gradle').text
        settingsDotGradle << """rootProject.name='${projectName}'"""
        gradleDotProperties << """version=${version}"""
        dotCrt << '''Test .crt file\n'''
        GradleRunner runner = GradleRunner.create()
                .withProjectDir(tmp.root)
                .withPluginClasspath()
                .withArguments('buildHadoopZips', '-is')

        when:
        BuildResult result = runner.build()

        then:
        result.task(':azkabanHadoopZip').outcome == TaskOutcome.SUCCESS
        result.task(':CRTHadoopZip').outcome == TaskOutcome.SUCCESS

        // find CRTHadoopZip, assert contains .crt file (build/distributions/build-hadoop-zips-1.0.0.zip)
        def crtHadoopZip = new File(tmp.root, "build/distributions/${projectName}-${version}.zip")
        crtHadoopZip.exists()
        def crtHadoopZipContents = new ZipFile(crtHadoopZip)
        assert crtHadoopZipContents.getEntry('.crt') != null

        // find azkabanHadoopZip, assert contents (build/distributions/build-hadoop-zips-1.0.0-azkaban.zip)
        //  contents == build.gradle, buildMetadata.json, build-hadoop-zips-1.0.0-sources.zip
        def azkabanZip = new File(tmp.root, "build/distributions/${projectName}-${version}-azkaban.zip")
        azkabanZip.exists()
        def zipFileContents = new ZipFile(azkabanZip)
        zipFileContents.getEntry('build.gradle') != null
        zipFileContents.getEntry('buildMetadata.json') != null
        zipFileContents.getEntry("${projectName}-${version}-sources.zip") != null
    }
}
