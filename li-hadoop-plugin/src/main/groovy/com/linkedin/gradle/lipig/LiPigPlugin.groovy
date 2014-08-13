package com.linkedin.gradle.lipig;

import com.linkedin.gradle.pig.PigExtension;
import com.linkedin.gradle.pig.PigPlugin;
import com.linkedin.gradle.pig.PigTaskHelper;

import org.gradle.api.Project;

class LiPigPlugin extends PigPlugin {
  @Override
  PigExtension makePigExtension(Project project) {
    return new LiPigExtension(project);
  }

  @Override
  String buildRemotePigCmd(String relFilePath, Map<String, String> parameters, Map<String, String> jvmProperties) {
    String pigCommand = pigExtension.pigCommand;
    String pigOptions = pigExtension.pigOptions ?: "";
    String pigParams = parameters == null ? "" : PigTaskHelper.buildPigParameters(parameters);
    String jvmParams = jvmProperties == null ? "" : PigTaskHelper.buildJvmParameters(jvmProperties);
    String remoteHostName = pigExtension.remoteHostName;
    String remoteSshOpts = pigExtension.remoteSshOpts;
    String remoteCacheDir = pigExtension.remoteCacheDir;
    String remoteProjDir = "${remoteCacheDir}/${project.name}";
    return "ssh ${remoteSshOpts} -tt ${remoteHostName} 'export PIG_UDFS=${remoteProjDir}; cd ${remoteProjDir}; ${pigCommand} ${jvmParams} ${pigOptions} -f ${relFilePath} ${pigParams}'";
  }
}
