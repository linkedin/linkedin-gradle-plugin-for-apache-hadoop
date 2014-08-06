package com.linkedin.gradle.lihadoop;

import com.linkedin.gradle.hadoop.HadoopPlugin;

import com.linkedin.gradle.liazkaban.LiAzkabanPlugin;
import com.linkedin.gradle.lipig.LiPigPlugin;

class LiHadoopPlugin extends HadoopPlugin {
  @Override
  Class getAzkabanPluginClass() {
    return LiAzkabanPlugin.class;
  }

  @Override
  Class getPigPluginClass() {
    return LiPigPlugin.class;
  }
}
