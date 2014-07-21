package com.linkedin.gradle.lihadoop;

import com.linkedin.gradle.hadoop.HadoopPlugin;
import com.linkedin.gradle.liazkaban.LiAzkabanPlugin;

class LiHadoopPlugin extends HadoopPlugin {
  @Override
  Class getAzkabanPluginClass() {
    return LiAzkabanPlugin.class;
  }
}
