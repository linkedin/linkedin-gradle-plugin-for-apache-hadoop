package com.linkedin.gradle.oozie;

public final class OozieConstants {

  // The URI of the cluster. e.g. webhdfs://eat1-nertznn01.grid.linkedin.com:50070
  public static final String OOZIE_CLUSTER_URI = "clusterURI";
  // The Name of the project. A directory will be created at PATH_TO_UPLOAD with this name.
  public static final String OOZIE_PROJECT_NAME = "projectName"
  // The Path where project directory must be created. e.g.  webhdfs://eat1-nertznn01.grid.linkedin.com:50070/user/annag
  public static final String PATH_TO_UPLOAD = "uploadPath"
  // The path of local directory which must be uploaded. This shouldn't be here. We should get this value from some other task such as OozieBuildProject or OozieBuildFlows.
  public static final String DIR_TO_UPLOAD = "dirToUpload"
}
