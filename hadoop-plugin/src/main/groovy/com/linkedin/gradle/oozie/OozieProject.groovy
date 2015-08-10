package com.linkedin.gradle.oozie;

class OozieProject {

  // The Name of the project. A directory will be created at uploadPath with this name. e.g. OozieProject
  String projectName = "";
  // The URI of the cluster. e.g. webhdfs://eat1-nertznn01.grid.linkedin.com:50070
  String clusterURI = "";
  // The Path where project directory must be created. e.g. webhdfs://eat1-nertznn01.grid.linkedin.com:50070/user/annag/
  String uploadPath = "";
  // The path of local directory which must be uploaded. This shouldn't be here. We should get this value from some other task such as OozieBuildProject or OozieBuildFlows.
  String dirToUpload = "";
}