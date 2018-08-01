package com.linkedin.gradle.zip

import org.gradle.api.file.FileVisitDetails;
import org.yaml.snakeyaml.Yaml;

import static com.linkedin.gradle.util.YamlUtils.setupYamlObject;

/**
 * YamlMerge is used to combine .tempprops files and .flow files. This is necessary to provide
 * backward compatibility with Flow 1.0 in Flow 2.0.
 */
class YamlMerge {
  // Yaml object used to read in/write out yaml files.
  static Yaml yamlWorker = setupYamlObject();

  /**
   * Takes in the file path of a .flow file and a list of properties from .tempprops files and
   * merges them.
   *
   * Merging involves taking all the properties defined in the .tempprops files and adding them
   * to the top level workflow config in the .flow file. This definition of merging enables
   * backward compatibility with Flow 1.0.
   *
   * @param flowFilePath Path to .flow that will receive the additional properties
   * @param tempPropsPathsList List of paths to .tempprops files
   * @param zipName The name of the zip to be generated - used to create the new .flow filename
   * @return String The name of the created file to be added to the task's sources
   */
  static String merge(FileVisitDetails flowFile, List<Map<String, String>> tempPropsList,
                      String zipName) {
    Map flow = readInYaml(flowFile);
    if (!flow.containsKey("config")) {
      flow.put("config", [:]);
    }
    tempPropsList.each { tempProps ->
      tempProps.each { tempPropsKey, tempPropsVal ->
        flow["config"][tempPropsKey] = tempPropsVal;
      }
    }
    String newFlowFileName = flowFile.name.replace(".flow", "_${zipName}.flow");
    File file = new File(flowFile.file.getParentFile(), newFlowFileName);
    FileWriter fileWriter = new FileWriter(file);
    yamlWorker.dump(flow, fileWriter);
    // Set to read-only to remind people that they should not be editing auto-generated yaml files.
    file.setWritable(false);
    fileWriter.close();
    return newFlowFileName;
  }

  /**
   * Takes in a yaml file that's assumed to be a Map at the root level and returning it.
   *
   * @param file Yaml file assumed to be a Map at the root level
   * @return Map pulled from the Yaml file
   */
  static Map readInYaml(FileVisitDetails file) {
    InputStream fileInputStream = file.open();
    Map loadedMap = (Map) yamlWorker.load(fileInputStream);
    fileInputStream.close();
    return loadedMap;
  }
}
