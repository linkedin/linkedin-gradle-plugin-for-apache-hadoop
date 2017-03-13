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
package com.linkedin.gradle.azkaban;

import com.linkedin.gradle.util.HtmlUtil;
import com.linkedin.gradle.zip.HadoopZipExtension;
import groovy.json.JsonBuilder;
import groovy.json.JsonSlurper;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.CopySpec;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_PASSWORD;
import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_PROJ_NAME;
import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_URL;
import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_USER_NAME;
import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_VAL_AUTO_FIX;
import static com.linkedin.gradle.azkaban.AzkabanConstants.AZK_ZIP_TASK;

/**
 * AzkabanHelper is a helper class for the Azkaban Tasks.
 */
class AzkabanHelper {

  private final static Logger logger = Logging.getLogger(AzkabanHelper);

  /**
   * Helper method to make a login request to Azkaban.
   *
   * @param azkabanUrl The Azkaban server URL
   * @param userName The username
   * @param password The password
   * @return The session id from the response
   */
  static String azkabanLogin(String azkabanUrl, String userName, char[] password) {
    List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
    urlParameters.add(new BasicNameValuePair("action", "login"));
    urlParameters.add(new BasicNameValuePair("username", userName));
    urlParameters.add(new BasicNameValuePair("password", password.toString()));

    // Clear the password array as soon as it is used. There will still object references to the
    // password floating around in the JVM, but this is still a good practice.
    Arrays.fill(password, ' ' as char);

    HttpPost httpPost = new HttpPost(azkabanUrl);
    httpPost.setEntity(new UrlEncodedFormEntity(urlParameters));
    httpPost.setHeader("Accept", "*/*");
    httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");

    HttpClient httpClient = new DefaultHttpClient();
    try {
      SSLSocketFactory socketFactory = new SSLSocketFactory(new TrustStrategy() {
        @Override
        boolean isTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
          return true;
        }
      });

      Scheme scheme = new Scheme("https", 443, socketFactory);
      httpClient.getConnectionManager().getSchemeRegistry().register(scheme);
      HttpResponse response = httpClient.execute(httpPost);

      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        throw new GradleException("Login attempt failed.\nStatus line: " + response.getStatusLine().toString() + "\nStatus code: " + response.getStatusLine().getStatusCode());
      }

      logger.lifecycle("--------------------------------------------------------------------------------");
      logger.lifecycle(parseResponse(response.toString()));
      String result = parseContent(response.getEntity().getContent());
      // logger.lifecycle("\n" + result);  // Commented out to not display the session id on the screen
      logger.lifecycle("--------------------------------------------------------------------------------");

      JSONObject jsonObj = new JSONObject(result);

      if (jsonObj.has("error")) {
        throw new GradleException(jsonObj.get("error").toString());
      }

      if (!jsonObj.has("session.id")) {
        throw new GradleException("Login attempt failed. The session ID could not be obtained.");
      }

      String sessionId = jsonObj.getString("session.id");
      saveSession(sessionId);
      return sessionId;
    }
    finally {
      httpClient.getConnectionManager().shutdown();
    }
  }

  /**
   * Configures the fields for azkabanUploadTask.
   *
   * @param azkProject The AzkabanProject
   * @param project The Gradle project
   * @return Whether or not to save the updated fields to the .azkabanPlugin.json file
   */
  static boolean configureTask(AzkabanProject azkProject, Project project) {
    def console = System.console();
    if (console == null) {
      String msg = "\nCannot access the system console. Refer to https://github.com/linkedin/linkedin-gradle-plugin-for-apache-hadoop/wiki/Azkaban-Features#password-masking";
      throw new GradleException(msg);
    }

    logger.lifecycle("Entering interactive mode. You can use the -PskipInteractive command line parameter to skip interactive mode and ONLY read from the .azkabanPlugin.json file.\n");
    logger.lifecycle("Azkaban Project Name: ${azkProject.azkabanProjName}");
    logger.lifecycle("Azkaban URL: ${azkProject.azkabanUrl}");
    logger.lifecycle("Azkaban User Name: ${azkProject.azkabanUserName}");
    logger.lifecycle("Azkaban Zip Task: ${azkProject.azkabanZipTask}");
    if (azkProject.azkabanPassword != null) {
      logger.lifecycle("Azkaban Password: ********\n");
    }

    try {
      def input = "y";
      boolean mustUpdate = (azkProject.azkabanProjName.isEmpty()
        || azkProject.azkabanUrl.isEmpty()
        || azkProject.azkabanUserName.isEmpty()
        || azkProject.azkabanZipTask.isEmpty());
      if (!mustUpdate) {
        input = consoleInput(console, " > Want to change any of the above? [y/N]: ", true);
      }

      if (input.equalsIgnoreCase("y")) {
        input = consoleInput(console, "${mustUpdate ? ' > ' : ''}New Azkaban project name ${azkProject.azkabanProjName.isEmpty() ? '' : "[enter to accept '${azkProject.azkabanProjName}']" }: ", mustUpdate);
        while (azkProject.azkabanProjName.isEmpty() && input.isEmpty()) {
          input = consoleInput(console, "New Azkaban project name (nonempty): ", false);
        }
        if (input != null && !input.isEmpty()) {
          azkProject.azkabanProjName = input.toString();
        }

        input = consoleInput(console, "New Azkaban URL ${azkProject.azkabanUrl.isEmpty() ? '' : "[enter to accept '${azkProject.azkabanUrl}']"}: ", false);
        while (azkProject.azkabanUrl.isEmpty() && input.isEmpty()) {
          input = consoleInput(console, "New Azkaban URL (nonempty): ", false);
        }
        if (input != null && !input.isEmpty()) {
          azkProject.azkabanUrl = input.toString();
        }

        input = consoleInput(console, "New Azkaban user name ${azkProject.azkabanUserName.isEmpty() ? '' : "[enter to accept '${azkProject.azkabanUserName}']"}: ", false);
        while (azkProject.azkabanUserName.isEmpty() && input.isEmpty()) {
          input = consoleInput(console, "New Azkaban user name (nonempty): ", false);
        }
        if (input != null && !input.isEmpty()) {
          azkProject.azkabanUserName = input.toString();
        }

        // Display a list of the zips configured in the user's hadoopZip block. This will help the
        // user to enter an appropriate Azkaban Zip task name.
        showConfiguredHadoopZips(project);
        logger.lifecycle("(You can also enter the name of any other Gradle Zip task whose zip you want upload to Azkaban)\n");

        input = consoleInput(console, " > New Azkaban Zip task ${azkProject.azkabanZipTask.isEmpty() ? '' : "[enter to accept '${azkProject.azkabanZipTask}']"}: ", true);
        while (azkProject.azkabanZipTask.isEmpty() && input.isEmpty()) {
          input = consoleInput(console, "New Azkaban Zip task (nonempty): ", false);
        }
        if (input != null && !input.isEmpty()) {
          azkProject.azkabanZipTask = input.toString();
        }

        if (azkProject.azkabanPassword != null) {
          input = consoleSecretInput(console, "New Azkaban password ${azkProject.azkabanPassword.isEmpty() ? '' : "[enter to accept current password]"}: ", false);
          if (input != null && !input.isEmpty()) {
            azkProject.azkabanPassword = input.toString();
          }
        }

        input = consoleInput(console, "Save these changes to the .azkabanPlugin.json file? [Y/n]: ", false);
        return !input.equalsIgnoreCase("n");
      }
    } catch (IOException ex) {
      logger.error("Failed in taking input from user in interactive mode." + "\n" + ex.toString());
    }

    return false;
  }

  /**
   * Helper routine to get input from the system console.
   *
   * @param console Using existing console, thereby preventing NullPointerException
   * @param message Message to be printed for taking input
   * @param shortDelay Whether or not to introduce a short delay to allow Gradle to complete writing to the console
   * @return The trimmed input
   */
  static String consoleInput(Console console, String message, boolean shortDelay) {
    // Give Gradle time to flush the logger to the screen and write its progress log line at the
    // bottom of the screen, so we can augment this line with a prompt for the input
    if (shortDelay) {
      sleep(500);
    }

    console.format(message).flush();
    return console.readLine().trim();
  }

  static String  consoleSecretInput(Console console, String message, boolean shortDelay) {
    // Give Gradle time to flush the logger to the screen and write its progress log line at the
    // bottom of the screen, so we can augment this line with a prompt for the input
    if (shortDelay) {
      sleep(500);
    }

    console.format(message).flush();
    return console.readPassword().toString();
  }

  /**
   * Fetch Sorted flows from JSON Response
   *
   * @param responseJson The response object from Azkaban API call "fetchprojectflows"
   * @return flows The list of flow names sorted in lexicographic order
   */
  static List<String> fetchSortedFlows(JSONObject responseJson) throws GradleException {
    List<String> flows = new ArrayList<String>();
    if (responseJson.has("flows")) {
      JSONArray jflows = responseJson.getJSONArray("flows");

      if (!jflows.length()) {
        String projectName = null;
        if (responseJson.has("project")) {
          projectName = responseJson.get("project").toString();
        }
        throw new GradleException("No defined flows in project : ${projectName}");
      }

      for (int i = 0; i < jflows.length(); i++) {
        JSONObject jflow = jflows.getJSONObject(i);
        if (jflow.has("flowId")) {
          flows.add(jflow.get("flowId").toString());
        }
      }
      Collections.sort(flows);
    }

    return flows;
  }

  /**
   * Gets the content from the HTTP response.
   *
   * @param The HTTP response as an input stream
   * @return The content from the response
   */
  static String parseContent(InputStream response) {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new InputStreamReader(response));
      StringBuilder result = new StringBuilder();
      String line = null;
      while ((line = reader.readLine()) != null) {
        result.append(line);
      }
      return result.toString();
    }
    finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Format the response String to remove HTML tags. This will improve the readability of warning
   * messages returned from Azkaban.
   *
   * @param response The HttpResponse from the server
   * @return The parsed response
   */
  static String parseResponse(String response) {
    String newline = System.getProperty("line.separator");
    return HtmlUtil.toText(response).replaceAll("azkaban.failure.message=", newline + newline);
  }

  /**
   * Pretty-Prints the Flow Statistics
   *
   * @param flow Azkaban flowname
   * @param execId Execution Id of the flow
   * @param status Execution status of the flow
   * @param jobStatus List of status statistics for jobs of the flow
   */
  static void printFlowStats(String flow, String execId, String status, List<String> jobStatus) {
    System.out.print(String.format("%-35s", flow));
    System.out.print(String.format("| %-15s", execId));
    System.out.print(String.format("| %-10s", status));
    if(execId == "NONE") {
      jobStatus.each {
        System.out.print(String.format("| %-10s", "-"));
      }
    } else {
      jobStatus.each { values ->
        System.out.print(String.format("| %-10s", values));
      }
    }

    System.out.println();
    System.out.flush();
  }

  /**
   * Pretty-Prints the Job Statistics
   *
   * @param jobName Job name
   * @param jobType Type of job like pigli, hive, javaJob etc
   * @param jobStatus Status of the Job
   * @param jobStartTime Start time of the Job
   * @param jobEndTime End time of the Job
   * @param elapsed Elapsed time after the Job execution starts until end time
   */
  static void printJobStats(String jobName, String jobType, String jobStatus, String jobStartTime, String jobEndTime, String elapsed) {
    System.out.print(String.format("%-50s", jobName));
    System.out.print(String.format("| %-10s", jobType));
    System.out.print(String.format("| %-12s", jobStatus));
    System.out.print(String.format("| %-20s", jobStartTime));
    System.out.print(String.format("| %-20s", jobEndTime));
    System.out.print(String.format("| %-20s", elapsed));

    System.out.println();
    System.out.flush();
  }

  /**
   * Reads the session id from session.file in the user's ~/.azkaban directory.
   *
   * @return sessionId The saved session id
   */
  static String readSession() {
    File file = new File(System.getProperty("user.home") + "/.azkaban/session.properties");
    String sessionId = null;
    if (file.exists()) {
      file.withInputStream { inputStream ->
        Properties properties = new Properties();
        properties.load(inputStream);
        sessionId = properties.getProperty("sessionId");
      }
    }
    return sessionId;
  }

  /**
   * Generates sessionId in case it is previously null by logging in to Azkaban.
   *
   * @param sessionId Current Azkaban Session ID
   * @param azkProject The Gradle Project
   * @return sessionId Updated Session ID
   */
  static String resumeOrGetSession(String sessionId, AzkabanProject azkProject) throws GradleException {
    // If no previous session is available, obtain a session id from server by sending login credentials.
    if (sessionId == null) {
      logger.lifecycle("No previous session found. Logging into Azkaban.\n");
      logger.lifecycle("Azkaban Project Name: ${azkProject.azkabanProjName}");
      logger.lifecycle("Azkaban URL: ${azkProject.azkabanUrl}");
      logger.lifecycle("Azkaban Zip Task: ${azkProject.azkabanZipTask}");
      logger.lifecycle("Azkaban User Name: ${azkProject.azkabanUserName}");

      if ( azkProject.azkabanPassword == null) {
        def console = System.console();
        if (console == null) {
          String msg = "\nCannot access the system console. To use this task, explicitly set JAVA_HOME to the version specified in product-spec.json (at LinkedIn) and pass --no-daemon in your command.";
          throw new GradleException(msg);
        }
        // Give Gradle time to flush the logger to the screen and write its progress log line at the
        // bottom of the screen, so we can augment this line with a prompt for the password
        sleep(500);
        console.format(" > Enter password: ").flush();
        sessionId = azkabanLogin(azkProject.azkabanUrl, azkProject.azkabanUserName, System.console().readPassword());
      } else {
        logger.lifecycle("Azkaban Password: *********");
        sessionId = azkabanLogin(azkProject.azkabanUrl, azkProject.azkabanUserName, azkProject.azkabanPassword.toCharArray());
      }
    }
    else {
      logger.lifecycle("\nResuming previous Azkaban session");
    }

    return sessionId;
  }

  /**
   * Stores the session id in a properties file under ~/.azkaban.
   *
   * @param sessionId The session id
   */
  static void saveSession(String sessionId) {
    if (!sessionId) {
      throw new GradleException("No session ID obtained to save");
    }

    File dir = new File(System.getProperty("user.home") + "/.azkaban");
    if (!dir.exists() && !dir.mkdirs()) {
      logger.error("Unable to create directory: " + dir.toString());
      return;
    }

    File file = new File(dir, "session.properties");
    if (file.exists() && !file.delete()) {
      logger.error("Unable to delete the existing file at: " + file.toString());
      return;
    }

    try {
      file.withWriter { writer ->
        Properties properties = new Properties();
        properties.setProperty("sessionId", sessionId);
        properties.store(writer, null);
      }

      // Make the file readable only by the user. The Java File API makes this a little awkward to express.
      file.setReadable(false, false);
      file.setReadable(true, true);
    }
    catch (IOException ex) {
      logger.error("Unable to store session ID to file: " + file.toString() + "\n" + ex.toString());
    }
  }

  /**
   * Helper function to display the zips configured with the HadoopZipExtension to the screen.
   * <p>
   * This is intended to make it easier for users to understand how the zips they configure
   * in the hadoopZip block are translated into Gradle Zip tasks.
   *
   * @param The Gradle project
   * @return Whether or not there are any zips configured with with the HadoopZipExtension
   */
  static boolean showConfiguredHadoopZips(Project project) {
    HadoopZipExtension hadoopZipExtension = project.extensions.getByName("hadoopZip");
    boolean foundZips = !hadoopZipExtension.zipMap.isEmpty();

    if (foundZips) {
      logger.lifecycle("\nThe following zips are declared in the hadoopZip block");
      logger.lifecycle("------------------------------------------------------");
      def counter = 1;
      hadoopZipExtension.zipMap.each { String zipName, CopySpec copySpec ->
        logger.lifecycle("${counter++}. ${zipName} : Enter '${zipName}HadoopZip' to use this zip");
      }
      logger.lifecycle("------------------------------------------------------");
    } else {
      logger.lifecycle("\nNo zips configured in the hadoopZip block. Consider using the hadoopZip block to easily configure Hadoop zips for your project.");
    }

    return foundZips;
  }


  /**
   * Helper method to read the plugin json file as a JSON object. For backwards compatibility
   * reasons, we should read it as a JSON object instead of coercing it into a domain object.
   *
   * @param project The Gradle project
   * @param jsonPath The path of the json file
   * @return A JSON object or null if the file does not exist
   */
  static def readAzkabanPluginJson(Project project, String jsonPath) {
    String pluginJsonPath = jsonPath;
    if (!new File(pluginJsonPath).exists()) {
      return null;
    }

    def reader = null;
    try {
      reader = new BufferedReader(new FileReader(pluginJsonPath));
      def slurper = new JsonSlurper();
      def pluginJson = slurper.parse(reader);
      return pluginJson;
    }
    catch (Exception ex) {
      throw new GradleException("\nError parsing ${pluginJsonPath}.\n" + ex.toString());
    }
    finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Reads the AzkabanProject object from the .azkabanPlugin.json file. Returns null if the file doesn't exist
   *
   * @param project The Gradle project
   * @param filePath The path to the json file
   * @return The created AzkabanProject if file exists, null if file doesn't exist
   */
  static AzkabanProject readAzkabanProjectFromJson(Project project, String filePath) {

    def pluginJson = AzkabanHelper.readAzkabanPluginJson(project, filePath);

    AzkabanProject azkProject = new AzkabanProject();

    if (pluginJson != null) {
      // If the file exists, the task should use this information, but give the user the chance
      // to confirm or change the values read from the file. If the user changes this information,
      // ask them if they want to save the changes (to the .azkabanPlugin.json file).
      azkProject.azkabanProjName = pluginJson[AZK_PROJ_NAME];
      azkProject.azkabanUrl = pluginJson[AZK_URL];
      azkProject.azkabanUserName = pluginJson[AZK_USER_NAME];
      azkProject.azkabanValidatorAutoFix = pluginJson[AZK_VAL_AUTO_FIX];
      azkProject.azkabanZipTask = pluginJson[AZK_ZIP_TASK];
      azkProject.azkabanPassword = pluginJson[AZK_PASSWORD];
      return azkProject;
    }

    return null;
  }

  /**
   * Reads the AzkabanProject from the interactive console
   *
   * @param project The Gradle project
   * @param defaultProject A default AzkabanProject to provide suggestions to the users
   * @param filePath The path to the .azkabanPlugin.json, used for overwriting the file
   * @return The created AzkabanProject
   */
  static AzkabanProject readAzkabanProjectFromInteractiveConsole(Project project, AzkabanProject defaultProject, String filePath) {

    AzkabanProject azkProject = defaultProject;

    // Write the updated value to the .azkabanPlugin.json file if user wants
    if (AzkabanHelper.configureTask(azkProject, project)) {
      String updatedPluginJson = new JsonBuilder(azkProject).toPrettyString();
      new File(filePath).write(updatedPluginJson);
    }

    return azkProject;
  }

  /**
   * Prints the list of flows with Indices.
   *
   * @param flows List of flows in the Azkaban Project.
   */
  static void printFlowsWithIndices(List<String> flows) {
    logger.lifecycle("-----    -----\nINDEX    FLOWS\n-----    -----");
    flows.eachWithIndex { String flow, index ->
      logger.lifecycle(" ${index}       ${flow}");
    }
    logger.lifecycle("---------------------------");
  }

  /**
   * Prints each of the responses of flows submitted for execution.
   *
   * @param responseList List of Http Responses from batch of Flow Executions
   */
  static void printFlowExecutionResponses(List<String> responseList) {
    for (String execResponse : responseList) {
      JSONObject execResponseObj = new JSONObject(execResponse);
      if (execResponseObj.has("error") && execResponseObj.has("flow")) {
        logger.error("Could not execute flow : ${execResponseObj.get("flow")}");
      } else if(execResponseObj.has("message")) {
        logger.lifecycle("${execResponseObj.get("flow")} : ${execResponseObj.get("message")}");
      } else {
        logger.error("Could not execute flow(s) ${execResponseObj}");
      }
    }
  }

}
