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


import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;
import org.json.JSONObject;

/**
 * AzkabanUploadTask handles uploading of the zip file to azkaban.
 */
class AzkabanUploadTask extends DefaultTask {
  File archivePath;
  AzkabanProject azkProject;
  static boolean showProgress = false; // Enabler for progress bar

  /**
   * The Gradle task action for uploading the zip file to Azkaban.
   */
  @TaskAction
  void upload() {
    if (azkProject.azkabanUrl == null) {
      // The .azkabanPlugin.json file must specify at least the Azkaban URL.
      throw new GradleException("Please set azkaban.url in the .azkabanPlugin.json file in your project directory.");
    }

    uploadToAzkaban(AzkabanHelper.readSession());
  }

  /**
   * Uploads the zip file to Azkaban.
   *
   * @param sessionId The Azkaban session id. If this is null, an attempt will be made to login to Azkaban.
   */
  void uploadToAzkaban(String sessionId) {

    // If no previous session is available, obtain a session id from server by sending login credentials.
    if (sessionId == null) {
      logger.lifecycle("No previous session found. Logging into Azkaban:");
      logger.lifecycle("Azkaban URL: ${azkProject.azkabanUrl}");
      logger.lifecycle("Azkaban user name: ${azkProject.azkabanUserName}");
      logger.lifecycle("Azkaban project name: ${azkProject.azkabanProjName}");
      logger.lifecycle("Azkaban password: ");
      sessionId = AzkabanHelper.azkabanLogin(azkProject.azkabanUrl, azkProject.azkabanUserName, System.console().readPassword());
    }
    else {
      logger.lifecycle("Resuming previous Azkaban session");
    }

    MultipartEntityBuilder mpEntityBuilder = MultipartEntityBuilder.create()
        .addTextBody("ajax", "upload")
        .addBinaryBody("file", archivePath, ContentType.create("application/zip"), archivePath.getName())
        .addTextBody("project", azkProject.azkabanProjName);

    if (!azkProject.azkabanValidatorAutoFix || !azkProject.azkabanValidatorAutoFix.equals("off")) {
      mpEntityBuilder.addTextBody("fix", "on");
    }

    HttpEntity reqEntity = mpEntityBuilder.build();

    HttpPost httpPost = new HttpPost(azkProject.azkabanUrl + "/manager");
    httpPost.setEntity(reqEntity);
    httpPost.setHeader("Accept", "*/*");
    httpPost.setHeader("Cookie", "azkaban.browser.session.id=" + sessionId);

    if (showProgress) {
      //Upload status
      int progressLimiter = 0;
      File zipfile = new File(archivePath.toString())
      int sizeInKB = zipfile.length() / 1024;
      logger.lifecycle("Zip Upload Progress...");
      logger.lifecycle("0%                                                                                                100%(" + sizeInKB.toString() + "KB)");
      logger.lifecycle("|                                                                                                  |");

      ProgressHttpEntityWrapper.ProgressCallback progressCallback = new ProgressHttpEntityWrapper.ProgressCallback() {
        @Override
        public void progress(float progress) {
          if((int)progress > progressLimiter && progressLimiter != 100) {
            progressLimiter++;
            print("x");
            System.out.flush();
          }
        }
      }

      httpPost.setEntity(new ProgressHttpEntityWrapper(reqEntity, progressCallback));
    }

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
        throw new GradleException("Upload task failed.\nStatus line: " + response.getStatusLine().toString() + "\nStatus code: " + response.getStatusLine().getStatusCode() + "\nAlternately, you can upload the zip to your project via Azkaban UI.");
      }

      logger.lifecycle("\n--------------------------------------------------------------------------------");
      logger.lifecycle(AzkabanHelper.parseResponse(response.toString()));
      String result = AzkabanHelper.parseContent(response.getEntity().getContent());
      logger.lifecycle("\n" + result);
      logger.lifecycle("--------------------------------------------------------------------------------");

      // Check if session has expired. If so, re-login.
      if (result.toLowerCase().contains("login error")) {
        logger.lifecycle("\nPrevious Azkaban session expired. Please re-login.");
        uploadToAzkaban(null);
        return;
      }

      // Check if there was an error during the upload.
      JSONObject jsonObj = new JSONObject(result);

      if (jsonObj.has("error")) {
        throw new GradleException(jsonObj.get("error").toString());
      }

      logger.lifecycle("\nZip " + archivePath.toString() + " uploaded successfully to " + httpPost.getURI() + "?project=" + azkProject.azkabanProjName);
    }
    finally {
      httpClient.getConnectionManager().shutdown();
    }
  }
}
