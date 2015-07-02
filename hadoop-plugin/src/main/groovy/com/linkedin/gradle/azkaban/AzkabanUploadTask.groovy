/*
 * Copyright 2014 LinkedIn Corp.
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
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.tasks.TaskAction;
import org.json.JSONException;
import org.json.JSONObject;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static com.linkedin.gradle.azkaban.AzkabanConstants.*;

/**
 * AzkabanUploadTask handles uploading of the zip file to azkaban.
 */
class AzkabanUploadTask extends DefaultTask {

    File archivePath;
    AzkabanProject azkProject;

    @TaskAction
    void upload() {

        // The .azkabanPlugin.json file must specify at least the Azkaban URL
        String azkabanUrl = azkProject.azkabanUrl;
        if (azkabanUrl == null) {
            throw new GradleException("""Please set azkaban.url in the .azkabanPlugin.json file in your project's root directory.""");
        }

        uploadToAzkaban(azkabanUrl, loadSession());
    }

    /**
     * Upload Zip File to Azkaban
     *
     * @param azkabanUrl The azkaban server url
     * @param sessionId The id of the session.
     */
    void uploadToAzkaban(String azkabanUrl, String sessionId) {

        def console = System.console();
        if (console == null) {
            throw new GradleException("\nCannot access the system console. To use the upload task, explicitly set JAVA_HOME to the version specified in product-spec.json and pass --no-daemon in your command.");
        }

        String username = azkProject.azkabanUsername;
        String projName = azkProject.azkabanProjName;
        if (username == null) {
            username = console.readLine("\n" + AZK_USER_NAME + ": ");
            azkProject.azkabanUsername = username;
        }
        if (projName == null) {
            projName = console.readLine("\n" + AZK_PROJ_NAME + ": ");
            azkProject.azkabanProjName = projName;
        }

        // If no previous session is available, obtain a session id from server by sending login credentials.
        if (sessionId == null) {
            println "\nAZKABAN PROPERTIES:";
            println AZK_URL + " = " + azkabanUrl;
            println AZK_USER_NAME + " = " + username;
            println AZK_PROJ_NAME + " = " + azkProject.azkabanProjName;
            println AZK_ZIP_TASK + " = " + azkProject.azkabanZipTask + "\n";
            sessionId =  azkabanLogin(azkabanUrl, username, console.readPassword("\nAzkaban Password: "));
        }

        HttpPost httpPost = new HttpPost(azkabanUrl + "/manager");

        FileBody fileBody = new FileBody(archivePath, "application/zip");
        MultipartEntity mpEntity = new MultipartEntity();
        mpEntity.addPart("file", fileBody);
        mpEntity.addPart("ajax", new StringBody("upload"));
        mpEntity.addPart("project", new StringBody(azkProject.azkabanProjName));
        String azkabanValidatorAutoFix = azkProject.azkabanValidatorAutoFix;
        if (azkabanValidatorAutoFix == null || !azkabanValidatorAutoFix.equals("off")) {
            mpEntity.addPart("fix", new StringBody("on"));
        }
        httpPost.setEntity(mpEntity);

        httpPost.setHeader("Cookie", "azkaban.browser.session.id=" + sessionId);
        httpPost.setHeader("Accept", "*/*");

        HttpClient httpClient = new DefaultHttpClient();
        HttpResponse response = null;
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
            response = httpClient.execute(httpPost);

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new GradleException("Upload Task Failed.\nStatus Line: " + response.getStatusLine().toString() + "\nStatus Code:" + response.getStatusLine().getStatusCode());
            }

            logger.lifecycle("\nUPLOAD");
            logger.lifecycle("--------------------------------------------------------------------------------");
            logger.lifecycle(parseResponse(response.toString()));
            String result = parseContent(response.getEntity().getContent());
            logger.lifecycle("\n" + result);
            logger.lifecycle("--------------------------------------------------------------------------------");

            // Check if the upload is successful
            JSONObject jsonObj;
            try {
                jsonObj = new JSONObject(result);
                if (jsonObj.has("error")) {
                    throw new GradleException(jsonObj.get("error").toString());
                } else {
                    logger.lifecycle("\nZip " + archivePath.toString() + " uploaded successfully");
                }
            } catch (Exception ex) {
                // Check if session has expired. If so, re-login.
                String str = "Login Error";
                if (result.toString().toLowerCase().contains(str.toLowerCase())) {
                    logger.lifecycle("\nSession Expired. Please Re-login.");
                    uploadToAzkaban(azkabanUrl, null);
                } else {
                    throw new JSONException(ex.toString());
                }
            }
        } finally {
            httpClient.getConnectionManager().shutdown();
        }
    }

    /**
     * Request for login to azkaban
     *
     * @param azkabanUrl The azkaban server url
     * @param userName Azkaban Username
     * @param password Azkaban Password
     * @return  The session.id from the response
     */
    String azkabanLogin(String azkabanUrl, String username, char[] password) {
        String sessionId = null;
        HttpPost httpPost = new HttpPost(azkabanUrl);

        List<NameValuePair> urlParameters = new ArrayList<NameValuePair>();
        urlParameters.add(new BasicNameValuePair("action", "login"));
        urlParameters.add(new BasicNameValuePair("username", username));
        urlParameters.add(new BasicNameValuePair("password", password.toString()));
        httpPost.setEntity(new UrlEncodedFormEntity(urlParameters));

        httpPost.setHeader("Accept", "*/*");
        httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");

        HttpClient httpClient = new DefaultHttpClient();
        HttpResponse response = null;
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
            response = httpClient.execute(httpPost);

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new GradleException("Login Attempt Failed.\nStatus Line: " + response.getStatusLine().toString() + "\nStatus Code:" + response.getStatusLine().getStatusCode());
            }

            logger.lifecycle("\nLOGIN");
            logger.lifecycle("--------------------------------------------------------------------------------");
            logger.lifecycle(parseResponse(response.toString()));
            String result = parseContent(response.getEntity().getContent());
            logger.lifecycle("\n" + result);
            logger.lifecycle("--------------------------------------------------------------------------------");

            // Check the status of Login.
            JSONObject jsonObj;
            try {
                jsonObj = new JSONObject(result);
                if (jsonObj.has("error")) {
                    throw new GradleException(jsonObj.get("error").toString());
                }
                if (!jsonObj.has("session.id")) {
                    throw new GradleException("Login Attempt Failed. Session ID couldn't be obtained.");
                } else {
                    sessionId = jsonObj.getString("session.id");
                }
            } catch (Exception ex) {
                throw new Exception(ex.toString());
            }
        } finally {
            httpClient.getConnectionManager().shutdown();
        }

        saveSession(sessionId);
        return sessionId;
    }

    /**
     * Format the response String to remove html tags. This will improve the readability of Azkaban returned
     * warning messages.
     *
     * @param response The HttpResponse from the server
     * @return The parsed Response
     */
     String parseResponse(String response) {
         String newline = System.getProperty("line.separator");
         return HtmlUtil.toText(response).replaceAll("azkaban.failure.message=", newline + newline);
     }

    /**
     * Load the session.id from cookie.file
     *
     * @return sessionId
     */
    String loadSession() {
        File file = new File(System.getProperty("user.home") + "/.azkaban/cookie.file");
        String sessionId = null;
        Scanner sc = null;
        try {
            if (file.exists()) {
                sc = new Scanner(file);
                if (sc.hasNext()) {
                    sessionId = sc.next();
                }
            }
        } finally {
            if (sc != null) {
                sc.close();
            }
        }
        return sessionId;
    }

    /**
     * Get the content from http response
     *
     * @param response
     * @return result
     */
    String parseContent(InputStream response) {
        BufferedReader rd = null;
        StringBuilder result = null;
        try {
            rd = new BufferedReader(new InputStreamReader(response));
            result = new StringBuilder();
            String line = "";
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }
        } catch (IOException ex) {
            throw new IOException(ex.toString());
        } finally {
            if (rd != null) {
                rd.close();
            }
        }
        return result.toString();
    }

    /**
     * Stores the session.id in a file under ~/.azkaban
     *
     * @param sessionId
     */
    void saveSession(String sessionId) {
        if (sessionId == null) {
            throw new GradleException("No session ID obtained to save.");
        }
        // Create a file to save the session ID
        File file = null;
        FileWriter writer = null;
        try {
            File dir = new File(System.getProperty("user.home") + "/.azkaban");
            if (!dir.exists()) {
                if (!dir.mkdirs()) {
                    log.error("Unable to create directories: " + dir.toString());
                    return;
                }
            }
            file = new File(dir, "cookie.file");
            if (file.exists()) {
                if (!file.delete()) {
                    log.error("Unable to delete session file. Path : " + file.toString());
                    return;
                }
            }
            file.createNewFile();
            writer = new FileWriter(file);
            writer.write(sessionId);
            file.setReadOnly();
        } catch (IOException ex) {
            log.error("Unable to store session ID to " + file.toString() + "\n" + ex.toString());
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
}