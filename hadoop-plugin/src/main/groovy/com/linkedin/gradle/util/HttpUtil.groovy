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
package com.linkedin.gradle.util;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

/**
 * Utility class for handling HTTP request and responses.
 * <p>
 * This is currently used by azkaban classes for HTTP request handling.
 */
public class HttpUtil {

  private final static Logger logger = Logging.getLogger(HttpUtil);

  /**
   * batchGet requests implementation using threads
   *
   * @param uriList List of uri's for performing GET requests
   * @return responseList The list of all the batch responses
   */
  static List<String> batchGet(List<URI> uriList) {
    PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setDefaultMaxPerRoute(20);
    CloseableHttpClient httpClient = HttpClients.custom().setConnectionManager(connectionManager).build();

    // create a thread for each URI
    GetThread[] threads = new GetThread[uriList.size()];
    for (int i = 0; i < threads.length; i++) {
      HttpGet httpget = new HttpGet(uriList.get(i));
      threads[i] = new GetThread(httpClient, httpget);
    }

    // start the threads
    for (int j = 0; j < threads.length; j++) {
      threads[j].start();
    }

    // join the threads
    for (int j = 0; j < threads.length; j++) {
      threads[j].join();
    }

    List<String> responseList = new ArrayList<String>();
    for (int j = 0; j < threads.length; j++) {
      responseList.add(threads[j].getResponse());
    }

    httpClient.close();

    return responseList;
  }

  /**
   * HTTP GET implementation
   *
   * @param uri URI of HTTP GET request
   * @return The response String
   */
  static String responseFromGET(URI uri) {
    HttpGet httpGet = new HttpGet(uri);
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();

    String response = EntityUtils.toString(httpClient.execute(httpGet).getEntity());
    httpClient.close();

    return response;
  }

  /**
   * HTTP POST implementation
   *
   * @param uri URI of HTTP POST request
   * @return The response String
   */
  static String responseFromPOST(URI uri) {
    HttpPost httpPost = new HttpPost(uri);
    httpPost.setHeader("Accept", "*/*");
    httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");

    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    String response = EntityUtils.toString(httpClient.execute(httpPost).getEntity());
    httpClient.close();

    return response;
  }

  /**
   * Creates the thread which returns a response object for a get request
   *
   * @param httpClient Closeable Http Client
   * @param httpget
   * @return response Returns the response string of respective thread.
   */
  static class GetThread extends Thread {
    private final CloseableHttpClient httpClient;
    private final HttpContext context;
    private final HttpGet httpget;
    private String response;

    public GetThread(CloseableHttpClient httpClient, HttpGet httpget) {
      this.httpClient = httpClient;
      this.context = HttpClientContext.create();
      this.httpget = httpget;
    }

    public String getResponse() {
      return this.response;
    }

    @Override
    public void run() {
      try {
        CloseableHttpResponse httpResponse = httpClient.execute(httpget, context);
        try {
          response = EntityUtils.toString(httpResponse.getEntity());
        } finally {
          httpResponse.close();
        }
      } catch (ClientProtocolException ex) {
        logger.error("ClientProtocolException in Thread creation. \n${ex}");
      } catch (IOException ex) {
        logger.error("IOException in Thread creation. \n${ex}");
      }
    }
  }
}
