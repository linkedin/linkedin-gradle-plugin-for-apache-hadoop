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
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

/**
 * Utility class for handling HTTP request and responses.
 * <p>
 * This is currently used by Azkaban classes for HTTP request handling.
 */
class HttpUtil {

  private final static Logger logger = Logging.getLogger(this.class);

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

    try {
      // create a thread for each URI
      GetThread[] threads = new GetThread[uriList.size()];
      for (int i = 0; i < threads.length; i++) {
        HttpGet httpGet = new HttpGet(uriList.get(i));
        threads[i] = new GetThread(httpClient, httpGet);
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
      return responseList; //finally will always execute in spite of return statement

    } finally {
      httpClient.close();
    }
  }

  /**
   * HTTP GET implementation
   *
   * @param uri URI of HTTP GET request
   * @return The response String
   */
  static String responseFromGET(URI uri) {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    try {
      String response = EntityUtils.toString(httpClient.execute(new HttpGet(uri)).getEntity());
      return response;
    } finally {
      httpClient.close();
    }
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
    try {
      String response = EntityUtils.toString(httpClient.execute(httpPost).getEntity());
      return response;
    } finally {
      httpClient.close();
    }
  }

  /**
   * Creates the thread which returns a response object for a get request
   *
   * @param httpClient Closeable Http Client
   * @param httpGet
   * @return response Returns the response string of respective thread.
   */
  static class GetThread extends Thread {
    private final CloseableHttpClient httpClient;
    private final HttpClientContext context;
    private final HttpGet httpGet;
    private String response;

    public GetThread(CloseableHttpClient httpClient, HttpGet httpGet) {
      this.httpClient = httpClient;
      this.context = HttpClientContext.create();
      this.httpGet = httpGet;
    }

    public String getResponse() {
      return this.response;
    }

    @Override
    public void run() {
      try {
        CloseableHttpResponse httpResponse = httpClient.execute(httpGet, context);
        try {
          response = EntityUtils.toString(httpResponse.getEntity());
        } finally {
          httpResponse.close();
        }
      } catch (ClientProtocolException ex) {
        logger.error("ClientProtocolException in Thread creation. \n${ex.getMessage()}");
      } catch (IOException ex) {
        logger.error("IOException in Thread creation. \n${ex.getMessage()}");
      }
    }
  }
}
