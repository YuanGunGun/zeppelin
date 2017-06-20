package org.apache.zeppelin.utils;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by norbertchen on 2017/6/19.
 */
public class HTTPUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HTTPUtils.class);

  public static GetMethod httpGet(String url, String path) throws IOException {
    LOG.info("Connecting to {}", url + path);
    HttpClient httpClient = new HttpClient();
    GetMethod getMethod = new GetMethod(url + path);
    getMethod.addRequestHeader("Origin", url);
    httpClient.executeMethod(getMethod);
    LOG.info("{} - {}", getMethod.getStatusCode(), getMethod.getStatusText());
    return getMethod;
  }

}
