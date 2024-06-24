/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.servicecomb.config.kie.client;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.http.HttpStatus;

import org.apache.servicecomb.config.common.exception.OperationException;
import org.apache.servicecomb.config.kie.client.model.ConfigConstants;
import org.apache.servicecomb.config.kie.client.model.ConfigurationsRequest;
import org.apache.servicecomb.config.kie.client.model.ConfigurationsResponse;
import org.apache.servicecomb.config.kie.client.model.KVCreateBody;
import org.apache.servicecomb.config.kie.client.model.KVDoc;
import org.apache.servicecomb.config.kie.client.model.KVResponse;
import org.apache.servicecomb.config.kie.client.model.KieAddressManager;
import org.apache.servicecomb.config.kie.client.model.KieConfiguration;
import org.apache.servicecomb.config.kie.client.model.LabelType;
import org.apache.servicecomb.config.kie.client.model.ValueType;
import org.apache.servicecomb.http.client.common.HttpRequest;
import org.apache.servicecomb.http.client.common.HttpResponse;
import org.apache.servicecomb.http.client.common.HttpTransport;
import org.apache.servicecomb.http.client.common.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.io.ByteArrayResource;

public class KieClient implements KieConfigOperation {

  private static final Logger LOGGER = LoggerFactory.getLogger(KieClient.class);

  private static final String KEY_APP = "app";

  private static final String KEY_ENVIRONMENT = "environment";

  private static final String KEY_SERVICE = "service";

  protected HttpTransport httpTransport;

  protected String revision = "0";

  private final KieAddressManager addressManager;

  private final KieConfiguration kieConfiguration;

  private final Map<LabelType, Map<String, KVDoc>> kvMap = new HashMap<>();//store KVDoc or only id???

  public static final String DEFAULT_KIE_API_VERSION = "v1";

  public KieClient(KieAddressManager addressManager, HttpTransport httpTransport, KieConfiguration kieConfiguration) {
    this.httpTransport = httpTransport;
    this.addressManager = addressManager;
    this.kieConfiguration = kieConfiguration;
  }

  @Override
  public ConfigurationsResponse queryConfigurations(ConfigurationsRequest request) {
    String address = addressManager.address();
    String url = buildUrl(request, address);
    try {
      if (kieConfiguration.isEnableLongPolling()) {
        url += "&wait=" + kieConfiguration.getPollingWaitInSeconds() + "s";
      }

      HttpRequest httpRequest = new HttpRequest(url, null, null, HttpRequest.GET);
      HttpResponse httpResponse = httpTransport.doRequest(httpRequest);
      ConfigurationsResponse configurationsResponse = new ConfigurationsResponse();
      if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
        revision = httpResponse.getHeader("X-Kie-Revision");
        KVResponse allConfigList = HttpUtils.deserialize(httpResponse.getContent(), KVResponse.class);
        Map<String, KVDoc> map = kvMap.computeIfAbsent(request.getLabelType(), k -> new HashMap<>(16));
        for (KVDoc doc : allConfigList.getData()) {
          map.put(doc.getKey(), doc);
        }
        Map<String, Object> configurations = getConfigByLabel(allConfigList);
        configurationsResponse.setConfigurations(configurations);
        configurationsResponse.setChanged(true);
        configurationsResponse.setRevision(revision);
        addressManager.recordSuccessState(address);
        return configurationsResponse;
      }
      if (httpResponse.getStatusCode() == HttpStatus.SC_BAD_REQUEST) {
        throw new OperationException("Bad request for query configurations.");
      }
      if (httpResponse.getStatusCode() == HttpStatus.SC_NOT_MODIFIED) {
        configurationsResponse.setChanged(false);
        addressManager.recordSuccessState(address);
        return configurationsResponse;
      }
      addressManager.recordFailState(address);
      throw new OperationException(
          "read response failed. status:" + httpResponse.getStatusCode() + "; message:" +
              httpResponse.getMessage() + "; content:" + httpResponse.getContent());
    } catch (Exception e) {
      LOGGER.error("query configuration from {} failed, message={}", url, e.getMessage());
      throw new OperationException("read response failed. ", e);
    }
  }

  @Override
  public boolean createConfiguration(String key, String value, LabelType labelType) {

    String address = addressManager.address();
    String url = buildCommonUrl(address);
    try {
      HttpRequest httpRequest = new HttpRequest(url, null, buildCreateBody(key, value, labelType), HttpRequest.POST);
      return sendRequest(address, httpRequest);
    } catch (Exception e) {
      LOGGER.error("create configuration from {} failed, message={}", url, e.getMessage());
      throw new OperationException("read response failed. ", e);
    }
  }

  @Override
  public boolean updateConfiguration(String key, String value, LabelType labelType) {

    String address = addressManager.address();
    String url = buildCommonUrl(address) + "/";
    try {
      String id = kvMap.get(labelType).get(key).getId();
      url += id;
      HttpRequest httpRequest = new HttpRequest(url, null, buildUpdateBody(key, value), HttpRequest.PUT);
      return sendRequest(address, httpRequest);
    } catch (Exception e) {
      LOGGER.error("update configuration key {} from {} failed, message={}", key, url, e.getMessage());
      throw new OperationException("read response failed. ", e);
    }
  }

  private boolean sendRequest(String address, HttpRequest httpRequest) throws IOException {
    HttpResponse httpResponse = httpTransport.doRequest(httpRequest);
    if (httpResponse.getStatusCode() == HttpStatus.SC_OK) {
      addressManager.recordSuccessState(address);
      return true;
    }
    addressManager.recordFailState(address);
    throw new OperationException(
        "read response failed. status:" + httpResponse.getStatusCode() + "; message:" +
            httpResponse.getMessage() + "; content:" + httpResponse.getContent());
  }

  private String buildCommonUrl(String currentAddress) {
    StringBuilder sb = new StringBuilder();
    sb.append(currentAddress);
    sb.append("/");
    sb.append(DEFAULT_KIE_API_VERSION);
    sb.append("/");
    sb.append(kieConfiguration.getProject());
    sb.append("/kie/kv");
    return sb.toString();
  }

  private String buildCreateBody(String key, String value, LabelType labelType) throws IOException {
    KVCreateBody body = new KVCreateBody();
    body.setKey(key);
    body.setValue(value);
    Map<String, String> labels = new HashMap<String, String>();
    if (labelType == LabelType.APP) {
      labels.put(KEY_APP, kieConfiguration.getAppName());
      labels.put(KEY_ENVIRONMENT, kieConfiguration.getEnvironment());
    } else if (labelType == LabelType.CUSTOM) {
      labels.put(kieConfiguration.getCustomLabel(), kieConfiguration.getCustomLabel());
    } else if (labelType == LabelType.SERVICE) {
      labels.put(KEY_APP, kieConfiguration.getAppName());
      labels.put(KEY_ENVIRONMENT, kieConfiguration.getEnvironment());
      labels.put(KEY_SERVICE, kieConfiguration.getServiceName());
    }
    body.setLabels(labels);
    return HttpUtils.serialize(body);
  }

  private String buildUpdateBody(String key, String value) throws IOException {
    KVCreateBody body = new KVCreateBody();
    body.setKey(key);
    body.setValue(value);
    return HttpUtils.serialize(body);
  }

  private String buildUrl(ConfigurationsRequest request, String currentAddress) {
    StringBuilder sb = new StringBuilder();
    sb.append(currentAddress);
    sb.append("/");
    sb.append(DEFAULT_KIE_API_VERSION);
    sb.append("/");
    sb.append(kieConfiguration.getProject());
    sb.append("/kie/kv?");
    sb.append(request.getLabelsQuery());
    sb.append("&revision=");
    sb.append(request.getRevision());
    if (request.isWithExact()) {
      sb.append("&match=exact");
    }
    return sb.toString();
  }

  private Map<String, Object> getConfigByLabel(KVResponse resp) {
    Map<String, Object> resultMap = new HashMap<>();
    resp.getData().stream()
        .filter(doc -> doc.getStatus() == null || ConfigConstants.STATUS_ENABLED.equalsIgnoreCase(doc.getStatus()))
        .map(this::processValueType)
        .collect(Collectors.toList())
        .forEach(resultMap::putAll);
    return resultMap;
  }

  private Map<String, Object> processValueType(KVDoc kvDoc) {
    ValueType valueType;
    try {
      valueType = ValueType.valueOf(kvDoc.getValueType());
    } catch (IllegalArgumentException e) {
      throw new OperationException("value type not support [" + kvDoc.getValue() + "]");
    }
    Properties properties = new Properties();
    Map<String, Object> kvMap = new HashMap<>();
    try {
      switch (valueType) {
        case yml:
        case yaml:
          YamlPropertiesFactoryBean yamlFactory = new YamlPropertiesFactoryBean();
          yamlFactory.setResources(new ByteArrayResource(kvDoc.getValue().getBytes(StandardCharsets.UTF_8)));
          return toMap(yamlFactory.getObject());
        case properties:
          properties.load(new StringReader(kvDoc.getValue()));
          return toMap(properties);
        case text:
        case string:
        default:
          kvMap.put(kvDoc.getKey(), kvDoc.getValue());
          return kvMap;
      }
    } catch (Exception e) {
      LOGGER.error("read config failed", e);
    }
    return Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> toMap(Properties properties) {
    if (properties == null) {
      return Collections.emptyMap();
    }
    Map<String, Object> result = new HashMap<>();
    Enumeration<String> keys = (Enumeration<String>) properties.propertyNames();
    while (keys.hasMoreElements()) {
      String key = keys.nextElement();
      Object value = properties.getProperty(key);
      result.put(key, value);
    }
    return result;
  }
}
