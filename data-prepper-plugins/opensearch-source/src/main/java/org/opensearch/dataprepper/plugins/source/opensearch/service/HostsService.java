/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.source.opensearch.service;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.opensearch.dataprepper.plugins.source.opensearch.model.ServiceInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

/**
 * It takes care of host related data
 */

public class HostsService {

    private static final String GET_REQUEST_MEHTOD = "GET";
    private static final String CONTENT_TYPE = "content-type";
    private static final String CONTENT_TYPE_VALUE = "application/json";
    private static final String DISTRIBUTION = "distribution";
    private static final String NUMBER = "number";
    private static final String VERSION = "version";
    private static final String REGULAR_EXPRESSION = "[^a-zA-Z0-9]";
    private static final String ELASTIC_SEARCH = "elasticsearch";
    private static final Logger LOG = LoggerFactory.getLogger(HostsService.class);
    public ServiceInfo findServiceDetailsByUrl( final String url){
        ServiceInfo serviceInfo = new ServiceInfo();
        try {
            JSONParser jsonParser = new JSONParser();
            StringBuilder response = new StringBuilder();
            if (StringUtils.isBlank(url))
                throw new IllegalArgumentException("Hostname cannot be null or empty");
            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();
            con.setRequestMethod(GET_REQUEST_MEHTOD);
            con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
            int responseCode = con.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    response.append(inputLine);
                }
                in.close();
                LOG.info("Response is  : {} ", response);
            } else {
                LOG.error("GET request did not work.");
            }
            JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
            Map<String, String> versionMap = ((Map) jsonObject.get(VERSION));
            for (Map.Entry<String, String> entry : versionMap.entrySet()) {
                if (entry.getKey().equals(DISTRIBUTION)) {
                    serviceInfo.setDistribution(String.valueOf(entry.getValue()));
                }
                if(entry.getKey().equals(NUMBER)) {
                    serviceInfo.setVersion(Integer.parseInt(String.valueOf(entry.getValue().replaceAll(REGULAR_EXPRESSION, ""))));
                }
            }
        } catch (Exception e) {

            LOG.error("Error while getting data source",e);
        }
        if (serviceInfo.getDistribution() == null)
            serviceInfo.setDistribution(ELASTIC_SEARCH);
        return new ServiceInfo();
    }
}
