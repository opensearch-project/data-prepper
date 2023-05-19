/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;


public class SourceInfoProvider {

    private static final Logger LOG = LoggerFactory.getLogger(SourceInfoProvider.class);

    private String datasource;

    private static final String GET_REQUEST_MEHTOD = "GET";

    private static final String CONTENT_TYPE = "content-type";

    private static final String CONTENT_TYPE_VALUE = "application/json";

    private static final String VERSION = "version";

    private static final String DISTRIBUTION = "distribution";

    private static final String ELASTIC_SEARCH = "elasticsearch";

    private static final String CLUSTER_STATS_ENDPOINTS = "_cluster/stats";

    private static final String CLUSTER_HEALTH_STATUS = "status";

    private static final String CLUSTER_HEALTH_STATUS_RED = "red";

    private static final String NODES = "nodes";

    private static final String VERSIONS = "versions";

    /**
     *
     *
      * @param openSearchSourceConfiguration
     * @return
     * This method will help to identify the source information eg(opensearch,elasticsearch)
     */
      public String getSourceInfo(final OpenSearchSourceConfiguration openSearchSourceConfiguration) {
        try {
            JSONParser jsonParser = new JSONParser();
            StringBuilder response = new StringBuilder();
            if (StringUtils.isBlank(openSearchSourceConfiguration.getHosts().get(0)))
                throw new IllegalArgumentException("Hostname cannot be null or empty");
            URL obj = new URL(openSearchSourceConfiguration.getHosts().get(0));
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
                    datasource = String.valueOf(entry.getValue());
                }
            }
        } catch (Exception e) {
          LOG.error("Error while getting data source",e);
        }
        if (datasource == null)
            datasource = ELASTIC_SEARCH;
        return datasource;
    }

    /**
     *
     * @param openSearchSourceConfiguration
     * @param sourceInfo
     * @return
     * @throws IOException
     * @throws ParseException
     * This method will check health of the source. if green or yellow and then it can be used for further processing
     */
    public SourceInfo checkStatus(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final SourceInfo sourceInfo) throws IOException, ParseException {
        String osVersion = null;
        URL obj = new URL(openSearchSourceConfiguration.getHosts().get(0) + CLUSTER_STATS_ENDPOINTS);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod(GET_REQUEST_MEHTOD);
        con.setRequestProperty(CONTENT_TYPE, CONTENT_TYPE_VALUE);
        int responseCode = con.getResponseCode();
        JSONParser jsonParser = new JSONParser();
        StringBuilder response = new StringBuilder();
        String status;
        if (responseCode == HttpURLConnection.HTTP_OK) {
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
            LOG.info("Response is {} ", response);
        } else {
            LOG.error("Connection is down");
        }
        JSONObject jsonObject = (JSONObject) jsonParser.parse(String.valueOf(response));
        status = (String) jsonObject.get(CLUSTER_HEALTH_STATUS);
        if (status.equalsIgnoreCase(CLUSTER_HEALTH_STATUS_RED))
            sourceInfo.setHealthStatus(false);
        Map<String, String> nodesMap = ((Map) jsonObject.get(NODES));
        for (Map.Entry<String, String> entry : nodesMap.entrySet()) {
            if (entry.getKey().equals(VERSIONS)) {
                osVersion = String.valueOf(entry.getValue());
                sourceInfo.setOsVersion(osVersion);
            }
        }
        LOG.info("version Number  : {} ", osVersion);
        return sourceInfo;
    }


}