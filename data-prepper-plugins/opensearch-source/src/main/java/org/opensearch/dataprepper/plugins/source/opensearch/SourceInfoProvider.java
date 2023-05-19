/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.cat.indices.IndicesRecord;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.record.Record;
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
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.ArrayList;

/**
 * Reference to Connection Info of OpenSearch, along with health check
 */
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

    private static final String REGULAR_EXPRESSION = "[^a-zA-Z0-9]";

    private static final String OPEN_SEARCH ="opensearch";

    private static final int VERSION_1_3_0 = 130;

    private static final int VERSION_7_10_0 = 7100;

    private static final Integer BATCH_SIZE_VALUE = 1000;

    private  final JsonFactory jsonFactory = new JsonFactory();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private HashMap<String, String> indexMap ;

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
     * This method will check health of the source, if green or yellow, and then it can be used for further processing
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
        return sourceInfo;
    }

    public void versionCheckForOpenSearch(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final SourceInfo sourceInfo, final OpenSearchClient client ,Buffer<Record<Event>> buffer) throws TimeoutException, IOException{
        int osVersionIntegerValue = Integer.parseInt(sourceInfo.getOsVersion().replaceAll(REGULAR_EXPRESSION, ""));
        objectMapper.registerModule(new JavaTimeModule());
        final Duration rate = objectMapper.convertValue(openSearchSourceConfiguration.getSchedulingParameterConfiguration().getRate(), Duration.class);
        if ((sourceInfo.getDataSource().equalsIgnoreCase(OPEN_SEARCH))
                && (osVersionIntegerValue >= VERSION_1_3_0)) {
            new Timer().scheduleAtFixedRate(new OpenSearchPITTask(openSearchSourceConfiguration,buffer,client),openSearchSourceConfiguration.getSchedulingParameterConfiguration().getStartTime().getSecond() , rate.toMillis());
        } else if (sourceInfo.getDataSource().equalsIgnoreCase(OPEN_SEARCH) && (osVersionIntegerValue < VERSION_1_3_0)) {

        }
    }

    public void versionCheckForElasticSearch(final OpenSearchSourceConfiguration openSearchSourceConfiguration, final SourceInfo sourceInfo, final ElasticsearchClient esClient ,Buffer<Record<Event>> buffer)  throws TimeoutException, IOException {
        int osVersionIntegerValue = Integer.parseInt(sourceInfo.getOsVersion().replaceAll(REGULAR_EXPRESSION, ""));
        objectMapper.registerModule(new JavaTimeModule());
        final Duration rate = objectMapper.convertValue(openSearchSourceConfiguration.getSchedulingParameterConfiguration().getRate(), Duration.class);
        if ((sourceInfo.getDataSource().equalsIgnoreCase(ELASTIC_SEARCH))
                && (osVersionIntegerValue >= VERSION_7_10_0)) {
            if( BATCH_SIZE_VALUE < openSearchSourceConfiguration.getSearchConfiguration().getBatchSize()) {
                if(!openSearchSourceConfiguration.getSearchConfiguration().getSorting().isEmpty()) {
                    new Timer().scheduleAtFixedRate(new ElasticSearchPITPaginationTask(openSearchSourceConfiguration,buffer,esClient),openSearchSourceConfiguration.getSchedulingParameterConfiguration().getStartTime().getSecond() , rate.toMillis());
                }
                else{
                    LOG.info("Sort must contain at least one field");
                }
            }
            else {
                new Timer().scheduleAtFixedRate(new ElasticSearchPITTask(openSearchSourceConfiguration,buffer,esClient),openSearchSourceConfiguration.getSchedulingParameterConfiguration().getStartTime().getSecond() , rate.toMillis());
            }

        } else if (sourceInfo.getDataSource().equalsIgnoreCase(ELASTIC_SEARCH) && (osVersionIntegerValue < VERSION_7_10_0)) {
        }
    }

    public void writeClusterDataToBuffer(final String responseBody,final Buffer<Record<Event>> buffer) throws TimeoutException {
        try {
            final JsonParser jsonParser = jsonFactory.createParser(responseBody);
            final Map<String, Object> innerJson = objectMapper.readValue(jsonParser, Map.class);
            Event event = JacksonLog.builder().withData(innerJson).build();
            Record<Event> jsonRecord = new Record<>(event);
            LOG.info("Data is pushed to buffer {} ",jsonRecord);
            buffer.write(jsonRecord, 1200);
        }
        catch (Exception e)
        {
            LOG.error("Unable to parse json data [{}], assuming plain text", responseBody, e);
            final Map<String, Object> plainMap = new HashMap<>();
            plainMap.put("message", responseBody);
            Event event = JacksonLog.builder().withData(plainMap).build();
            Record<Event> jsonRecord = new Record<>(event);
            buffer.write(jsonRecord, 1200);
        }
    }

    public List<IndicesRecord> callCatOpenSearchIndices(final OpenSearchClient client) throws IOException,ParseException {
        List<IndicesRecord> indexInfoList = client.cat().indices().valueBody();
        return indexInfoList;
    }

    public List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord> callCatElasticIndices(final ElasticsearchClient client) throws IOException,ParseException {
        List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord> indexInfoList = client.cat().indices().valueBody();
        return indexInfoList;
    }
    public HashMap<String, String> getElasticSearchIndexMap(final List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord> indexInfos) {
        HashMap<String,String> indexMap = new HashMap<>();
        for(co.elastic.clients.elasticsearch.cat.indices.IndicesRecord indexInfo : indexInfos) {
            String indexName = indexInfo.index();
            String indexSize = indexInfo.storeSize();
            indexMap.put(indexName,indexSize);
        }
        return indexMap;
    }

    public HashMap<String, String> getOpenSearchIndexMap(final List<IndicesRecord> indexInfos) {
        HashMap<String,String> indexMap = new HashMap<>();
        for(IndicesRecord indexInfo : indexInfos) {
            String indexName = indexInfo.index();
            String indexSize = indexInfo.storeSize();
            indexMap.put(indexName,indexSize);
        }
        return indexMap;
    }

    public  List<IndicesRecord> getOpenSearchIndicesRecords(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                            final List<IndicesRecord>  indicesRecords) {
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
            List<String> filteredIncludeIndexes = openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().stream()
                    .filter(index -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(index))).collect(Collectors.toList());
            openSearchSourceConfiguration.getIndexParametersConfiguration().setInclude(filteredIncludeIndexes);
        }
        openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().forEach(index -> {
            IndicesRecord indexRecord =
                    new IndicesRecord.Builder().index(index).build();
            indicesRecords.add(indexRecord);

        });
        return indicesRecords;
    }

    public  List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord> getElasticSearchIndicesRecords(final OpenSearchSourceConfiguration openSearchSourceConfiguration,
                                                                                                            final List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord>  indicesRecords) {
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
            List<String> filteredIncludeIndexes = openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().stream()
                    .filter(index -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(index))).collect(Collectors.toList());
            openSearchSourceConfiguration.getIndexParametersConfiguration().setInclude(filteredIncludeIndexes);
        }
        openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().forEach(index -> {
            co.elastic.clients.elasticsearch.cat.indices.IndicesRecord indexRecord =
                    new co.elastic.clients.elasticsearch.cat.indices.IndicesRecord.Builder().index(index).build();
            indicesRecords.add(indexRecord);

        });
        return indicesRecords;
    }

    public List<IndicesRecord> getCatOpenSearchIndices(final OpenSearchSourceConfiguration openSearchSourceConfiguration, OpenSearchClient client) throws IOException, ParseException {
        List<IndicesRecord> catIndices = new ArrayList<>();
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude() == null ||
                openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().isEmpty()) {
            catIndices = callCatOpenSearchIndices(client);

            if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                    && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
                catIndices = catIndices.stream().filter(c -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(c.index()))).
                        collect(Collectors.toList());
            }
        } else {
            catIndices = getOpenSearchIndicesRecords(openSearchSourceConfiguration, catIndices);
        }

        indexMap = getOpenSearchIndexMap(catIndices);
        return catIndices;
    }

    public List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord> getCatElasticIndices(final OpenSearchSourceConfiguration openSearchSourceConfiguration, ElasticsearchClient client) throws IOException, ParseException {
        List<co.elastic.clients.elasticsearch.cat.indices.IndicesRecord> catIndices = new ArrayList<>();
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude() == null ||
                openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().isEmpty()) {
            catIndices = callCatElasticIndices(client);

            if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                    && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
                catIndices = catIndices.stream().filter(c -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(c.index()))).
                        collect(Collectors.toList());
            }
        } else {
            catIndices = getElasticSearchIndicesRecords(openSearchSourceConfiguration, catIndices);
        }

        indexMap = getElasticSearchIndexMap(catIndices);
        return catIndices;

    }

    public List<IndicesRecord> getCatIndicesOpenSearch(final OpenSearchSourceConfiguration openSearchSourceConfiguration, OpenSearchClient client) throws IOException, ParseException {
        List<IndicesRecord> catIndices = new ArrayList<>();
        if (openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude() == null ||
                openSearchSourceConfiguration.getIndexParametersConfiguration().getInclude().isEmpty()) {
            catIndices = callCatOpenSearchIndices(client);

            if (openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude() != null
                    && !openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().isEmpty()) {
                catIndices = catIndices.stream().filter(c -> !(openSearchSourceConfiguration.getIndexParametersConfiguration().getExclude().contains(c.index()))).
                        collect(Collectors.toList());
            }
        } else {
            catIndices = getOpenSearchIndicesRecords(openSearchSourceConfiguration, catIndices);
        }

        indexMap = getOpenSearchIndexMap(catIndices);
        LOG.info("Indexes  are {} :  ", indexMap);
        return catIndices;
    }
}