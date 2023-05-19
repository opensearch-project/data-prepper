/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;


public class PrepareConnection {

    private static final String HEADER_NAME = "X-Elastic-Product";

    private static final String HEADER_VALUE = "Elasticsearch";

    /**
     * @param openSearchSourceConfiguration
     * @return
     * @throws MalformedURLException This method create opensearch client based on host information, which will be used to call opensearch apis
     */
    public OpenSearchClient prepareOpensearchConnection(final OpenSearchSourceConfiguration openSearchSourceConfiguration) throws MalformedURLException {
        URL urlLink = getHostDetails(openSearchSourceConfiguration.getHosts().get(0));
        final HttpHost host = new HttpHost(urlLink.getProtocol(), urlLink.getHost(), urlLink.getPort());
        final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        final OpenSearchTransport transport = ApacheHttpClient5TransportBuilder
                .builder(host)
                .setMapper(new org.opensearch.client.json.jackson.JacksonJsonpMapper())
                .build();
        return new OpenSearchClient(transport);
    }

    /**
     * @param openSearchSourceConfiguration
     * @return
     * @throws MalformedURLException This method create Elasticsearch client based on host information, which will be used to call opensearch apis
     */
    public ElasticsearchClient prepareElasticSearchConnection(final OpenSearchSourceConfiguration openSearchSourceConfiguration) throws MalformedURLException {
        URL urlLink = getHostDetails(openSearchSourceConfiguration.getHosts().get(0));
        RestClient client = org.elasticsearch.client.RestClient.builder(new org.apache.http.HttpHost(urlLink.getHost(), urlLink.getPort())).
                setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                        .setDefaultHeaders(List.of(new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())))
                        .addInterceptorLast((HttpResponseInterceptor) (response, context) -> response.addHeader(HEADER_NAME, HEADER_VALUE))).build();
        JacksonJsonpMapper jacksonJsonpMapper = new JacksonJsonpMapper();
        ElasticsearchTransport transport = new RestClientTransport(client, jacksonJsonpMapper);
        return new ElasticsearchClient(transport);
    }

    private URL getHostDetails(final String url) throws MalformedURLException {
        URL urlLink = new URL(url);
        return urlLink;
    }
}




