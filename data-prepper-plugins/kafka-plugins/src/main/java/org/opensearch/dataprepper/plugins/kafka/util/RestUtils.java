/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.util;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.jetbrains.annotations.NotNull;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class RestUtils {

    private final RestService restService;

    private static String method = "POST";
    private static String CONTENT_TYPE_KEY = "Content-Type";
    private static String CONTENT_TYPE_JSON_VALUE = "application/vnd.schemaregistry.v1+json";

    private final SchemaConfig schemaConfig;

    public RestUtils(final SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        this.restService = getRestService(schemaConfig.getRegistryURL());
    }

    @NotNull
    private RestService getRestService(final String url) {
        final RestService restService = new RestService(url);
        restService.configure(getSchemaProperties());
        return restService;
    }


    @NotNull
    private Map<String, String> getRequestProperties() {
        final Map requestProperties = new HashMap();
        requestProperties.put(CONTENT_TYPE_KEY, CONTENT_TYPE_JSON_VALUE);
        return requestProperties;
    }

    @NotNull
    private Map getSchemaProperties() {
        final Properties schemaProps = new Properties();
        SinkPropertyConfigurer.setSchemaCredentialsConfig(schemaConfig, schemaProps);
        final Map propertiesMap = schemaProps;
        return propertiesMap;
    }


    public <T> T getHttpResponse(final String schemaString, final String path, final TypeReference<T> responseFormat) throws IOException, RestClientException {
        return restService.
                httpRequest(path, method,
                        schemaString.getBytes(StandardCharsets.UTF_8),
                        getRequestProperties(), responseFormat);
    }
}
