/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.plugins.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.CompatibilityCheckResponse;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.opensearch.dataprepper.plugins.kafka.configuration.SchemaConfig;
import org.opensearch.dataprepper.plugins.kafka.util.JsonUtils;
import org.opensearch.dataprepper.plugins.kafka.util.RestUtils;
import org.opensearch.dataprepper.plugins.kafka.util.SinkPropertyConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SchemaService {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaService.class);

    private CachedSchemaRegistryClient schemaRegistryClient;

    private static final int cacheCapacity = 100;

    private static final String REGISTER_API_PATH = "/subjects/" + "%s" + "/versions?normalize=false";

    private static final String COMPATIBILITY_API_PATH = "compatibility/subjects/" + "%s" + "/versions/" + "%s";

    private final String schemaString;

    private final String serdeFormat;

    private final String topic;

    private final SchemaConfig schemaConfig;

    private RestUtils restUtils;

    private final JsonUtils jsonUtils = new JsonUtils();


    private SchemaService(SchemaServiceBuilder builder) {
        this.serdeFormat = builder.serdeFormat;
        this.schemaConfig = builder.schemaConfig;
        this.topic = builder.topic;
        this.restUtils = builder.restUtils;
        this.schemaString = getSchemaString();
        this.schemaRegistryClient = builder.cachedSchemaRegistryClient;
    }

    public static class SchemaServiceBuilder {
        private SchemaConfig schemaConfig;
        private String topic;
        private String serdeFormat;
        private RestUtils restUtils;
        private CachedSchemaRegistryClient cachedSchemaRegistryClient;


        public SchemaServiceBuilder() {

        }

        public SchemaServiceBuilder getRegisterationAndCompatibilityService(final String topic,final String serdeFormat,final RestUtils restUtils,
                                                                            final SchemaConfig schemaConfig) {
            this.topic = topic;
            this.serdeFormat = serdeFormat;
            this.restUtils = restUtils;
            this.schemaConfig = schemaConfig;
            this.cachedSchemaRegistryClient = getSchemaRegistryClient(schemaConfig);
            return this;
        }

        public SchemaServiceBuilder getFetchSchemaService(final String topic, final SchemaConfig schemaConfig) {
            this.topic = topic;
            this.schemaConfig = schemaConfig;
            this.cachedSchemaRegistryClient = getSchemaRegistryClient(schemaConfig);
            return this;
        }

        public SchemaService build() {
            return new SchemaService(this);
        }
    }

    public Schema getSchema(final String topic) {
        final String valueToParse = getValueToParse(topic);
        if (ObjectUtils.isEmpty(valueToParse)) {
            return null;
        }
        return new Schema.Parser().parse(valueToParse);

    }

    public String getValueToParse(final String topic) {
        try {
            if (schemaRegistryClient != null) {
                return schemaRegistryClient.
                        getLatestSchemaMetadata(topic).getSchema();
            }
        } catch (IOException | RestClientException e) {
            LOG.warn(e.getMessage());
        }
        return null;
    }


    public void registerSchema(final String topic) {
        try {
            final String oldSchema = getValueToParse(topic);
            if (ObjectUtils.isEmpty(oldSchema)) {
                registerSchema(topic, schemaString);
            } else if (isSchemaDifferent(oldSchema, schemaString) && isSchemasCompatible(schemaString, topic)) {
                registerSchema(topic, schemaString);
            }
        } catch (Exception e) {
            throw new RuntimeException("error occured while  schema registeration " + e.getMessage());
        }
    }

    private void registerSchema(final String topic, final String schemaString) throws IOException, RestClientException {
        final String path = String.format(REGISTER_API_PATH, topic);
        final RegisterSchemaRequest schemaRequest = new RegisterSchemaRequest();
        schemaRequest.setSchema(schemaString);
        schemaRequest.setSchemaType(serdeFormat != null ? serdeFormat.toUpperCase() : null);
        final RegisterSchemaResponse registerSchemaResponse = restUtils.getHttpResponse(schemaRequest.toJson(), path, new TypeReference<>() {
        });
        if (registerSchemaResponse == null) {
            throw new RuntimeException("Schema Registeration failed");
        }
    }


    @NotNull
    private String getSchemaString() {
        if (schemaConfig != null) {
            if (schemaConfig.isCreate()) {
                final String schemaString = getSchemaDefinition();
                if (schemaString == null) {
                    throw new RuntimeException("Invalid schema definition");
                }
                return schemaString;
            }
        }
        return null;
    }


    private String getSchemaDefinition() {
        try {
            if (schemaConfig.getInlineSchema() != null) {
                return schemaConfig.getInlineSchema();
            } else if (schemaConfig.getSchemaFileLocation() != null) {
                return parseSchemaFromJsonFile(schemaConfig.getSchemaFileLocation());
            } else if (checkS3SchemaValidity(schemaConfig.getS3FileConfig())) {
                return getS3SchemaObject(schemaConfig.getS3FileConfig());
            }
        } catch (IOException io) {
            LOG.error(io.getMessage());
        }
        return null;
    }


    private boolean checkS3SchemaValidity(final SchemaConfig.S3FileConfig s3FileConfig) throws IOException {
        if (s3FileConfig.getBucketName() != null && s3FileConfig.getFileKey() != null && s3FileConfig.getRegion() != null) {
            return true;
        } else {
            return false;
        }
    }

    private static S3Client buildS3Client(final String region) {
        final AwsCredentialsProvider credentialsProvider = AwsCredentialsProviderChain.builder()
                .addCredentialsProvider(DefaultCredentialsProvider.create()).build();
        return S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(credentialsProvider)
                .httpClientBuilder(ApacheHttpClient.builder())
                .build();
    }

    private String getS3SchemaObject(final SchemaConfig.S3FileConfig s3FileConfig) throws IOException {
        final S3Client s3Client = buildS3Client(s3FileConfig.getRegion());
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3FileConfig.getBucketName())
                .key(s3FileConfig.getFileKey())
                .build();
        final ResponseInputStream<GetObjectResponse> s3Object = s3Client.getObject(getObjectRequest);
        final Map<String, Object> stringObjectMap = jsonUtils.getReadValue(s3Object, new TypeReference<>() {
        });
        return jsonUtils.getJsonValue(stringObjectMap);
    }

    private String parseSchemaFromJsonFile(final String location) throws IOException {
        final Map<?, ?> jsonMap;
        try {
            jsonMap = jsonUtils.getReadValue(Paths.get(location).toFile(), Map.class);
        } catch (FileNotFoundException e) {
            LOG.error("Schema file not found, Error: {}", e.getMessage());
            throw new IOException("Can't proceed without schema.");
        }
        final Map<Object, Object> schemaMap = new HashMap<>();
        for (final Map.Entry<?, ?> entry : jsonMap.entrySet()) {
            schemaMap.put(entry.getKey(), entry.getValue());
        }
        try {
            return jsonUtils.getJsonValue(schemaMap);
        } catch (Exception e) {
            LOG.error("Unable to parse schema from the provided schema file, Error: {}", e.getMessage());
            throw new IOException("Can't proceed without schema.");
        }
    }

    private static CachedSchemaRegistryClient getSchemaRegistryClient(final SchemaConfig schemaConfig) {
        if (schemaConfig != null && schemaConfig.getRegistryURL() != null) {
            return new CachedSchemaRegistryClient(
                    schemaConfig.getRegistryURL(),
                    cacheCapacity, getSchemaProperties(schemaConfig));
        }
        return null;
    }

    @NotNull
    private static Map getSchemaProperties(final SchemaConfig schemaConfig) {
        final Properties schemaProps = new Properties();
        SinkPropertyConfigurer.setSchemaCredentialsConfig(schemaConfig, schemaProps);
        final Map propertiesMap = schemaProps;
        return propertiesMap;
    }

    private Boolean isSchemaDifferent(final String oldSchema, final String newSchema) throws JsonProcessingException {
        return jsonUtils.isJsonNodeDifferent(jsonUtils.getJsonNode(oldSchema), jsonUtils.getJsonNode(newSchema));

    }


    private Boolean isSchemasCompatible(final String schemaString, final String topic) {
        final String path = String.format(COMPATIBILITY_API_PATH, topic, schemaConfig.getVersion());
        try {
            final RegisterSchemaRequest request = new RegisterSchemaRequest();
            request.setSchema(schemaString);
            request.setSchemaType(serdeFormat != null ? serdeFormat.toUpperCase() : null);
            final CompatibilityCheckResponse compatibilityCheckResponse = restUtils.getHttpResponse(request.toJson(), path, new TypeReference<>() {
            });
            if (ObjectUtils.isEmpty(compatibilityCheckResponse)) {
                return false;
            }
            return compatibilityCheckResponse.getIsCompatible();
        } catch (Exception ex) {
            LOG.error("Error occured in testing compatiblity " + ex.getMessage());
            return false;
        }
    }


}
