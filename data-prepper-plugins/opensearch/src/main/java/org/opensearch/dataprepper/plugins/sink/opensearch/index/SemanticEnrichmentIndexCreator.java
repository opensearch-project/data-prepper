/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.document.Document;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.opensearch.OpenSearchClient;
import software.amazon.awssdk.services.opensearchserverless.OpenSearchServerlessClient;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SemanticEnrichmentIndexCreator {

    private static final Logger LOG = LoggerFactory.getLogger(SemanticEnrichmentIndexCreator.class);

    private final AwsCredentialsProvider credentialsProvider;
    private final Region region;
    private final boolean serverless;
    private final String collectionIdOrDomainName;

    public SemanticEnrichmentIndexCreator(final AwsCredentialsSupplier awsCredentialsSupplier,
                                          final ConnectionConfiguration connectionConfiguration,
                                          final String resourceName) {
        final AwsCredentialsOptions awsCredentialsOptions = connectionConfiguration.createAwsCredentialsOptions();
        this.credentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
        this.region = awsCredentialsOptions.getRegion();
        this.serverless = connectionConfiguration.isServerless();
        this.collectionIdOrDomainName = resolveCollectionIdOrDomainName(
                resourceName, connectionConfiguration.getHosts());
    }

    private String resolveCollectionIdOrDomainName(final String resourceName, final List<String> hosts) {
        if (resourceName != null && !resourceName.isEmpty()) {
            LOG.info("Using configured resource_name: {}", resourceName);
            return resourceName;
        }

        if (serverless) {
            LOG.info("resource_name not configured, extracting collection ID from host URL");
            return OpenSearchEndpointIdentifier.extractCollectionId(hosts);
        }

        LOG.info("resource_name not configured, extracting domain name from host URL");
        return OpenSearchEndpointIdentifier.extractDomainName(hosts);
    }

    public void createIndex(final String indexName, final SemanticEnrichmentConfig semanticConfig) throws IOException {
        if (serverless) {
            createServerlessIndex(indexName, semanticConfig);
            return;
        }
        createManagedDomainIndex(indexName, semanticConfig);
    }

    private void createServerlessIndex(final String indexName,
                                       final SemanticEnrichmentConfig semanticConfig) throws IOException {
        LOG.info("Creating serverless index [{}] with semantic enrichment for fields {}",
                indexName, semanticConfig.getFields());

        try (final OpenSearchServerlessClient client = OpenSearchServerlessClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build()) {

            client.createIndex(software.amazon.awssdk.services.opensearchserverless.model.CreateIndexRequest.builder()
                    .id(collectionIdOrDomainName)
                    .indexName(indexName)
                    .indexSchema(Document.fromMap(toDocumentMap(buildIndexSchema(semanticConfig))))
                    .build());

            LOG.info("Successfully created serverless index [{}] with semantic enrichment", indexName);

        } catch (final software.amazon.awssdk.services.opensearchserverless.model.ConflictException e) {
            LOG.info("Index [{}] already exists, skipping creation", indexName);
        } catch (final software.amazon.awssdk.core.exception.SdkException e) {
            throw new IOException(String.format(
                    "Failed to create index [%s] via serverless control plane: %s", indexName, e.getMessage()), e);
        }
    }

    private void createManagedDomainIndex(final String indexName,
                                          final SemanticEnrichmentConfig semanticConfig) throws IOException {
        LOG.info("Creating managed domain index [{}] with semantic enrichment for fields {}",
                indexName, semanticConfig.getFields());

        try (final OpenSearchClient client = OpenSearchClient.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build()) {

            client.createIndex(software.amazon.awssdk.services.opensearch.model.CreateIndexRequest.builder()
                    .domainName(collectionIdOrDomainName)
                    .indexName(indexName)
                    .indexSchema(Document.fromMap(toDocumentMap(buildIndexSchema(semanticConfig))))
                    .build());

            LOG.info("Successfully created managed domain index [{}] with semantic enrichment", indexName);

        } catch (final software.amazon.awssdk.services.opensearch.model.ResourceAlreadyExistsException e) {
            LOG.info("Index [{}] already exists, skipping creation", indexName);
        } catch (final software.amazon.awssdk.services.opensearch.model.ConflictException e) {
            LOG.info("Index [{}] already exists, skipping creation", indexName);
        } catch (final software.amazon.awssdk.core.exception.SdkException e) {
            throw new IOException(String.format(
                    "Failed to create index [%s] via managed domain control plane: %s", indexName, e.getMessage()), e);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Document> toDocumentMap(final Map<String, Object> map) {
        final Map<String, Document> result = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            result.put(entry.getKey(), toDocument(entry.getValue()));
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private Document toDocument(final Object value) {
        if (value instanceof Map) {
            return Document.fromMap(toDocumentMap((Map<String, Object>) value));
        } else if (value instanceof String) {
            return Document.fromString((String) value);
        } else if (value instanceof Integer || value instanceof Long || value instanceof Double || value instanceof Float) {
            return Document.fromNumber(value.toString());
        } else if (value instanceof Boolean) {
            return Document.fromBoolean((Boolean) value);
        }
        return Document.fromString(String.valueOf(value));
    }

    Map<String, Object> buildIndexSchema(final SemanticEnrichmentConfig semanticConfig) {
        final Map<String, Object> properties = new LinkedHashMap<>();
        for (final SemanticFieldMapping fieldMapping : semanticConfig.getFields()) {
            final Map<String, Object> fieldProps = new LinkedHashMap<>();
            fieldProps.put("type", "text");
            final Map<String, String> semanticEnrichment = new LinkedHashMap<>();
            semanticEnrichment.put("status", "ENABLED");
            semanticEnrichment.put("language_options", fieldMapping.getLanguage().getValue());
            fieldProps.put("semantic_enrichment", semanticEnrichment);
            properties.put(fieldMapping.getName(), fieldProps);
        }
        return Map.of("mappings", Map.of("properties", properties));
    }

}
