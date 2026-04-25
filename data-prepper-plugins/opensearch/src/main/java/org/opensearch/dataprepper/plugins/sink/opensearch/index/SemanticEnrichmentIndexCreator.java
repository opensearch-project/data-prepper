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
import java.net.URI;
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
                                          final SemanticEnrichmentConfig semanticConfig) {
        final AwsCredentialsOptions awsCredentialsOptions = connectionConfiguration.createAwsCredentialsOptions();
        this.credentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
        this.region = awsCredentialsOptions.getRegion();
        this.serverless = connectionConfiguration.isServerless();
        this.collectionIdOrDomainName = resolveCollectionIdOrDomainName(
                semanticConfig, connectionConfiguration.getHosts());
    }

    private String resolveCollectionIdOrDomainName(final SemanticEnrichmentConfig semanticConfig,
                                                   final List<String> hosts) {
        if (serverless) {
            if (semanticConfig.getCollectionName() != null && !semanticConfig.getCollectionName().isEmpty()) {
                LOG.info("Using configured collection_name: {}", semanticConfig.getCollectionName());
                return semanticConfig.getCollectionName();
            }

            LOG.info("collection_name not configured, extracting from host URL");
            return extractCollectionId(hosts);
        }

        if (semanticConfig.getDomainName() != null && !semanticConfig.getDomainName().isEmpty()) {
            LOG.info("Using configured domain_name: {}", semanticConfig.getDomainName());
            return semanticConfig.getDomainName();
        }

        LOG.info("domain_name not configured, extracting from host URL");
        return extractDomainName(hosts);
    }

    public void createIndex(final String indexName, final SemanticEnrichmentConfig semanticConfig) throws IOException {
        if (serverless) {
            createServerlessIndex(indexName, semanticConfig);
        } else {
            createManagedDomainIndex(indexName, semanticConfig);
        }
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
        for (final String field : semanticConfig.getFields()) {
            final Map<String, Object> fieldMapping = new LinkedHashMap<>();
            fieldMapping.put("type", "text");
            final Map<String, String> semanticEnrichment = new LinkedHashMap<>();
            semanticEnrichment.put("status", "ENABLED");
            semanticEnrichment.put("language_options", semanticConfig.getLanguage());
            fieldMapping.put("semantic_enrichment", semanticEnrichment);
            properties.put(field, fieldMapping);
        }
        return Map.of("mappings", Map.of("properties", properties));
    }

    static String extractCollectionId(final List<String> hosts) {
        final String hostname = getHostname(hosts);
        return hostname.split("\\.")[0];
    }

    static String extractDomainName(final List<String> hosts) {
        final String hostname = getHostname(hosts);
        final String prefix = hostname.split("\\.")[0];
        final String withoutSearchPrefix = prefix.replaceFirst("^(search-|vpc-)", "");
        final int lastHyphen = withoutSearchPrefix.lastIndexOf('-');
        if (lastHyphen <= 0) {
            throw new IllegalArgumentException(
                    "Unable to extract domain name from host: " + hostname +
                            ". Please set the 'domain_name' option in semantic_enrichment config.");
        }
        return withoutSearchPrefix.substring(0, lastHyphen);
    }

    private static String getHostname(final List<String> hosts) {
        if (hosts == null || hosts.isEmpty()) {
            throw new IllegalArgumentException("Hosts list is empty, cannot extract endpoint identifier");
        }
        final String hostname = URI.create(hosts.get(0)).getHost();
        if (hostname == null) {
            throw new IllegalArgumentException("Unable to parse hostname from: " + hosts.get(0));
        }
        return hostname;
    }
}
