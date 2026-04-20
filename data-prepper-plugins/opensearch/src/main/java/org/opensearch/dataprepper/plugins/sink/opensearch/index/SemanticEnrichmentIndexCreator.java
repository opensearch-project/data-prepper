/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.aws.api.AwsCredentialsOptions;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.plugins.sink.opensearch.ConnectionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;
import software.amazon.awssdk.auth.signer.params.Aws4SignerParams;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.HttpExecuteResponse;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SemanticEnrichmentIndexCreator {

    private static final Logger LOG = LoggerFactory.getLogger(SemanticEnrichmentIndexCreator.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String AOSS_SERVICE_NAME = "aoss";
    private static final String ES_SERVICE_NAME = "es";
    private static final String AOSS_ENDPOINT_FORMAT = "https://aoss.%s.amazonaws.com/";
    private static final String ES_ENDPOINT_FORMAT = "https://es.%s.amazonaws.com/2021-01-01/opensearch/domain/%s/index";
    private static final String AOSS_CREATE_INDEX_TARGET = "OpenSearchServerless.CreateIndex";
    private static final String AOSS_CONTENT_TYPE = "application/x-amz-json-1.0";
    private static final String ES_CONTENT_TYPE = "application/json";

    private final AwsCredentialsProvider credentialsProvider;
    private final Region region;
    private final boolean serverless;
    private final String collectionIdOrDomainName;

    public SemanticEnrichmentIndexCreator(final AwsCredentialsSupplier awsCredentialsSupplier,
                                          final ConnectionConfiguration connectionConfiguration) {
        final AwsCredentialsOptions awsCredentialsOptions = connectionConfiguration.createAwsCredentialsOptions();
        this.credentialsProvider = awsCredentialsSupplier.getProvider(awsCredentialsOptions);
        this.region = awsCredentialsOptions.getRegion();
        this.serverless = connectionConfiguration.isServerless();
        this.collectionIdOrDomainName = serverless
                ? extractCollectionId(connectionConfiguration.getHosts())
                : extractDomainName(connectionConfiguration.getHosts());
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
        LOG.info("Creating index [{}] with semantic enrichment via AOSS control plane for fields {}",
                indexName, semanticConfig.getFields());

        final Map<String, Object> body = new LinkedHashMap<>();
        body.put("id", collectionIdOrDomainName);
        body.put("indexName", indexName);
        body.put("indexSchema", buildIndexSchema(semanticConfig));

        final URI endpoint = URI.create(String.format(AOSS_ENDPOINT_FORMAT, region.id()));
        final SdkHttpFullRequest request = SdkHttpFullRequest.builder()
                .uri(endpoint)
                .method(SdkHttpMethod.POST)
                .putHeader("Content-Type", AOSS_CONTENT_TYPE)
                .putHeader("X-Amz-Target", AOSS_CREATE_INDEX_TARGET)
                .contentStreamProvider(() -> toStream(body))
                .build();

        executeSignedRequest(request, AOSS_SERVICE_NAME, indexName, "AOSS control plane");
    }

    private void createManagedDomainIndex(final String indexName,
                                          final SemanticEnrichmentConfig semanticConfig) throws IOException {
        LOG.info("Creating index [{}] with semantic enrichment via OpenSearch Service control plane for fields {}",
                indexName, semanticConfig.getFields());

        final Map<String, Object> body = new LinkedHashMap<>();
        body.put("IndexName", indexName);
        body.put("IndexSchema", buildIndexSchema(semanticConfig));

        final URI endpoint = URI.create(String.format(ES_ENDPOINT_FORMAT, region.id(), collectionIdOrDomainName));
        final SdkHttpFullRequest request = SdkHttpFullRequest.builder()
                .uri(endpoint)
                .method(SdkHttpMethod.POST)
                .putHeader("Content-Type", ES_CONTENT_TYPE)
                .contentStreamProvider(() -> toStream(body))
                .build();

        executeSignedRequest(request, ES_SERVICE_NAME, indexName, "OpenSearch Service control plane");
    }

    private void executeSignedRequest(final SdkHttpFullRequest request,
                                      final String serviceName,
                                      final String indexName,
                                      final String apiDescription) throws IOException {
        final Aws4Signer signer = Aws4Signer.create();
        final Aws4SignerParams signerParams = Aws4SignerParams.builder()
                .awsCredentials(credentialsProvider.resolveCredentials())
                .signingName(serviceName)
                .signingRegion(region)
                .build();

        final SdkHttpFullRequest signedRequest = signer.sign(request, signerParams);

        try (final SdkHttpClient httpClient = ApacheHttpClient.builder().build()) {
            final HttpExecuteRequest executeRequest = HttpExecuteRequest.builder()
                    .request(signedRequest)
                    .contentStreamProvider(signedRequest.contentStreamProvider().orElse(null))
                    .build();

            final HttpExecuteResponse response = httpClient.prepareRequest(executeRequest).call();
            final SdkHttpResponse httpResponse = response.httpResponse();
            final int statusCode = httpResponse.statusCode();

            String responseBody = "";
            if (response.responseBody().isPresent()) {
                responseBody = new String(response.responseBody().get().readAllBytes(), StandardCharsets.UTF_8);
            }

            if (statusCode >= 200 && statusCode < 300) {
                LOG.info("Successfully created index [{}] with semantic enrichment via {}", indexName, apiDescription);
            } else if (responseBody.contains("ConflictException") || responseBody.contains("ResourceAlreadyExistsException")) {
                LOG.info("Index [{}] already exists, skipping creation", indexName);
            } else {
                throw new IOException(String.format(
                        "Failed to create index [%s] via %s. Status: %d, Response: %s",
                        indexName, apiDescription, statusCode, responseBody));
            }
        }
    }

    private Map<String, Object> buildIndexSchema(final SemanticEnrichmentConfig semanticConfig) {
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

    private ByteArrayInputStream toStream(final Map<String, Object> body) {
        try {
            return new ByteArrayInputStream(OBJECT_MAPPER.writeValueAsBytes(body));
        } catch (final IOException e) {
            throw new RuntimeException("Failed to serialize request body", e);
        }
    }

    static String extractCollectionId(final List<String> hosts) {
        final String hostname = getHostname(hosts);
        if (!hostname.contains(".aoss.")) {
            throw new IllegalArgumentException(
                    "Host does not appear to be an AOSS endpoint: " + hostname +
                            ". Semantic enrichment via AOSS control plane requires a serverless collection.");
        }
        return hostname.split("\\.")[0];
    }

    static String extractDomainName(final List<String> hosts) {
        final String hostname = getHostname(hosts);
        // Managed domain format: search-{domain-name}-{random}.{region}.es.amazonaws.com
        // or vpc-{domain-name}-{random}.{region}.es.amazonaws.com
        if (!hostname.contains(".es.amazonaws.com")) {
            throw new IllegalArgumentException(
                    "Host does not appear to be a managed OpenSearch domain: " + hostname +
                            ". Expected format: search-{domain}-{id}.{region}.es.amazonaws.com");
        }
        final String prefix = hostname.split("\\.")[0]; // e.g., search-my-domain-abc123
        final String withoutSearchPrefix = prefix.replaceFirst("^(search-|vpc-)", "");
        // Remove the trailing random ID: last hyphen-separated segment
        final int lastHyphen = withoutSearchPrefix.lastIndexOf('-');
        if (lastHyphen <= 0) {
            throw new IllegalArgumentException(
                    "Unable to extract domain name from host: " + hostname);
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
