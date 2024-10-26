/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.index;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.micrometer.core.instrument.util.StringUtils;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.ExistsAliasRequest;
import org.opensearch.client.transport.endpoints.BooleanResponse;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.FileReader;
import org.opensearch.dataprepper.plugins.sink.opensearch.s3.S3FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;

import javax.ws.rs.HttpMethod;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.opensearch.dataprepper.plugins.sink.opensearch.index.AbstractIndexManager.TIME_PATTERN;

class IsmPolicyManagement implements IsmPolicyManagementStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(IsmPolicyManagement.class);

    // TODO: replace with new _opensearch API
    private static final String POLICY_MANAGEMENT_ENDPOINT = "/_opendistro/_ism/policies/";
    public static final String DEFAULT_INDEX_SUFFIX = "-000001";
    private static final String POLICY_FILE_ROOT_KEY = "policy";
    private static final String POLICY_FILE_ISM_TEMPLATE_KEY = "ism_template";
    private static final String S3_PREFIX = "s3://";

    private final RestHighLevelClient restHighLevelClient;
    private final OpenSearchClient openSearchClient;
    private final String policyName;
    private final String policyFile;
    private final String policyFileWithoutIsmTemplate;
    private S3Client s3Client;

    public IsmPolicyManagement(final OpenSearchClient openSearchClient,
                               final RestHighLevelClient restHighLevelClient,
                               final String policyName,
                               final String policyFile,
                               final String policyFileWithoutIsmTemplate) {
        checkNotNull(restHighLevelClient);
        checkNotNull(openSearchClient);
        checkArgument(StringUtils.isNotEmpty(policyName));
        checkArgument(StringUtils.isNotEmpty(policyFile));
        checkArgument(StringUtils.isNotEmpty(policyFileWithoutIsmTemplate));
        this.openSearchClient = openSearchClient;
        this.restHighLevelClient = restHighLevelClient;
        this.policyName = policyName;
        this.policyFile = policyFile;
        this.policyFileWithoutIsmTemplate = policyFileWithoutIsmTemplate;
    }

    public IsmPolicyManagement(final OpenSearchClient openSearchClient,
                               final RestHighLevelClient restHighLevelClient,
                               final String policyName,
                               final String policyFile,
                               final S3Client s3Client) {
        checkNotNull(restHighLevelClient);
        checkNotNull(openSearchClient);
        checkArgument(StringUtils.isNotEmpty(policyName));
        checkArgument(StringUtils.isNotEmpty(policyFile));
        this.openSearchClient = openSearchClient;
        this.restHighLevelClient = restHighLevelClient;
        this.policyName = policyName;
        this.policyFile = policyFile;
        this.policyFileWithoutIsmTemplate = null;
        this.s3Client = s3Client;
    }

    @Override
    public Optional<String> checkAndCreatePolicy(final String indexAlias) throws IOException {
        final String policyManagementEndpoint = POLICY_MANAGEMENT_ENDPOINT + policyName;

        String policyJsonString = retrievePolicyJsonString(policyFile);
	if(!indexAlias.isEmpty()) {
                final ObjectMapper mapper = new ObjectMapper();
                final JsonNode jsonNode = mapper.readTree(policyJsonString);
                final ArrayNode iparray = mapper.createArrayNode();
                iparray.add(indexAlias + "*");
                ((ObjectNode) jsonNode.get("policy").get("ism_template")).put("index_patterns", iparray);
                policyJsonString = jsonNode.toString();
        }
        LOG.debug("Got the policystring as {} and indexAlias as {}", policyJsonString, indexAlias);
        Request request = createPolicyRequestFromFile(policyManagementEndpoint, policyJsonString);

        try {
            restHighLevelClient.getLowLevelClient().performRequest(request);
        } catch (ResponseException e1) {
            final String msg = e1.getMessage();
            if (msg.contains("Invalid field: [ism_template]")) {

                if(StringUtils.isEmpty(policyFileWithoutIsmTemplate)) {
                    policyJsonString = dropIsmTemplateFromPolicy(policyJsonString);
                } else {
                    policyJsonString = retrievePolicyJsonString(policyFileWithoutIsmTemplate);
                }

                request = createPolicyRequestFromFile(policyManagementEndpoint, policyJsonString);
                try {
                    restHighLevelClient.getLowLevelClient().performRequest(request);
                } catch (ResponseException e2) {
                    if (e2.getMessage().contains("version_conflict_engine_exception")
                            || e2.getMessage().contains("resource_already_exists_exception")) {
                        // Do nothing - likely caused by
                        // (1) a race condition where the resource was created by another host before this host's
                        // restClient made its request;
                        // (2) policy already exists in the cluster
                    } else {
                        throw e2;
                    }
                }
                return Optional.of(policyName);
            } else if (e1.getMessage().contains("version_conflict_engine_exception")
                    || e1.getMessage().contains("resource_already_exists_exception")) {
                // Do nothing - likely caused by
                // (1) a race condition where the resource was created by another host before this host's
                // restClient made its request;
                // (2) policy already exists in the cluster
            } else {
                throw e1;
            }
        }

        return Optional.empty();
    }

    @Override
    public List<String> getIndexPatterns(final String indexAlias){
        checkArgument(StringUtils.isNotEmpty(indexAlias));
        return Collections.singletonList(TIME_PATTERN.matcher(indexAlias).replaceAll("*") + "-*");
    }

    @Override
    public boolean checkIfIndexExistsOnServer(final String indexAlias) throws IOException {
        checkArgument(StringUtils.isNotEmpty(indexAlias));
        final BooleanResponse booleanResponse = openSearchClient.indices().existsAlias(
                new ExistsAliasRequest.Builder().name(indexAlias).build());
        return booleanResponse.value();
    }

    @Override
    public CreateIndexRequest getCreateIndexRequest(final String indexAlias) {
        checkArgument(StringUtils.isNotEmpty(indexAlias));
        final String initialIndexName = indexAlias + DEFAULT_INDEX_SUFFIX;
        final CreateIndexRequest createIndexRequest
                = new CreateIndexRequest.Builder()
                .index(initialIndexName).aliases(
                        indexAlias, new org.opensearch.client.opensearch.indices.Alias.Builder()
                                .isWriteIndex(true)
                                .build()
                )
                                .build();
        return createIndexRequest;
    }

    private String retrievePolicyJsonString(final String fileName) throws IOException {
        if (fileName.startsWith(S3_PREFIX)) {
            final String ismPolicyJsonString = retrievePolicyJsonStringFromS3(fileName);
            if (ismPolicyJsonString != null) {
                return ismPolicyJsonString;
            }
            else {
                throw new RuntimeException("Error encountered while processing ISM policy provided from S3.");
            }
        } else {
            return retrievePolicyJsonStringFromFile(fileName);
        }
    }

    private String retrievePolicyJsonStringFromS3(final String fileName) throws IOException {
        String ismPolicy = null;
        final FileReader s3FileReader = new S3FileReader(s3Client);
        try (final InputStream inputStream = s3FileReader.readFile(fileName)) {
            final Map<String, Object> stringObjectMap = new ObjectMapper().readValue(inputStream, new TypeReference<>() {
            });
            ismPolicy = new ObjectMapper().writeValueAsString(stringObjectMap);
        } catch (JsonProcessingException e) {
            LOG.info("Error encountered while processing JSON content in ISM policy file from S3.");
        }

        return ismPolicy;
    }

        private String retrievePolicyJsonStringFromFile(final String fileName) throws IOException {
        final StringBuilder policyJsonBuffer = new StringBuilder();
        final File file = new File(fileName);
        final URL policyFileUrl;

        if (file.isAbsolute()) {
            policyFileUrl = file.toURI().toURL();
        } else {
            policyFileUrl = getClass().getClassLoader().getResource(fileName);
        }
        try (final InputStream inputStream = policyFileUrl.openStream();
             final BufferedReader reader = new BufferedReader(new InputStreamReader(Objects.requireNonNull(inputStream)))) {
            reader.lines().forEach(line -> policyJsonBuffer.append(line).append("\n"));
        }
        return policyJsonBuffer.toString();
    }

    private Request createPolicyRequestFromFile(final String endPoint, final String policyJsonString) {
        final Request request = new Request(HttpMethod.PUT, endPoint);
        request.setJsonEntity(policyJsonString);
        return request;
    }

    private String dropIsmTemplateFromPolicy(final String policyJsonString) throws JsonProcessingException {
        final ObjectMapper mapper = new ObjectMapper();
        final JsonNode jsonNode = mapper.readTree(policyJsonString);
        ((ObjectNode)jsonNode.get(POLICY_FILE_ROOT_KEY)).remove(POLICY_FILE_ISM_TEMPLATE_KEY);
        return jsonNode.toString();
    }

}
